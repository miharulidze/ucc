#include "tl_spin_bcast.h"
#include "tl_spin_p2p.h"
#include "tl_spin_mcast.h"
#include "tl_spin_bitmap.h"

static ucc_status_t ucc_tl_spin_bcast_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t           *task     = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t           *team     = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_context_t        *ctx      = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_tl_spin_worker_info_t    *ctrl_ctx = team->ctrl_ctx;
    ucc_rank_t                    root     = task->super.bargs.args.root;
    ucc_tl_spin_rcache_region_t  *cache_entry;
    int                           reg_changed;

    if (team->subset.myrank == root) {
        errno = 0;
        reg_changed = 0;
        if (UCC_OK != ucc_rcache_get(ctx->mcast.rcache,
                                     task->src_ptr,
                                     task->src_buf_size,
                                     &reg_changed,
                                     (ucc_rcache_region_t **)&cache_entry)) {
            tl_error(UCC_TASK_LIB(task), "Root failed to register send buffer memory errno: %d", errno);
            return UCC_ERR_NO_RESOURCE;
        }
        task->cached_sbuf_mkey = cache_entry;
    }

    errno = 0;
    reg_changed = 0;
    if (UCC_OK != ucc_rcache_get(ctx->p2p.rcache,
                                 task->dst_ptr,
                                 task->dst_buf_size,
                                 &reg_changed,
                                 (ucc_rcache_region_t **)&cache_entry)) {
        tl_error(UCC_TASK_LIB(task), "Root failed to register receive buffer memory errno: %d", errno);
        return UCC_ERR_NO_RESOURCE;
    }
    task->cached_rbuf_mkey = cache_entry;

    // check if workers are busy
    if (team->cur_task) {
        ucc_assert_always(0); // test
        goto enqueue;
    } else {
        pthread_mutex_lock(&team->tx_signal_mutex);
        pthread_mutex_lock(&team->rx_signal_mutex);
        team->cur_task = task;
        pthread_mutex_unlock(&team->tx_signal_mutex);
        pthread_mutex_unlock(&team->rx_signal_mutex);
    }

    tl_debug(UCC_TASK_LIB(task), "init coll task ptr=%p tgid=%u", task, task->id);

    ib_qp_post_recv(team->workers[1].reliability.qps[UCC_TL_SPIN_RN_QP_ID], NULL, NULL, 0, 0);

    // FIXME: without RNR retry that might result in RNR at the root's left neighbor.
    pthread_mutex_lock(&team->rx_signal_mutex);
    team->rx_signal = UCC_TL_SPIN_WORKER_START;
    pthread_mutex_unlock(&team->rx_signal_mutex);

    ucc_tl_spin_team_rc_ring_barrier(team->subset.myrank, ctrl_ctx);

    // start workers
    if (team->subset.myrank == root) {
        pthread_mutex_lock(&team->tx_signal_mutex);
        team->tx_signal = UCC_TL_SPIN_WORKER_START;
        pthread_mutex_unlock(&team->tx_signal_mutex);
    }

enqueue:
    coll_task->status = UCC_INPROGRESS;
    tl_debug(UCC_TASK_LIB(task), "start coll task ptr=%p tgid=%u", task, task->id);
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

static void ucc_tl_spin_bcast_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t    *task          = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t    *team          = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_context_t *ctx           = UCC_TL_SPIN_TEAM_CTX(team);
    int                    is_root       = team->subset.myrank == task->super.bargs.args.root ? 1 : 0;
    int                    total_workers = is_root ? 
                                           ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers :
                                           ctx->cfg.n_rx_workers;
    int                    workers_completed = 0;

    if (task->id != team->cur_task->id) {
        coll_task->status = UCC_INPROGRESS;
    } else {
        ucc_assert_always(task == team->cur_task);
    }

    if (is_root) {
        pthread_mutex_lock(&team->tx_compls_mutex);
        workers_completed += team->tx_compls;
        pthread_mutex_unlock(&team->tx_compls_mutex);
    }

    pthread_mutex_lock(&team->rx_compls_mutex);
    workers_completed += team->rx_compls;
    pthread_mutex_unlock(&team->rx_compls_mutex);

    if (workers_completed == total_workers) {
        coll_task->status = UCC_OK;
    } else {
        coll_task->status = UCC_INPROGRESS;
    }

    //tl_debug(UCC_TASK_LIB(task), "progress coll task ptr=%p tgid=%u", task, task->id);
}

static ucc_status_t ucc_tl_spin_bcast_finalize(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t    *task    = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t    *team    = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_context_t *ctx     = UCC_TL_SPIN_TEAM_CTX(team);
    int                    is_root = team->subset.myrank == task->super.bargs.args.root ? 1 : 0;

    if (task->id != team->cur_task->id) {
        ucc_assert_always(0); // atm I don't know are there any scenario when we get into this branch
        goto mpool_put;
    } else {
        ucc_assert_always(task == team->cur_task);
    }

    if (is_root) {
        pthread_mutex_lock(&team->tx_signal_mutex);
        team->tx_signal = UCC_TL_SPIN_WORKER_POLL;
        pthread_mutex_unlock(&team->tx_signal_mutex);
        pthread_mutex_lock(&team->tx_compls_mutex);
        team->tx_compls = 0;
        pthread_mutex_unlock(&team->tx_compls_mutex);
    }

    pthread_mutex_lock(&team->rx_signal_mutex);
    team->rx_signal = UCC_TL_SPIN_WORKER_POLL;
    pthread_mutex_unlock(&team->rx_signal_mutex);
    pthread_mutex_lock(&team->rx_compls_mutex);
    team->rx_compls = 0;
    pthread_mutex_unlock(&team->rx_compls_mutex);

    pthread_mutex_lock(&team->tx_signal_mutex);
    pthread_mutex_lock(&team->rx_signal_mutex);
    team->cur_task = NULL;
    pthread_mutex_unlock(&team->tx_signal_mutex);
    pthread_mutex_unlock(&team->rx_signal_mutex);

    if (is_root) {
        ucc_rcache_region_put(ctx->mcast.rcache, &task->cached_sbuf_mkey->super);
    }
    ucc_rcache_region_put(ctx->p2p.rcache, &task->cached_rbuf_mkey->super);

mpool_put:
    tl_debug(UCC_TASK_LIB(task), "finalize coll task ptr=%p tgid=%u", task, task->id);
    ucc_mpool_put(task);
    return UCC_OK;
}

void ucc_tl_spin_bcast_init_task_tx_info(ucc_tl_spin_task_t    *task,
                                         ucc_tl_spin_context_t *ctx)
{
    task->tx_thread_work = task->src_buf_size / ctx->cfg.n_tx_workers;
    task->batch_bsize    = ctx->mcast.mtu * ctx->cfg.mcast_tx_batch_sz;    
    task->n_batches      = task->tx_thread_work / task->batch_bsize;
    if (task->tx_thread_work < task->batch_bsize) {
        ucc_assert_always(task->n_batches == 0);
        task->last_batch_size = task->tx_thread_work / ctx->mcast.mtu;
    } else {
        task->last_batch_size = (task->tx_thread_work - task->batch_bsize * task->n_batches) / ctx->mcast.mtu;
    }
    ucc_assert_always(task->tx_thread_work >= (task->n_batches * task->batch_bsize + task->last_batch_size * ctx->mcast.mtu));
    task->last_pkt_size = task->tx_thread_work - (task->n_batches * task->batch_bsize + task->last_batch_size * ctx->mcast.mtu);
}

ucc_status_t ucc_tl_spin_bcast_init(ucc_tl_spin_task_t   *task,
                                    ucc_base_coll_args_t *coll_args,
                                    ucc_tl_spin_team_t   *team)
{
    ucc_tl_spin_context_t *ctx         = UCC_TL_SPIN_TEAM_CTX(team);
    size_t                 count       = coll_args->args.src.info.count;
    size_t                 dt_size     = ucc_dt_size(coll_args->args.src.info.datatype);

    ucc_assert_always(ctx->cfg.n_tx_workers == ctx->cfg.n_rx_workers);
    ucc_assert_always(count * dt_size >= ctx->cfg.n_tx_workers);
    ucc_assert_always(((count * dt_size) % ctx->cfg.n_tx_workers) == 0);

    task->src_ptr          = coll_args->args.src.info.buffer;
    task->dst_ptr          = coll_args->args.src.info.buffer;
    task->src_buf_size     = task->dst_buf_size = count * dt_size;
    ucc_assert_always(task->src_buf_size <= ctx->cfg.max_recv_buf_size);
    task->start_chunk_id   = 0;
    task->inplace_start_id = task->inplace_end_id = SIZE_MAX;
    ucc_tl_spin_bcast_init_task_tx_info(task, ctx);

    task->pkts_to_send = task->tx_thread_work / ctx->mcast.mtu;
    if (task->last_pkt_size) {
        task->pkts_to_send++;
    }
    task->pkts_to_recv = task->pkts_to_send;

    task->timeout = ((double)task->tx_thread_work /
                    (double)ctx->cfg.link_bw) / 
                    1000000000.0 * 
                    (double)ctx->cfg.timeout_scaling_param;
    tl_debug(UCC_TL_SPIN_TEAM_LIB(team), "work: %zu, bw: %d, task timeout: %.16lf, scaling param: %d", 
             task->tx_thread_work, ctx->cfg.link_bw, task->timeout, ctx->cfg.timeout_scaling_param);
    task->super.post      = ucc_tl_spin_bcast_start;
    task->super.progress  = ucc_tl_spin_bcast_progress;
    task->super.finalize  = ucc_tl_spin_bcast_finalize;
    task->coll_type       = UCC_COLL_TYPE_BCAST;
    tl_debug(UCC_TL_SPIN_TEAM_LIB(team), "got new task of size: %zu src_ptr: %p", 
             count * dt_size, task->src_ptr);

    return UCC_OK;
}

inline ucc_status_t
ucc_tl_spin_coll_worker_tx_bcast_start(ucc_tl_spin_worker_info_t *ctx)
{
    ucc_status_t status = ucc_tl_spin_coll_worker_tx_handler(ctx);    
    ucc_assert_always(status == UCC_OK);

    pthread_mutex_lock(&ctx->team->tx_compls_mutex);
    ctx->team->tx_compls++;
    pthread_mutex_unlock(&ctx->team->tx_compls_mutex);

    return UCC_OK;
}

inline ucc_status_t
ucc_tl_spin_coll_worker_rx_bcast_start(ucc_tl_spin_worker_info_t *ctx)
{
    int is_root = ctx->team->subset.myrank == ctx->team->cur_task->super.bargs.args.root ? 1 : 0;
    ucc_status_t status;

    if (!is_root) {
        status = ucc_tl_spin_coll_worker_rx_handler(ctx);
        ucc_assert_always(status == UCC_OK);
    } else {
        ctx->reliability.to_recv = 0;
    }
    status = ucc_tl_spin_coll_worker_rx_reliability_handler(ctx);
    ucc_assert_always(status == UCC_OK);

    pthread_mutex_lock(&ctx->team->rx_compls_mutex);
    ctx->team->rx_compls++;
    pthread_mutex_unlock(&ctx->team->rx_compls_mutex);

    return UCC_OK;
}

inline ucc_status_t 
ucc_tl_spin_coll_worker_tx_handler(ucc_tl_spin_worker_info_t *ctx)
{
#ifndef UCC_TL_SPIN_DISABLE_MCAST
    ucc_tl_spin_task_t           *task           = ctx->team->cur_task;
    size_t                        remaining_work = task->tx_thread_work;
    void                         *buf            = task->src_ptr + ctx->id * task->tx_thread_work;
    int                           ncomps         = 0;
    ucc_tl_spin_packed_chunk_id_t packed_chunk_id;
    int                           i;
    struct ibv_wc                 wc;

    ucc_assert_always(ctx->signal != NULL);
    ucc_assert_always(ctx->n_mcg == 1);

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "tx worker %u got root start signal, tgid=%u", ctx->id, task->id);
    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team),
            "tx worker %u got bcast task of size: %zu n_batches: %zu last_batch_size: %zu last_pkt_sz: %zu", 
             ctx->id, task->tx_thread_work, task->n_batches, task->last_batch_size, task->last_pkt_size);

    packed_chunk_id.chunk_metadata.task_id  = task->id;
    packed_chunk_id.chunk_metadata.chunk_id = task->start_chunk_id;

    for (i = 0; i < task->n_batches; i++) {
        ib_qp_ud_post_mcast_send_batch(ctx->qps[0], ctx->ahs[0],
                                       ctx->swrs[0], ctx->ssges[0],
                                       task->cached_sbuf_mkey->mr,
                                       buf,
                                       ctx->ctx->mcast.mtu,
                                       ctx->ctx->cfg.mcast_tx_batch_sz,
                                       packed_chunk_id, 0);
        ncomps = ib_cq_poll(ctx->cq, 1, &wc); // only last packet in batch reports CQe
        ucc_assert_always(ncomps == 1);
        buf                      += task->batch_bsize;
        remaining_work           -= task->batch_bsize;
        packed_chunk_id.chunk_metadata.chunk_id += ctx->ctx->cfg.mcast_tx_batch_sz;
    }

    if (task->last_batch_size) {
        ib_qp_ud_post_mcast_send_batch(ctx->qps[0], ctx->ahs[0], 
                                       ctx->swrs[0], ctx->ssges[0],
                                       task->cached_sbuf_mkey->mr,
                                       buf,
                                       ctx->ctx->mcast.mtu,
                                       task->last_batch_size,
                                       packed_chunk_id, 0);
        ncomps = ib_cq_poll(ctx->cq, 1, &wc);
        ucc_assert_always(ncomps == 1);
        buf                      += task->last_batch_size * ctx->ctx->mcast.mtu;
        remaining_work           -= task->last_batch_size * ctx->ctx->mcast.mtu;
        packed_chunk_id.chunk_metadata.chunk_id += task->last_batch_size;
    }

    ucc_assert_always(remaining_work == task->last_pkt_size);
    if (task->last_pkt_size) {
        ib_qp_ud_post_mcast_send(ctx->qps[0], ctx->ahs[0], 
                                 ctx->swrs[0],
                                 task->cached_sbuf_mkey->mr,
                                 buf,
                                 task->last_pkt_size,
                                 packed_chunk_id.imm_data, 0);
        ncomps = ib_cq_poll(ctx->cq, 1, &wc);
        ucc_assert_always(ncomps == 1);
    }

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "tx worker %u finished multicasting\n", ctx->id);
#endif
    return UCC_OK;
}

inline void
ucc_tl_spin_coll_worker_rx_reliability_get_missing_ranks(ucc_tl_spin_worker_info_t *ctx, 
                                                         ucc_rank_t my_rank, 
                                                         int is_rooted, ucc_rank_t root)
{
    size_t i;

    ctx->reliability.n_missing_ranks = 0;
    for (i = 0; i < UCC_TL_TEAM_SIZE(ctx->team); i++) {
        if (i == my_rank || (is_rooted && (i != root))) {
            continue;
        }
        if (ctx->reliability.recvd_per_rank[i] < ctx->team->cur_task->pkts_to_send) {
            ctx->reliability.missing_ranks[ctx->reliability.n_missing_ranks++] = i;
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "missing chunks from rank: %zu\n", i);
        }
    }
}

inline void
ucc_tl_spin_coll_worker_rx_reliability_ln(ucc_tl_spin_worker_info_t *ctx, struct ibv_wc *wc)
{
    ucc_tl_spin_task_t                  *task = ctx->team->cur_task;
    ucc_tl_spin_reliability_proto_info_t request;
    size_t                               gap_start_offset, gap_size, read_offset;

ln_state_machine_entrance:
    switch(ctx->reliability.ln_state) {
        case UCC_TL_SPIN_RELIABILITY_PROTO_INIT:
            ucc_assert_always(!wc);
            if (!ctx->reliability.to_recv) {
                ctx->reliability.ln_state = UCC_TL_SPIN_RELIABILITY_PROTO_SEND_FIN;
                tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "ln[%zu]: INIT->SEND_FIN", (size_t)UCC_TL_TEAM_RANK(ctx->team));
                goto ln_state_machine_entrance;
            }
            ucc_tl_spin_coll_worker_rx_reliability_get_missing_ranks(ctx, UCC_TL_TEAM_RANK(ctx->team), 
                                                                     task->coll_type == UCC_COLL_TYPE_BCAST, 
                                                                     task->super.bargs.args.root);
            ucc_assert_always(ctx->reliability.n_missing_ranks > 0);
            ctx->reliability.current_rank = 0;
            ctx->reliability.ln_state     = UCC_TL_SPIN_RELIABILITY_PROTO_SEND_FETCH_REQ;
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), 
                     "ln[%zu]: INIT->SEND_FETCH_REQ, n_missing_ranks: %zu", 
                     (size_t)UCC_TL_TEAM_RANK(ctx->team), ctx->reliability.n_missing_ranks);
            // fall through
        case UCC_TL_SPIN_RELIABILITY_PROTO_SEND_FETCH_REQ:
            ctx->reliability.current_bitmap_offset = 0;
            ib_qp_post_recv(ctx->reliability.qps[UCC_TL_SPIN_LN_QP_ID], // pre-post WR for ACK
                            ctx->reliability.ln_rbuf_info_mr,
                            ctx->reliability.ln_rbuf_info, 
                            sizeof(ucc_tl_spin_buf_info_t), 
                            task->id);
            request.proto.pkt_type = UCC_TL_SPIN_RELIABILITY_PKT_NEED_FETCH;
            request.proto.rank_id  = ctx->reliability.missing_ranks[ctx->reliability.current_rank];
            ib_qp_rc_post_send(ctx->reliability.qps[UCC_TL_SPIN_LN_QP_ID], NULL, NULL, 0, request.imm_data, task->id);
            ctx->reliability.ln_state = UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_ACK;
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team),
                     "ln[%zu]: SEND_FETCH_REQ->WAIT_ACK, current_missing_rank_idx: %zu, missing_rank: %zu", 
                     (size_t)UCC_TL_TEAM_RANK(ctx->team),
                     (size_t)ctx->reliability.current_rank,
                     (size_t)ctx->reliability.missing_ranks[ctx->reliability.current_rank]);
            break;
        case UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_ACK:
            if (wc->opcode == IBV_WC_SEND) {
                break;
            }
            ucc_assert_always(wc->opcode == IBV_WC_RECV);
            ctx->reliability.ln_state = UCC_TL_SPIN_RELIABILITY_PROTO_ISSUE_READ;
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team),
                     "ln[%zu]: WAIT_ACK->ISSUE_READ, remote buf addr: %p, rkey : %u",
                     (size_t)UCC_TL_TEAM_RANK(ctx->team), 
                     (void *)ctx->reliability.ln_rbuf_info->addr, 
                     ctx->reliability.ln_rbuf_info->rkey);
            // fall through
        case UCC_TL_SPIN_RELIABILITY_PROTO_ISSUE_READ: // TODO: implement reads overlapping (if needed for perf)
            if (wc->opcode != IBV_WC_RECV) {
                ucc_assert_always(wc->opcode == IBV_WC_RDMA_READ);
                ctx->reliability.recvd_per_rank[ctx->reliability.missing_ranks[ctx->reliability.current_rank]] += ctx->reliability.last_gap_size;
            }
            gap_size = ucc_tl_spin_bitmap_get_next_gap(&ctx->reliability.bitmap,
                                                       task->pkts_to_send,
                                                       task->pkts_to_send * ctx->reliability.missing_ranks[ctx->reliability.current_rank],
                                                       ctx->reliability.current_bitmap_offset,
                                                       &gap_start_offset,
                                                       &ctx->reliability.current_bitmap_offset);
            if (wc->opcode == IBV_WC_RECV) {
                ucc_assert_always(gap_size > 0);
            }
            if (gap_size) {
                read_offset = task->src_buf_size * ctx->reliability.missing_ranks[ctx->reliability.current_rank] + ctx->ctx->mcast.mtu * gap_start_offset;
                if ((ctx->reliability.recvd_per_rank[ctx->reliability.missing_ranks[ctx->reliability.current_rank]] + gap_size == task->pkts_to_send) && task->last_pkt_size) {
                    ctx->reliability.sge.length = ctx->ctx->mcast.mtu * (gap_size - 1) + task->last_pkt_size;
                } else {
                    ctx->reliability.sge.length = ctx->ctx->mcast.mtu * gap_size;
                }
                ctx->reliability.sge.addr                   = (uint64_t)task->dst_ptr + read_offset;
                ctx->reliability.sge.lkey                   = task->cached_rbuf_mkey->mr->lkey;
                ctx->reliability.rd_swr.wr.rdma.remote_addr = ctx->reliability.ln_rbuf_info->addr + read_offset;
                ctx->reliability.rd_swr.wr.rdma.rkey        = ctx->reliability.ln_rbuf_info->rkey;
                ctx->reliability.rd_swr.wr_id               = task->id;
                ib_qp_rc_post_swr(ctx->reliability.qps[UCC_TL_SPIN_LN_QP_ID], &ctx->reliability.rd_swr);
                ctx->reliability.last_gap_size = gap_size;
                tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team),
                         "ln[%zu]: ISSUE_READ->ISSUE_READ, rank: %zu, recvd_per_rank: %zu, gap_start_offset: %zu, gap_size: %zu, read_size: %u, dst_buf_size: %zu, mtu: %d",
                         (size_t)UCC_TL_TEAM_RANK(ctx->team),
                         (size_t)ctx->reliability.missing_ranks[ctx->reliability.current_rank],
                         (size_t)ctx->reliability.recvd_per_rank[ctx->reliability.missing_ranks[ctx->reliability.current_rank]],
                         gap_start_offset, gap_size, ctx->reliability.sge.length, task->dst_buf_size, ctx->ctx->mcast.mtu);
                break;
            }
            if (ctx->reliability.current_rank < (ctx->reliability.n_missing_ranks - 1)) {
                ucc_assert_always(ctx->reliability.recvd_per_rank[ctx->reliability.missing_ranks[ctx->reliability.current_rank]] ==
                                  task->pkts_to_send);
                ctx->reliability.current_rank++;
                ctx->reliability.ln_state = UCC_TL_SPIN_RELIABILITY_PROTO_SEND_FETCH_REQ;
                tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "ln[%zu]: ISSUE_READ->SEND_FETCH_REQ", 
                         (size_t)UCC_TL_TEAM_RANK(ctx->team));
                goto ln_state_machine_entrance;
            }
            ctx->reliability.ln_state = UCC_TL_SPIN_RELIABILITY_PROTO_SEND_FIN;
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "ln[%zu]: ISSUE_READ->SEND_FIN", 
                     (size_t)UCC_TL_TEAM_RANK(ctx->team));
            // fall through
        case UCC_TL_SPIN_RELIABILITY_PROTO_SEND_FIN:
            request.proto.pkt_type = UCC_TL_SPIN_RELIABILITY_PKT_FIN;
            request.proto.rank_id  = 0xDEAD;
            ib_qp_rc_post_send(ctx->reliability.qps[UCC_TL_SPIN_LN_QP_ID], NULL, NULL, 0, request.imm_data, task->id);
            ctx->reliability.ln_state = UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_FIN_SEND_COMPL;
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "ln[%zu]: SEND_FIN->WAIT_FIN_SEND_COMPL",
                     (size_t)UCC_TL_TEAM_RANK(ctx->team));
            break;
        case UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_FIN_SEND_COMPL:
            ctx->reliability.ln_state = UCC_TL_SPIN_RELIABILITY_PROTO_FINALIZE;
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "ln[%zu]: WAIT_SEND_COMPL->FINALIZE", 
                     (size_t)UCC_TL_TEAM_RANK(ctx->team));
            //fall through
        case UCC_TL_SPIN_RELIABILITY_PROTO_FINALIZE:
            break;
        default:
            ucc_assert_always(0);
    }
}

inline void
ucc_tl_spin_coll_worker_rx_reliability_rn(ucc_tl_spin_worker_info_t *ctx, struct ibv_wc *wc)
{
    ucc_tl_spin_task_t                  *task = ctx->team->cur_task;
    ucc_tl_spin_reliability_proto_info_t req;

    switch(ctx->reliability.rn_state) {
    case UCC_TL_SPIN_RELIABILITY_PROTO_INIT:
        ctx->reliability.rn_rbuf_info->addr = (uint64_t)task->dst_ptr;
        ctx->reliability.rn_rbuf_info->rkey = task->cached_rbuf_mkey->mr->rkey;
        ctx->reliability.rn_state           = UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_REQ;
        tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team),
                 "rn[%zu]: INIT->WAIT_REQ, buf metadata, addr: %p, rkey : %u",
                 (size_t)UCC_TL_TEAM_RANK(ctx->team), 
                 (void *)ctx->reliability.rn_rbuf_info->addr, 
                 ctx->reliability.rn_rbuf_info->rkey);
        break;
    case UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_LN_FETCH:
        ucc_assert_always(!wc);
        req = ctx->reliability.cached_req;
        if (ctx->reliability.recvd_per_rank[req.proto.rank_id] < task->pkts_to_send) {
            break;
        }
        ctx->reliability.rn_state = UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_REQ;
        tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "rn[%zu]: WAIT_LN_FETCH->WAIT_REQ", (size_t)UCC_TL_TEAM_RANK(ctx->team));
        // fall through
    case UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_REQ:
        if (wc) {
            if (wc->opcode == IBV_WC_SEND) {
                break;
            } else {
                ucc_assert_always(wc->opcode == IBV_WC_RECV);
                req.imm_data = wc->imm_data;
            }
        }
        switch(req.proto.pkt_type) {
            case UCC_TL_SPIN_RELIABILITY_PKT_NEED_FETCH:
                ucc_assert_always(req.proto.rank_id < UCC_TL_TEAM_SIZE(ctx->team));
                if ((req.proto.rank_id != UCC_TL_TEAM_RANK(ctx->team)) && 
                    (ctx->reliability.recvd_per_rank[req.proto.rank_id] < task->pkts_to_send)) {
                    ctx->reliability.cached_req = req;
                    ctx->reliability.rn_state   = UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_LN_FETCH;
                    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), 
                             "rn[%zu]: WAIT_REQ->WAIT_LN_FETCH, fetch_rank: %u, recvd_per_fetch_rank: %zu", 
                             (size_t)UCC_TL_TEAM_RANK(ctx->team), req.proto.rank_id, 
                             ctx->reliability.recvd_per_rank[req.proto.rank_id]);
                } else {
                    // re-post RWR to buffer subsequent Fetch requests of Fin
                    ib_qp_post_recv(ctx->reliability.qps[UCC_TL_SPIN_RN_QP_ID], NULL, NULL, 0, task->id);
                    ib_qp_rc_post_send(ctx->reliability.qps[UCC_TL_SPIN_RN_QP_ID],
                                       ctx->reliability.rn_rbuf_info_mr,
                                       ctx->reliability.rn_rbuf_info,
                                       sizeof(ucc_tl_spin_buf_info_t),
                                       0, task->id);
                    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team),
                             "rn[%zu]: WAIT_REC->WAIT_REQ, Sent the chunk request ACK for fetch_rank: %u, "
                             "recvd_per_fetch_rank: %zu",
                             (size_t)UCC_TL_TEAM_RANK(ctx->team), req.proto.rank_id, 
                             ctx->reliability.recvd_per_rank[req.proto.rank_id]);
                }
                break;
            case UCC_TL_SPIN_RELIABILITY_PKT_FIN:
                ucc_assert_always(req.proto.rank_id = 0xDEAD);
                ctx->reliability.rn_state = UCC_TL_SPIN_RELIABILITY_PROTO_FINALIZE;
                tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "rn[%zu]: WAIT_REQ->FINALIZE",
                         (size_t)UCC_TL_TEAM_RANK(ctx->team));
                break;
            default:
                ucc_assert_always(0);
        }
        break;
    case UCC_TL_SPIN_RELIABILITY_PROTO_FINALIZE:
        break;
    default:
        ucc_assert_always(0);
    }
}

inline ucc_status_t
ucc_tl_spin_coll_worker_rx_reliability_handler(ucc_tl_spin_worker_info_t *ctx)
{
    int           ncomps = 0;
    struct ibv_wc wc;

    ucc_assert_always(ctx->reliability.ln_state == UCC_TL_SPIN_RELIABILITY_PROTO_INIT);
    ucc_assert_always(ctx->reliability.rn_state == UCC_TL_SPIN_RELIABILITY_PROTO_INIT);

    // move left and right neighbor state machine from init state
    ucc_tl_spin_coll_worker_rx_reliability_ln(ctx, NULL);
    ucc_tl_spin_coll_worker_rx_reliability_rn(ctx, NULL);

    while ((ctx->reliability.ln_state != UCC_TL_SPIN_RELIABILITY_PROTO_FINALIZE) || 
           (ctx->reliability.rn_state != UCC_TL_SPIN_RELIABILITY_PROTO_FINALIZE)) 
    {
        ncomps = ib_cq_poll(ctx->reliability.cq, 1, &wc);
        if (ncomps != 1) {
            return UCC_ERR_NO_RESOURCE;
        }
        if (wc.qp_num == ctx->reliability.qps[UCC_TL_SPIN_LN_QP_ID]->qp_num) {
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "got ln_qp CQe, wr_id=%u", (uint32_t)wc.wr_id);
            ucc_tl_spin_coll_worker_rx_reliability_ln(ctx, &wc);
            if (ctx->reliability.rn_state == UCC_TL_SPIN_RELIABILITY_PROTO_WAIT_LN_FETCH) {
                tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "progress of rn without CQe");
                ucc_tl_spin_coll_worker_rx_reliability_rn(ctx, NULL);
            }
        } else if (wc.qp_num == ctx->reliability.qps[UCC_TL_SPIN_RN_QP_ID]->qp_num) {
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "got rn_qp CQe, wr_id=%u", (uint32_t)wc.wr_id);
            ucc_tl_spin_coll_worker_rx_reliability_rn(ctx, &wc);
        } else {
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "got wrong CQe");
            return UCC_ERR_NO_RESOURCE;
        }
    }
    return UCC_OK;
}

inline ucc_status_t
ucc_tl_spin_coll_worker_rx_handler(ucc_tl_spin_worker_info_t *ctx)
{
    ucc_tl_spin_task_t           *task                = ctx->team->cur_task;
#ifndef UCC_TL_SPIN_DISABLE_MCAST
    void                         *rbuf                = ctx->staging_rbuf[0];
    size_t                       *tail_idx            = &ctx->tail_idx[0];
    void                         *buf                 = task->dst_ptr + ctx->id * task->tx_thread_work;
    size_t                        mtu                 = ctx->ctx->mcast.mtu;
    double                        timeout             = task->timeout;
    uint32_t                      chunk_id            = 0;
    size_t                        rank_id             = 0;
    size_t                        rank_buf_offset;
    ucc_tl_spin_packed_chunk_id_t packed_chunk_id;
    double                        t_start, t_end;
    size_t                        pkt_len;
    struct ibv_wc                 wcs[2];
    int                           ncomps;

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), 
            "rx worker %u got bcast task of size: %zu n_batches: %zu last_batch_size: %zu last_pkt_sz: %zu buf: %p, tgid=%u", 
             ctx->id, task->tx_thread_work, task->n_batches, task->last_batch_size, task->last_pkt_size, buf, task->id);
#endif

    ctx->reliability.to_recv = task->pkts_to_recv;

#ifndef UCC_TL_SPIN_DISABLE_MCAST
    t_start                  = ucc_get_time();
    t_end                    = t_start;
    while (ctx->reliability.to_recv && ((t_end - t_start) < timeout)) {
        ncomps = ib_cq_try_poll(ctx->cq, 1, wcs);
        if (ncomps) {
            ucc_assert_always(ncomps == 1);
            ucc_assert_always(wcs->byte_len > 40);
            pkt_len = wcs->byte_len - 40;

            // ignore traffic from the previous iterations
            packed_chunk_id.imm_data = wcs->imm_data;
            if (packed_chunk_id.chunk_metadata.task_id != task->id) {
                tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "got task id: %u, expected id: %u", 
                         packed_chunk_id.chunk_metadata.task_id, task->id);
                goto repost_rwr;
            }

            chunk_id = packed_chunk_id.chunk_metadata.chunk_id;
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team),
                     "rx worker %u got bcasted chunk of size: %zu, id: %u, tail_idx: %zu",
                     ctx->id, pkt_len, chunk_id, *tail_idx);

            // ignore loopback traffic in cast of allgather
            if ((task->inplace_start_id <= chunk_id) && (chunk_id <= task->inplace_end_id)) {
                    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "got loopback chunk id");
                goto repost_rwr;
            }

            // ready to copy
            rank_id         = chunk_id / task->pkts_to_send;
            ucc_assert_always(rank_id < UCC_TL_TEAM_SIZE(ctx->team));
            rank_buf_offset = chunk_id % task->pkts_to_send;
            memcpy(buf + task->src_buf_size * rank_id + mtu * rank_buf_offset, 
                   rbuf + mtu * (*tail_idx),
                   pkt_len);
            ucc_tl_spin_bitmap_set_bit(&ctx->reliability.bitmap, chunk_id);
            ctx->reliability.recvd_per_rank[rank_id]++;
            ctx->reliability.to_recv--;

repost_rwr:
            ib_qp_post_recv_wr(ctx->qps[0], &ctx->rwrs[0][*tail_idx]);
            *tail_idx = (*tail_idx + 1) % ctx->ctx->cfg.mcast_qp_depth;
            ucc_assert_always(mtu * (*tail_idx) <= ctx->staging_rbuf_len);
            
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team),
                     "rx worker %u stored chunk of size: %zu, id: %u",
                     ctx->id, pkt_len, chunk_id);
        }
        t_end = ucc_get_time();
        ucc_assert_always(t_start <= t_end);
    }

    if (ctx->reliability.to_recv) {
        tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team),
                 "rx worker %u got timed out. remaining work of %zu. last recvd chunk id: %u",
                 ctx->id, ctx->reliability.to_recv, chunk_id);
    }

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "rx worker %u datapath finished and exits\n", ctx->id);
#endif
    return UCC_OK;
}