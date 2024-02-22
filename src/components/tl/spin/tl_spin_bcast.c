#include "tl_spin_bcast.h"
#include "tl_spin_p2p.h"
#include "tl_spin_mcast.h"

static ucc_status_t ucc_tl_spin_bcast_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t          *task            = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t          *team            = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_context_t       *ctx             = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_tl_spin_worker_info_t   *ctrl_ctx        = team->ctrl_ctx;
    ucc_rank_t                   root            = task->super.bargs.args.root;
    int                          reg_change_flag = 0;
    int                          comps           = 0;
    ucc_tl_spin_rcache_region_t *cache_entry;
    struct ibv_wc                wc[1];

    if (team->subset.myrank == root) {
        errno = 0;
        if (UCC_OK != ucc_rcache_get(ctx->rcache,
                                     task->base_ptr,
                                     task->buf_size,
                                     &reg_change_flag, 
                                     (ucc_rcache_region_t **)&cache_entry)) {
            tl_error(UCC_TASK_LIB(task), "Root failed to register send buffer memory errno: %d", errno);
            return UCC_ERR_NO_RESOURCE;
        }
        task->cached_mkey = cache_entry;
    } 

    // check if workers are busy
    if (team->cur_task) {
        ucc_assert_always(0); // test
        goto enqueue;
    } else {
        team->cur_task = task;
    }

    if (team->subset.myrank != root) {
        pthread_mutex_unlock(&team->rx_signal_mutex);
        team->rx_signal = UCC_TL_SPIN_WORKER_START;
        pthread_mutex_unlock(&team->rx_signal_mutex);
    }

    tl_debug(UCC_TASK_LIB(task), "barrier 1: rank %d", team->subset.myrank);
    // ring pass 1
    if (team->subset.myrank == 0) {
        ib_qp_rc_post_send(ctrl_ctx->qps[1], NULL, NULL, 0, 0);
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // send to the right neighbor completed
        tl_debug(UCC_TASK_LIB(task), "barrier pass 1: root rank, send OK");
        ucc_assert_always(comps == 1);
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // received data from the last rank in the ring (left neighbor)
        ucc_assert_always(comps == 1);
        ib_qp_post_recv(ctrl_ctx->qps[0], NULL, NULL, 0, 0);
        tl_debug(UCC_TASK_LIB(task), "barrier pass 1: root rank, recv OK");
    } else {
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // recv from the left neighbor
        ib_qp_post_recv(ctrl_ctx->qps[0], NULL, NULL, 0, 0);
        ucc_assert_always(comps == 1);
        tl_debug(UCC_TASK_LIB(task), "barrier pass 1: rank %d, recv OK", team->subset.myrank);
        ib_qp_rc_post_send(ctrl_ctx->qps[1], NULL, NULL, 0, 0); // send to right neighbor
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc);
        ucc_assert_always(comps == 1);
        tl_debug(UCC_TASK_LIB(task), "barrier pass 1: rank %d, send OK", team->subset.myrank);
    }

    // ring pass 2
    if (team->subset.myrank == 0) {
        ib_qp_rc_post_send(ctrl_ctx->qps[1], NULL, NULL, 0, 0);
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // send to the right neighbor completed
        tl_debug(UCC_TASK_LIB(task), "barrier pass 2: root rank, send OK");
        ucc_assert_always(comps == 1);
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // received data from the last rank in the ring (left neighbor)
        ucc_assert_always(comps == 1);
        ib_qp_post_recv(ctrl_ctx->qps[0], NULL, NULL, 0, 0);
        tl_debug(UCC_TASK_LIB(task), "barrier pass 2: root rank, recv OK");
    } else {
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // recv from the left neighbor
        ib_qp_post_recv(ctrl_ctx->qps[0], NULL, NULL, 0, 0);
        ucc_assert_always(comps == 1);
        tl_debug(UCC_TASK_LIB(task), "barrier pass 2: rank %d, recv OK", team->subset.myrank);
        ib_qp_rc_post_send(ctrl_ctx->qps[1], NULL, NULL, 0, 0); // send to right neighbor
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc);
        ucc_assert_always(comps == 1);
        tl_debug(UCC_TASK_LIB(task), "barrier pass 2: rank %d, send OK", team->subset.myrank);
    }

    // TODO: post work/task here

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
    int                    total_workers = is_root ? ctx->cfg.n_tx_workers : ctx->cfg.n_rx_workers;
    int                    workers_completed;

    if (task->id != team->cur_task->id) {
        coll_task->status = UCC_INPROGRESS;
    } else {
        ucc_assert_always(task == team->cur_task);
    }

    if (is_root) {
        pthread_mutex_lock(&team->tx_compls_mutex);
        workers_completed = team->tx_compls;
        pthread_mutex_unlock(&team->tx_compls_mutex);
    } else {
        pthread_mutex_lock(&team->rx_compls_mutex);
        workers_completed = team->rx_compls;
        pthread_mutex_unlock(&team->rx_compls_mutex);
    }

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
    } else {
        pthread_mutex_unlock(&team->rx_signal_mutex);
        team->rx_signal = UCC_TL_SPIN_WORKER_POLL;
        pthread_mutex_unlock(&team->rx_signal_mutex);
        pthread_mutex_lock(&team->rx_compls_mutex);
        team->rx_compls = 0;
        pthread_mutex_unlock(&team->rx_compls_mutex);
    }

    team->cur_task = NULL;

    if (is_root) {
        ucc_rcache_region_put(ctx->rcache, &task->cached_mkey->super);
    }

mpool_put:
    tl_debug(UCC_TASK_LIB(task), "finalize coll task ptr=%p tgid=%u", task, task->id);
    ucc_mpool_put(task);
    return UCC_OK;
}

ucc_status_t ucc_tl_spin_bcast_init(ucc_tl_spin_task_t   *task,
                                    ucc_base_coll_args_t *coll_args,
                                    ucc_tl_spin_team_t   *team)
{
    ucc_tl_spin_context_t *ctx         = UCC_TL_SPIN_TEAM_CTX(team);
    size_t                 count       = coll_args->args.src.info.count;
    size_t                 dt_size     = ucc_dt_size(coll_args->args.src.info.datatype);
    int                    n_workers   = ctx->cfg.n_tx_workers;

    ucc_assert_always(count * dt_size >= n_workers);
    ucc_assert_always(((count * dt_size) % n_workers) == 0);

    task->base_ptr        = coll_args->args.src.info.buffer;
    task->buf_size        = count * dt_size;
    task->per_thread_work = task->buf_size / n_workers;
    task->batch_bsize     = ctx->mcast.mtu * ctx->cfg.mcast_tx_batch_sz;    
    task->n_batches       = task->per_thread_work / task->batch_bsize;
    if (task->per_thread_work < task->batch_bsize) {
        ucc_assert_always(task->n_batches == 0);
        task->last_batch_size = task->per_thread_work / ctx->mcast.mtu;
    } else {
        task->last_batch_size = (task->per_thread_work - task->batch_bsize * task->n_batches) / ctx->mcast.mtu;
    }
    ucc_assert_always(task->per_thread_work >= (task->n_batches * task->batch_bsize + task->last_batch_size * ctx->mcast.mtu));
    task->last_pkt_size = task->per_thread_work - (task->n_batches * task->batch_bsize + task->last_batch_size * ctx->mcast.mtu);

    task->pkts_to_recv = task->per_thread_work / ctx->mcast.mtu;
    if (task->last_pkt_size) {
        task->pkts_to_recv++;
    }

    task->timeout = ((double)task->per_thread_work /
                    (double)ctx->cfg.link_bw) / 
                    1000000000.0 * 
                    (double)ctx->cfg.timeout_scaling_param;
    tl_debug(UCC_TL_SPIN_TEAM_LIB(team), "work: %zu, bw: %d, task timeout: %.16lf, scaling param: %d", 
             task->per_thread_work, ctx->cfg.link_bw, task->timeout, ctx->cfg.timeout_scaling_param);
    task->super.post      = ucc_tl_spin_bcast_start;
    task->super.progress  = ucc_tl_spin_bcast_progress;
    task->super.finalize  = ucc_tl_spin_bcast_finalize;
    task->coll_type       = UCC_COLL_TYPE_BCAST;
    tl_debug(UCC_TL_SPIN_TEAM_LIB(team), "got new task of size: %zu buf: %p", 
             count * dt_size, task->base_ptr);

    return UCC_OK;
}

inline ucc_status_t 
ucc_tl_spin_coll_worker_tx_bcast_start(ucc_tl_spin_worker_info_t *ctx)
{
    ucc_tl_spin_task_t           *task            = ctx->team->cur_task;
    size_t                        remaining_work  = task->per_thread_work;
    void                         *buf             = task->base_ptr + ctx->id * task->per_thread_work;
    int                           ncomps          = 0;
    ucc_tl_spin_packed_chunk_id_t packed_chunk_id;
    int                           i;
    struct ibv_wc                 wc;

    ucc_assert_always(ctx->signal != NULL);
    ucc_assert_always(ctx->n_mcg == 1);

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "tx worker %u got root start signal", ctx->id);
    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team),
            "tx worker %u got bcast task of size: %zu n_batches: %zu last_batch_size: %zu last_pkt_sz: %zu", 
             ctx->id, task->per_thread_work, task->n_batches, task->last_batch_size, task->last_pkt_size);

    packed_chunk_id.chunk_metadata.task_id  = task->id;
    packed_chunk_id.chunk_metadata.chunk_id = 0;

    for (i = 0; i < task->n_batches; i++) {
        ib_qp_ud_post_mcast_send_batch(ctx->qps[0], ctx->ahs[0],
                                       ctx->swrs[0], ctx->ssges[0],
                                       task->cached_mkey->mr,
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
                                       task->cached_mkey->mr,
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
                                 task->cached_mkey->mr,
                                 buf,
                                 task->last_pkt_size,
                                 packed_chunk_id.imm_data, 0);
        ncomps = ib_cq_poll(ctx->cq, 1, &wc);
        ucc_assert_always(ncomps == 1);
    }

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "tx worker %u finished multicasting\n", ctx->id);

    return UCC_OK;
}

inline ucc_status_t
ucc_tl_spin_coll_worker_rx_bcast_start(ucc_tl_spin_worker_info_t *ctx)
{
    ucc_tl_spin_task_t           *task     = ctx->team->cur_task;
    void                         *rbuf     = ctx->staging_rbuf[0];
    size_t                       *tail_idx = &ctx->tail_idx[0];
    void                         *buf      = task->base_ptr + ctx->id * task->per_thread_work;
    size_t                        to_recv  = task->pkts_to_recv;
    size_t                        mtu      = ctx->ctx->mcast.mtu;
    double                        timeout  = task->timeout;
    uint32_t                      chunk_id = 0;
    ucc_tl_spin_packed_chunk_id_t packed_chunk_id;
    double                        t_start, t_end;
    size_t                        pkt_len;
    struct ibv_wc                 wc[1];
    int                           ncomps;

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), 
            "rx worker %u got bcast task of size: %zu n_batches: %zu last_batch_size: %zu last_pkt_sz: %zu buf: %p", 
             ctx->id, task->per_thread_work, task->n_batches, task->last_batch_size, task->last_pkt_size, buf);

    t_start = ucc_get_time();
    t_end   = t_start;
    while (to_recv && ((t_end - t_start) < timeout)) {
        ncomps = ib_cq_try_poll(ctx->cq, 1, wc);
        if (ncomps) {
            ucc_assert_always(ncomps == 1);
            ucc_assert_always(wc->byte_len > 40);
            pkt_len  = wc->byte_len - 40;

            packed_chunk_id.imm_data = wc->imm_data;
            if (packed_chunk_id.chunk_metadata.task_id != task->id) {
                tl_error(UCC_TL_SPIN_TEAM_LIB(ctx->team), "got task id: %u, expected id: %u", 
                         packed_chunk_id.chunk_metadata.task_id, task->id);
                goto repost_rwr;
            }
            chunk_id = packed_chunk_id.chunk_metadata.chunk_id;
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team),
                     "rx worker %u got bcasted chunk of size: %zu, id: %u, tail_idx: %zu",
                     ctx->id, pkt_len, chunk_id, *tail_idx);

            memcpy(buf + mtu * chunk_id, rbuf + mtu * (*tail_idx), pkt_len);
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team),
                     "rx worker %u memcpy src: %p, dst: %p",
                     ctx->id, buf + mtu * chunk_id, rbuf + mtu * (*tail_idx));
            to_recv--;
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

    if (to_recv) {
        tl_error(UCC_TL_SPIN_TEAM_LIB(ctx->team),
                 "rx worker %u got timed out. remaining work of %zu. last recvd chunk id: %u",
                 ctx->id, to_recv, chunk_id);
    }

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "rx worker %u finished and exits\n", ctx->id);

    return UCC_OK;
}