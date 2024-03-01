#include "tl_spin_allgather.h"
#include "tl_spin_bcast.h"
#include "tl_spin_p2p.h"
#include "tl_spin_mcast.h"
#include "components/mc/ucc_mc.h"

static ucc_status_t ucc_tl_spin_allgather_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t          *task        = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t          *team        = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_context_t       *ctx         = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_tl_spin_worker_info_t   *ctrl_ctx    = team->ctrl_ctx;
    int                          reg_changed = 0;
    ucc_tl_spin_rcache_region_t *cache_entry;
    ucc_status_t                 status;

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

    ib_qp_post_recv(team->workers[1].reliability.qps[UCC_TL_SPIN_RN_QP_ID], NULL, NULL, 0, task->id);

    pthread_mutex_lock(&team->rx_signal_mutex);
    team->rx_signal = UCC_TL_SPIN_WORKER_START;
    pthread_mutex_unlock(&team->rx_signal_mutex);

    ucc_tl_spin_team_rc_ring_barrier(team->subset.myrank, ctrl_ctx);

    pthread_mutex_lock(&team->tx_signal_mutex);
    team->tx_signal = UCC_TL_SPIN_WORKER_START;
    pthread_mutex_unlock(&team->tx_signal_mutex);

    if (!UCC_IS_INPLACE(task->super.bargs.args)) {
        tl_debug(UCC_TASK_LIB(task), "in place memcpy, src: %p, dst: %p, size: %zu", 
                 PTR_OFFSET(task->dst_ptr, task->src_buf_size * UCC_TL_TEAM_RANK(team)), 
                 task->src_ptr,
                 task->src_buf_size);
        status = ucc_mc_memcpy(PTR_OFFSET(task->dst_ptr, task->src_buf_size * UCC_TL_TEAM_RANK(team)),
                               task->src_ptr,
                               task->src_buf_size,
                               task->super.bargs.args.dst.info.mem_type, 
                               task->super.bargs.args.dst.info.mem_type);
        if (ucc_unlikely(UCC_OK != status)) {
            return status;
        }
    }

enqueue:
    coll_task->status = UCC_INPROGRESS;
    tl_debug(UCC_TASK_LIB(task), "start coll task ptr=%p tgid=%u", task, task->id);
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

static void ucc_tl_spin_allgather_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t    *task              = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t    *team              = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_context_t *ctx               = UCC_TL_SPIN_TEAM_CTX(team);
    int                    total_workers     = ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers;
    int                    workers_completed = 0;

    if (task->id != team->cur_task->id) {
        coll_task->status = UCC_INPROGRESS;
    } else {
        ucc_assert_always(task == team->cur_task);
    }

    pthread_mutex_lock(&team->tx_compls_mutex);
    workers_completed += team->tx_compls;
    pthread_mutex_unlock(&team->tx_compls_mutex);
    pthread_mutex_lock(&team->rx_compls_mutex);
    workers_completed += team->rx_compls;
    pthread_mutex_unlock(&team->rx_compls_mutex);

    if (workers_completed == total_workers) {
        coll_task->status = UCC_OK;
    } else {
        coll_task->status = UCC_INPROGRESS;
    }
}

static ucc_status_t ucc_tl_spin_allgather_finalize(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t    *task    = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t    *team    = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_context_t *ctx     = UCC_TL_SPIN_TEAM_CTX(team);

    if (task->id != team->cur_task->id) {
        ucc_assert_always(0); // atm I don't know are there any scenario when we get into this branch
        goto mpool_put;
    } else {
        ucc_assert_always(task == team->cur_task);
    }

    pthread_mutex_lock(&team->tx_signal_mutex);
    team->tx_signal = UCC_TL_SPIN_WORKER_POLL;
    pthread_mutex_unlock(&team->tx_signal_mutex);
    pthread_mutex_lock(&team->tx_compls_mutex);
    team->tx_compls = 0;
    pthread_mutex_unlock(&team->tx_compls_mutex);

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

    ucc_rcache_region_put(ctx->p2p.rcache, &task->cached_sbuf_mkey->super);
    ucc_rcache_region_put(ctx->mcast.rcache, &task->cached_rbuf_mkey->super);
mpool_put:
    tl_debug(UCC_TASK_LIB(task), "finalize coll task ptr=%p tgid=%u", task, task->id);
    ucc_mpool_put(task);
    return UCC_OK;
}

ucc_status_t ucc_tl_spin_allgather_init(ucc_tl_spin_task_t   *task,
                                        ucc_base_coll_args_t *coll_args,
                                        ucc_tl_spin_team_t   *team)
{
    ucc_tl_spin_context_t *ctx = UCC_TL_SPIN_TEAM_CTX(team);
    size_t                 count       = coll_args->args.dst.info.count;
    size_t                 dt_size     = ucc_dt_size(coll_args->args.dst.info.datatype);
    uint64_t               seq_length;

    ucc_assert_always(UCC_TL_TEAM_SIZE(team) % ctx->cfg.n_ag_mcast_roots == 0);
    seq_length = UCC_TL_TEAM_SIZE(team) / ctx->cfg.n_ag_mcast_roots;

    task->src_buf_size = count * dt_size / UCC_TL_TEAM_SIZE(team);
    task->dst_buf_size = count * dt_size;
    ucc_assert_always(task->dst_buf_size <= ctx->cfg.max_recv_buf_size);
    ucc_assert_always(task->dst_buf_size % UCC_TL_TEAM_SIZE(team) == 0);
    
    task->dst_ptr = coll_args->args.dst.info.buffer;
    if (!UCC_IS_INPLACE(task->super.bargs.args)) {
        task->src_ptr = coll_args->args.src.info.buffer;
    } else {
        task->src_ptr = task->dst_ptr + UCC_TL_TEAM_RANK(team) * task->src_buf_size;
    }
    
    ucc_tl_spin_bcast_init_task_tx_info(task, ctx);

    task->pkts_to_send = task->tx_thread_work / ctx->mcast.mtu;
    if (task->last_pkt_size) {
        task->pkts_to_send++;
    }
    task->pkts_to_recv     = task->pkts_to_send * (UCC_TL_TEAM_SIZE(team) - 1);
    task->start_chunk_id   = task->pkts_to_send * UCC_TL_TEAM_RANK(team);

    task->inplace_start_id = task->start_chunk_id;
    task->inplace_end_id   = task->inplace_start_id + task->pkts_to_send - 1;

    task->ag.mcast_seq_starter  = UCC_TL_TEAM_RANK(team)       % seq_length == 0 ? 1 : 0;
    task->ag.mcast_seq_finisher = (UCC_TL_TEAM_RANK(team) + 1) % seq_length == 0 ? 1 : 0;

    task->super.post      = ucc_tl_spin_allgather_start;
    task->super.progress  = ucc_tl_spin_allgather_progress;
    task->super.finalize  = ucc_tl_spin_allgather_finalize;
    task->coll_type       = UCC_COLL_TYPE_ALLGATHER;

    ucc_assert_always(ctx->cfg.n_tx_workers == ctx->cfg.n_rx_workers);
    task->timeout = (((double)task->tx_thread_work * (UCC_TL_TEAM_SIZE(team) - 1)) /
                    (double)ctx->cfg.link_bw) /
                    1000000000.0 * 
                    (double)ctx->cfg.timeout_scaling_param;

    tl_debug(UCC_TL_SPIN_TEAM_LIB(team), "got new task, "
             "ag_seq_starter: %d ag_seq_finisher: %d, "
             "src_ptr: %p, src_buf_size: %zu, dst_ptr: %p, dst_buf_size: %zu "
             "pkts_to_send: %zu, pkts_to_recv: %zu",
             task->ag.mcast_seq_starter, task->ag.mcast_seq_finisher,
             task->src_ptr, task->src_buf_size, task->dst_ptr, task->dst_buf_size,
             task->pkts_to_send, task->pkts_to_recv);

    return UCC_OK;
}

ucc_status_t
ucc_tl_spin_coll_worker_tx_allgather_start(ucc_tl_spin_worker_info_t *ctx)
{
    ucc_tl_spin_task_t *task     = ctx->team->cur_task;
    ucc_status_t        status;
    int                 compls;
    struct ibv_wc       wc[1];

    if (!task->ag.mcast_seq_starter) {
        compls = ib_cq_poll(ctx->team->ctrl_ctx->cq, 1, wc);
        ib_qp_post_recv(ctx->team->ctrl_ctx->qps[1], NULL, NULL, 0, 0); // re-post
        ucc_assert_always(compls == 1);
        tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "got mcast signal from left neighbor");
    }

    status = ucc_tl_spin_coll_worker_tx_handler(ctx);
    ucc_assert_always(status == UCC_OK);

    if (!task->ag.mcast_seq_finisher) {
        tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "sending mcast signal");
        ib_qp_rc_post_send(ctx->team->ctrl_ctx->qps[0], NULL, NULL, 0, 0, 0);
        compls = ib_cq_poll(ctx->team->ctrl_ctx->cq, 1, wc);
        ucc_assert_always(compls == 1);
        tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "sent mcast signal to right neighbor. is finisher: %d", task->ag.mcast_seq_finisher);
    }

    pthread_mutex_lock(&ctx->team->tx_compls_mutex);
    compls = ctx->team->tx_compls++;
    pthread_mutex_unlock(&ctx->team->tx_compls_mutex);

    return UCC_OK;
}

ucc_status_t
ucc_tl_spin_coll_worker_rx_allgather_start(ucc_tl_spin_worker_info_t *ctx)
{
    ucc_status_t status;

    status = ucc_tl_spin_coll_worker_rx_handler(ctx);
    ucc_assert_always(status == UCC_OK);
    status  = ucc_tl_spin_coll_worker_rx_reliability_handler(ctx);
    ucc_assert_always(status == UCC_OK);

    pthread_mutex_lock(&ctx->team->rx_compls_mutex);
    ctx->team->rx_compls++;
    pthread_mutex_unlock(&ctx->team->rx_compls_mutex);

    return UCC_OK;
}