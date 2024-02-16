#include "tl_spin.h"
#include "tl_spin_coll.h"
#include "tl_spin_mcast.h"
#include "tl_spin_p2p.h"

static uint32_t tgid = 0;

ucc_status_t ucc_tl_spin_bcast_init(ucc_tl_spin_task_t *task);

static ucc_status_t ucc_tl_spin_coll_finalize(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t *task = ucc_derived_of(coll_task, ucc_tl_spin_task_t);

    tl_debug(UCC_TASK_LIB(task), "finalizing coll task ptr=%p gid=%u", 
             task, task->dummy_task_id);
    ucc_mpool_put(task);
    return UCC_OK;
}

ucc_status_t ucc_tl_spin_coll_init(ucc_base_coll_args_t *coll_args,
                                   ucc_base_team_t *team,
                                   ucc_coll_task_t **task_h)
{
    ucc_tl_spin_context_t *ctx = ucc_derived_of(team->context, ucc_tl_spin_context_t);
    ucc_tl_spin_task_t *task   = NULL;
    ucc_status_t status        = UCC_OK;

    task = ucc_mpool_get(&ctx->req_mp);
    ucc_coll_task_init(&task->super, coll_args, team);

    task->super.finalize = ucc_tl_spin_coll_finalize;

    switch (coll_args->args.coll_type) {
    case UCC_COLL_TYPE_BCAST:
        status = ucc_tl_spin_bcast_init(task);
        break;
    default:
        tl_debug(UCC_TASK_LIB(task),
                 "collective %d is not supported",
                 coll_args->args.coll_type);
        status = UCC_ERR_NOT_SUPPORTED;
        goto err;
    }

    task->dummy_task_id = tgid++;

    tl_debug(UCC_TASK_LIB(task), "init coll task ptr=%p tgid=%u", 
             task, task->dummy_task_id);
    *task_h = &task->super;
    return status;

err:
    ucc_mpool_put(task);
    return status;
}

ucc_status_t ucc_tl_spin_bcast_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t        *task     = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t        *team     = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_worker_info_t *ctrl_ctx = team->ctrl_ctx;
    ucc_rank_t                 root     = task->super.bargs.args.root;
    int                        comps    = 0;
    struct ibv_wc              wc[1];

    ucc_debug("barrier 1: rank %d", team->subset.myrank);
    // ring pass 1
    if (team->subset.myrank == 0) {
        ib_qp_rc_post_send(ctrl_ctx->qps[1], NULL, NULL, 0, 0);
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // send to the right neighbor completed
        ucc_debug("barrier pass 1: root rank, send OK");
        ucc_assert_always(comps == 1);
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // received data from the last rank in the ring (left neighbor)
        ucc_assert_always(comps == 1);
        ib_qp_post_recv(ctrl_ctx->qps[0], NULL, NULL, 0, 0);
        ucc_debug("barrier pass 1: root rank, recv OK");
    } else {
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // recv from the left neighbor
        ib_qp_post_recv(ctrl_ctx->qps[0], NULL, NULL, 0, 0);
        ucc_assert_always(comps == 1);
        ucc_debug("barrier pass 1: rank %d, recv OK", team->subset.myrank);
        ib_qp_rc_post_send(ctrl_ctx->qps[1], NULL, NULL, 0, 0); // send to right neighbor
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc);
        ucc_assert_always(comps == 1);
        ucc_debug("barrier pass 1: rank %d, send OK", team->subset.myrank);
    }

    // ring pass 2
    if (team->subset.myrank == 0) {
        ib_qp_rc_post_send(ctrl_ctx->qps[1], NULL, NULL, 0, 0);
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // send to the right neighbor completed
        ucc_debug("barrier pass 2: root rank, send OK");
        ucc_assert_always(comps == 1);
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // received data from the last rank in the ring (left neighbor)
        ucc_assert_always(comps == 1);
        ib_qp_post_recv(ctrl_ctx->qps[0], NULL, NULL, 0, 0);
        ucc_debug("barrier pass 2: root rank, recv OK");
    } else {
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // recv from the left neighbor
        ib_qp_post_recv(ctrl_ctx->qps[0], NULL, NULL, 0, 0);
        ucc_assert_always(comps == 1);
        ucc_debug("barrier pass 2: rank %d, recv OK", team->subset.myrank);
        ib_qp_rc_post_send(ctrl_ctx->qps[1], NULL, NULL, 0, 0); // send to right neighbor
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc);
        ucc_assert_always(comps == 1);
        ucc_debug("barrier pass 2: rank %d, send OK", team->subset.myrank);
    }

    // start workers
    if (team->subset.myrank == root) {
        pthread_mutex_lock(&team->tx_signal_mutex);
        team->tx_signal = UCC_TL_SPIN_WORKER_START;
        pthread_mutex_unlock(&team->tx_signal_mutex);
    } else {
        pthread_mutex_unlock(&team->rx_signal_mutex);
        team->rx_signal = UCC_TL_SPIN_WORKER_START;
        pthread_mutex_unlock(&team->rx_signal_mutex);
    }

    coll_task->status = UCC_INPROGRESS;
    tl_debug(UCC_TASK_LIB(task), "start coll task ptr=%p tgid=%u",
             task, task->dummy_task_id);
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_spin_bcast_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t    *task          = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t    *team          = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_context_t *ctx           = UCC_TL_SPIN_TEAM_CTX(team);
    int                    is_root       = team->subset.myrank == task->super.bargs.args.root ? 1 : 0;
    int                    total_workers = is_root ? ctx->cfg.n_tx_workers : ctx->cfg.n_rx_workers;
    int                    workers_completed;

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

    //tl_debug(UCC_TASK_LIB(task), "progress coll task ptr=%p tgid=%u", task, task->dummy_task_id);
}

ucc_status_t ucc_tl_spin_bcast_finalize(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t    *task    = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t    *team    = UCC_TL_SPIN_TASK_TEAM(task);
    int                    is_root = team->subset.myrank == task->super.bargs.args.root ? 1 : 0;

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

    tl_debug(UCC_TASK_LIB(task), "finalize coll task ptr=%p tgid=%u",
             task, task->dummy_task_id);
    ucc_mpool_put(task);
    return UCC_OK;
}

ucc_status_t ucc_tl_spin_bcast_init(ucc_tl_spin_task_t *task)
{
    task->super.post     = ucc_tl_spin_bcast_start;
    task->super.progress = ucc_tl_spin_bcast_progress;
    task->super.finalize = ucc_tl_spin_bcast_finalize;
    return UCC_OK;
}

inline ucc_status_t 
ucc_tl_spin_coll_worker_tx_start(ucc_tl_spin_worker_info_t *ctx)
{
    char *buf             = ucc_calloc(1, 2048);
    struct ibv_mr *buf_mr = ibv_reg_mr(ctx->ctx->mcast.pd, buf, 2048, IBV_ACCESS_REMOTE_WRITE |
                                                                      IBV_ACCESS_LOCAL_WRITE  |
                                                                      IBV_ACCESS_REMOTE_READ);
    int ncomps            = 0;
    struct ibv_wc wc[1];

    ucc_debug("tx thread got root start signal\n");
    ucc_assert_always(buf && buf_mr);
    ucc_assert_always(ctx->signal != NULL);

    ib_qp_ud_post_mcast_send(ctx->qps[0], ctx->ahs[0], buf_mr, buf, 2048, 0);
    ncomps = ib_cq_poll(ctx->cq, 1, wc);
    ucc_assert_always(ncomps == 1);
    ucc_debug("tx thread sent multicast\n");

    ibv_dereg_mr(buf_mr);
    ucc_free(buf);

    return UCC_OK;
}

inline ucc_status_t
ucc_tl_spin_coll_worker_rx_start(ucc_tl_spin_worker_info_t *ctx)
{
    struct ibv_wc wc[1];
    int           ncomps;

    ucc_debug("rx thread started\n");
    ncomps = ib_cq_poll(ctx->cq, 1, wc);
    ucc_assert_always(ncomps == 1);
    ucc_debug("rx thread got the multicasted buffer\n");
    ucc_debug("rx thread exits\n");

    return UCC_OK;
}

void *ucc_tl_spin_coll_worker_main(void *arg)
{
    ucc_tl_spin_worker_info_t *ctx       = (ucc_tl_spin_worker_info_t *)arg;
    int                        completed = 0;
    ucc_status_t               status;
    int                        signal;

    ucc_debug("worker thread started\n");

poll:
    pthread_mutex_lock(ctx->signal_mutex);
    signal = *(ctx->signal);
    pthread_mutex_unlock(ctx->signal_mutex);
    
    switch (signal) {

    case (UCC_TL_SPIN_WORKER_POLL):
        completed = 0;
        goto poll;
        break;

    case (UCC_TL_SPIN_WORKER_START):
        if (completed) {
            goto poll;
        }

        switch (ctx->type) {

        case (UCC_TL_SPIN_WORKER_TYPE_TX):
            status = ucc_tl_spin_coll_worker_tx_start(ctx);
            break;

        case (UCC_TL_SPIN_WORKER_TYPE_RX):
            status = ucc_tl_spin_coll_worker_rx_start(ctx);
            break;

        default:
            ucc_debug("worker thread shouldn't be here");
            ucc_assert_always(0);
        }

        ucc_assert_always(status == UCC_OK);

        completed = 1;
        pthread_mutex_lock(ctx->compls_mutex);
        (*(ctx->compls))++;
        pthread_mutex_unlock(ctx->compls_mutex);
        break;

    case (UCC_TL_SPIN_WORKER_FIN):
        break;

    default:
        ucc_debug("worker thread shouldn't be here\n");
        ucc_assert_always(0);
        break;
    }

    ucc_debug("worker thread exits\n");

    return arg;
}