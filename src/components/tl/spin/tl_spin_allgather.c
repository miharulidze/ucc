#include "tl_spin_allgather.h"
#include "tl_spin_p2p.h"
#include "tl_spin_mcast.h"

static ucc_status_t ucc_tl_spin_allgather_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t        *task     = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t        *team     = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_worker_info_t *ctrl_ctx = team->ctrl_ctx;
    int                        comps    = 0;
    struct ibv_wc              wc[1];
    
    if (team->cur_task) {
        ucc_assert_always(0); // test
        goto enqueue;
    } else {
        team->cur_task = task;
    }

    pthread_mutex_unlock(&team->rx_signal_mutex);
    team->rx_signal = UCC_TL_SPIN_WORKER_START;
    pthread_mutex_unlock(&team->rx_signal_mutex);

    // prepost multicast sequence signaling 
    if (!task->ag.mcast_seq_starter) {
        ib_qp_post_recv(ctrl_ctx->qps[1], NULL, NULL, 0, 0);
    }

    tl_debug(UCC_TASK_LIB(task), "barrier 1: rank %d", team->subset.myrank);
    // ring pass 1
    if (team->subset.myrank == 0) {
        ib_qp_rc_post_send(ctrl_ctx->qps[0], NULL, NULL, 0, 0);
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // send to the right neighbor completed
        tl_debug(UCC_TASK_LIB(task), "barrier pass 1: root rank, send OK");
        ucc_assert_always(comps == 1);
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // received data from the last rank in the ring (left neighbor)
        ucc_assert_always(comps == 1);
        ib_qp_post_recv(ctrl_ctx->qps[1], NULL, NULL, 0, 0);
        tl_debug(UCC_TASK_LIB(task), "barrier pass 1: root rank, recv OK");
    } else {
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // recv from the left neighbor
        ib_qp_post_recv(ctrl_ctx->qps[1], NULL, NULL, 0, 0);
        ucc_assert_always(comps == 1);
        tl_debug(UCC_TASK_LIB(task), "barrier pass 1: rank %d, recv OK", team->subset.myrank);
        ib_qp_rc_post_send(ctrl_ctx->qps[0], NULL, NULL, 0, 0); // send to right neighbor
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc);
        ucc_assert_always(comps == 1);
        tl_debug(UCC_TASK_LIB(task), "barrier pass 1: rank %d, send OK", team->subset.myrank);
    }

    // ring pass 2
    if (team->subset.myrank == 0) {
        ib_qp_rc_post_send(ctrl_ctx->qps[0], NULL, NULL, 0, 0);
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // send to the right neighbor completed
        tl_debug(UCC_TASK_LIB(task), "barrier pass 2: root rank, send OK");
        ucc_assert_always(comps == 1);
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // received data from the last rank in the ring (left neighbor)
        ucc_assert_always(comps == 1);
        ib_qp_post_recv(ctrl_ctx->qps[1], NULL, NULL, 0, 0);
        tl_debug(UCC_TASK_LIB(task), "barrier pass 2: root rank, recv OK");
    } else {
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc); // recv from the left neighbor
        ib_qp_post_recv(ctrl_ctx->qps[1], NULL, NULL, 0, 0);
        ucc_assert_always(comps == 1);
        tl_debug(UCC_TASK_LIB(task), "barrier pass 2: rank %d, recv OK", team->subset.myrank);
        ib_qp_rc_post_send(ctrl_ctx->qps[0], NULL, NULL, 0, 0); // send to right neighbor
        comps = ib_cq_poll(ctrl_ctx->cq, 1, wc);
        ucc_assert_always(comps == 1);
        tl_debug(UCC_TASK_LIB(task), "barrier pass 2: rank %d, send OK", team->subset.myrank);
    }

    pthread_mutex_unlock(&team->tx_signal_mutex);
    team->tx_signal = UCC_TL_SPIN_WORKER_START;
    pthread_mutex_unlock(&team->tx_signal_mutex);

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
    ucc_tl_spin_task_t    *task = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t    *team = UCC_TL_SPIN_TASK_TEAM(task);
    
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
    
    pthread_mutex_unlock(&team->rx_signal_mutex);
    team->rx_signal = UCC_TL_SPIN_WORKER_POLL;
    pthread_mutex_unlock(&team->rx_signal_mutex);
    pthread_mutex_lock(&team->rx_compls_mutex);
    team->rx_compls = 0;
    pthread_mutex_unlock(&team->rx_compls_mutex);

    team->cur_task = NULL;

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
    uint64_t               seq_length;

    ucc_assert_always(UCC_TL_TEAM_SIZE(team) % ctx->cfg.n_ag_mcast_roots == 0);
    seq_length = UCC_TL_TEAM_SIZE(team) / ctx->cfg.n_ag_mcast_roots;

    task->ag.mcast_seq_starter  = team->subset.myrank       % seq_length == 0 ? 1 : 0;
    task->ag.mcast_seq_finisher = (team->subset.myrank + 1) % seq_length == 0 ? 1 : 0;

    task->super.post      = ucc_tl_spin_allgather_start;
    task->super.progress  = ucc_tl_spin_allgather_progress;
    task->super.finalize  = ucc_tl_spin_allgather_finalize;
    task->coll_type       = UCC_COLL_TYPE_ALLGATHER;

    tl_debug(UCC_TL_SPIN_TEAM_LIB(team), "got new task, seq_starter: %d seq_finisher: %d", 
             task->ag.mcast_seq_starter, task->ag.mcast_seq_finisher);

    return UCC_OK;
}

ucc_status_t
ucc_tl_spin_coll_worker_tx_allgather_start(ucc_tl_spin_worker_info_t *ctx)
{
    ucc_tl_spin_task_t *task     = ctx->team->cur_task;
    int                 compls;
    struct ibv_wc       wc[1];

    if (!task->ag.mcast_seq_starter) {
        compls = ib_cq_poll(ctx->team->ctrl_ctx->cq, 1, wc);
        ucc_assert_always(compls == 1);
        tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "got mcast signal from left neighbor");
    }

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "multicasted");

    pthread_mutex_lock(&ctx->team->tx_compls_mutex);
    compls = ctx->team->tx_compls++;
    pthread_mutex_unlock(&ctx->team->tx_compls_mutex);

    if (((compls + 1) == ctx->ctx->cfg.n_tx_workers) && !task->ag.mcast_seq_finisher) {
        tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "sending mcast signal");
        ib_qp_rc_post_send(ctx->team->ctrl_ctx->qps[0], NULL, NULL, 0, 0);
        compls = ib_cq_poll(ctx->team->ctrl_ctx->cq, 1, wc);
        ucc_assert_always(compls == 1);
        tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "sent mcast signal to right neighbor. is finisher: %d", task->ag.mcast_seq_finisher);
    }

    return UCC_OK;
}

ucc_status_t
ucc_tl_spin_coll_worker_rx_allgather_start(ucc_tl_spin_worker_info_t *ctx)
{
    pthread_mutex_lock(&ctx->team->rx_compls_mutex);
    ctx->team->rx_compls++;
    pthread_mutex_unlock(&ctx->team->rx_compls_mutex);
    return UCC_OK;
}