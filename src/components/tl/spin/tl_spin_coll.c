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
    ucc_tl_spin_task_t *task            = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t *team            = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_worker_info_t *ctrl_ctx = team->ctrl_ctx;
    struct ibv_wc              wc[1];
    int comps = 0;

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

    coll_task->status = UCC_INPROGRESS;
    tl_debug(UCC_TASK_LIB(task), "start coll task ptr=%p tgid=%u",
             task, task->dummy_task_id);
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_spin_bcast_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t *task = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t *team = UCC_TL_SPIN_TASK_TEAM(task);
    //ucc_rank_t          root = task->super.bargs.args.root;

    pthread_mutex_lock(&team->signal_mutex);
    if (team->subset.myrank == 0) {
        ucc_debug("signaling to tx worker");
        team->workers_signal = UCC_TL_SPIN_WORKER_START;
    } else {
        team->workers_signal = UCC_TL_SPIN_WORKER_IGNORE_TX;
    }
    pthread_mutex_unlock(&team->signal_mutex);

    while (1) {
        // TODO
    }

    coll_task->status = UCC_OK;
    tl_debug(UCC_TASK_LIB(task), "progress coll task ptr=%p tgid=%u",
             task, task->dummy_task_id);
}

ucc_status_t ucc_tl_spin_bcast_finalize(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t *task = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
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

void *ucc_tl_spin_coll_worker_tx(void *arg)
{
    ucc_tl_spin_worker_info_t *ctx = (ucc_tl_spin_worker_info_t *)arg;
    char *buf = ucc_calloc(1, 2048);
    struct ibv_mr *buf_mr = ibv_reg_mr(ctx->ctx->mcast.pd, buf, 2048, IBV_ACCESS_REMOTE_WRITE |
                                                                      IBV_ACCESS_LOCAL_WRITE  |
                                                                      IBV_ACCESS_REMOTE_READ);
    struct ibv_wc wc[1];
    int ncomps = 0;
    int signal;

    ucc_debug("tx thread started\n");

    ucc_assert_always(buf && buf_mr);
    ucc_assert_always(ctx->signal != NULL);

poll:
    pthread_mutex_lock(ctx->signal_mutex);
    signal = *(ctx->signal);
    pthread_mutex_unlock(ctx->signal_mutex);

    switch (signal) {
    case (UCC_TL_SPIN_WORKER_INIT):
        goto poll;
        break;
    case (UCC_TL_SPIN_WORKER_START):
        ucc_debug("tx thread got root start signal\n");
        ib_qp_ud_post_mcast_send(ctx->qps[0], ctx->ahs[0], buf_mr, buf, 2048, 0);
        ncomps = ib_cq_poll(ctx->cq, 1, wc);
        ucc_assert_always(ncomps == 1);
        ucc_debug("tx thread sent multicast\n");
        break;
    case (UCC_TL_SPIN_WORKER_IGNORE_TX):
        break;
    default:
        printf("tx thread shouldn't be here\n");
        break;
    }

    ibv_dereg_mr(buf_mr);
    ucc_free(buf);
    ucc_debug("tx thread exits\n");

    return arg;
}

void * ucc_tl_spin_coll_worker_rx(void *arg)
{
    ucc_tl_spin_worker_info_t *ctx = (ucc_tl_spin_worker_info_t *)arg;
    struct ibv_wc wc[1];
    int ncomps;

    ucc_debug("rx thread started\n");
    ncomps = ib_cq_poll(ctx->cq, 1, wc);
    ucc_assert_always(ncomps == 1);
    ucc_debug("rx thread got the multicasted buffer\n");
    ucc_debug("rx thread exits\n");

    return arg;
}