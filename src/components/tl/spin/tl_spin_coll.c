#include "tl_spin.h"
#include "tl_spin_coll.h"
#include "tl_spin_p2p.h"
#include "tl_spin_mcast.h"
#include "tl_spin_rbuf.h"
#include "tl_spin_bcast.h"
#include "tl_spin_allgather.h"
#include "tl_spin_bitmap.h"

ucc_status_t ucc_tl_spin_coll_init(ucc_base_coll_args_t *coll_args,
                                   ucc_base_team_t *tl_team,
                                   ucc_coll_task_t **task_h)
{
    ucc_tl_spin_context_t *ctx    = ucc_derived_of(tl_team->context, ucc_tl_spin_context_t);
    ucc_tl_spin_team_t    *team   = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_task_t    *task   = NULL;
    ucc_status_t           status = UCC_OK;

    task = ucc_mpool_get(&ctx->req_mp);
    ucc_coll_task_init(&task->super, coll_args, tl_team);

    switch (coll_args->args.coll_type) {
    case UCC_COLL_TYPE_BCAST:
        status = ucc_tl_spin_bcast_init(task, coll_args, team);
        break;
    case UCC_COLL_TYPE_ALLGATHER:
        status = ucc_tl_spin_allgather_init(task, coll_args, team);
        break;
    default:
        tl_debug(UCC_TASK_LIB(task),
                 "collective %d is not supported",
                 coll_args->args.coll_type);
        status = UCC_ERR_NOT_SUPPORTED;
        goto err;
    }

    task->id = team->task_id++ % UCC_TL_SPIN_MAX_TASKS;

    tl_debug(UCC_TASK_LIB(task), "init coll task ptr=%p tgid=%u", task, task->id);
    *task_h = &task->super;
    return status;

err:
    ucc_mpool_put(task);
    return status;
}

inline ucc_status_t 
ucc_tl_spin_coll_activate_workers(ucc_tl_spin_task_t *task)
{
    ucc_tl_spin_team_t    *team = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_context_t *ctx  = UCC_TL_SPIN_TEAM_CTX(team);
    int i;
 
    task->tx_start  = 0;
    task->tx_compls = 0;
    task->rx_compls = 0;
    // barrier 1
    ucc_tl_spin_team_rc_ring_barrier(team->subset.myrank, team->ctrl_ctx);
    if (rbuf_has_space(&team->task_rbuf)) {
        // prepost right neighbor reliability signal. Ugly.
        for (i = 0; i < (ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers); i++) {
            if ((task->coll_type == UCC_TL_SPIN_WORKER_TASK_TYPE_ALLGATHER) &&
                (team->workers[i].type == UCC_TL_SPIN_WORKER_TYPE_TX) && 
                !task->ag.mcast_seq_starter) {
                    ib_qp_post_recv(team->workers[i].reliability.qps[UCC_TL_SPIN_RN_QP_ID], NULL, NULL, 0, task->id);
                }
            if (team->workers[i].type == UCC_TL_SPIN_WORKER_TYPE_RX) {
                ib_qp_post_recv(team->workers[i].reliability.qps[UCC_TL_SPIN_RN_QP_ID], NULL, NULL, 0, task->id);
            }
        }
        rbuf_push_head(&team->task_rbuf, (uintptr_t)task);
        // rx threads might already see the task and start polling
    } else {
        return UCC_ERR_NO_RESOURCE;
    }
    // barrier 2
    ucc_tl_spin_team_rc_ring_barrier(team->subset.myrank, team->ctrl_ctx);
    // fire up tx threads
    task->tx_start = 1;
    return UCC_OK;
}

inline void 
ucc_tl_spin_coll_progress(ucc_tl_spin_task_t *task, ucc_status_t *coll_status)
{
    ucc_tl_spin_team_t    *team = UCC_TL_SPIN_TASK_TEAM(task);
    ucc_tl_spin_context_t *ctx  = UCC_TL_SPIN_TEAM_CTX(team);
    if ((task->tx_compls + task->rx_compls) != (ctx->cfg.n_rx_workers + ctx->cfg.n_rx_workers)) {
        *coll_status = UCC_INPROGRESS;
        return;
    }
    rbuf_pop_tail(&team->task_rbuf);
    *coll_status = UCC_OK;
}

ucc_status_t ucc_tl_spin_coll_finalize(ucc_tl_spin_task_t *task)
{
    tl_debug(UCC_TASK_LIB(task), "finalizing coll task ptr=%p gid=%u", task, task->id);
    ucc_mpool_put(task);
    return UCC_OK;
}

void ucc_tl_spin_coll_post_kill_task(ucc_tl_spin_team_t *team)
{
    ucc_tl_spin_context_t *ctx  = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_tl_spin_task_t    *task = ucc_mpool_get(&ctx->req_mp);
    task->coll_type = UCC_TL_SPIN_WORKER_TASK_TYPE_KILL;
    ucc_assert_always(rbuf_has_space(&team->task_rbuf));
    rbuf_push_head(&team->task_rbuf, (uintptr_t)task);
}

void ucc_tl_spin_coll_kill_task_finalize(ucc_tl_spin_team_t *team)
{
    int idx;
    ucc_tl_spin_task_t *task = (ucc_tl_spin_task_t *)rbuf_get_tail_element(&team->task_rbuf, &idx);
    ucc_assert_always((task != NULL) && (task->coll_type == UCC_TL_SPIN_WORKER_TASK_TYPE_KILL));
    ucc_mpool_put(task);
    rbuf_pop_tail(&team->task_rbuf);
}

void *ucc_tl_spin_coll_worker_main(void *arg)
{
    ucc_tl_spin_worker_info_t *ctx             = (ucc_tl_spin_worker_info_t *)arg;
    int                        got_kill_signal = 0;
    ucc_tl_spin_task_t        *cur_task        = NULL;
    int                        cur_task_idx    = 0;
    int                        prev_task_idx   = INT_MAX;
    ucc_status_t               status;

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "worker %u thread started", ctx->id);

    while (1) {
        cur_task = (ucc_tl_spin_task_t *)rbuf_get_tail_element(&ctx->team->task_rbuf, &cur_task_idx);
        if (!cur_task) {
            continue;
        }
        if (cur_task_idx == prev_task_idx) {
            continue;
        }
        switch (ctx->type) {
            case (UCC_TL_SPIN_WORKER_TYPE_TX):
                switch (cur_task->coll_type) {
                    case (UCC_TL_SPIN_WORKER_TASK_TYPE_BCAST):
                        status = ucc_tl_spin_coll_worker_tx_bcast_start(ctx, cur_task);
                        break;
                    case (UCC_TL_SPIN_WORKER_TASK_TYPE_ALLGATHER):
                        status = ucc_tl_spin_coll_worker_tx_allgather_start(ctx, cur_task);
                        break;
                    case (UCC_TL_SPIN_WORKER_TASK_TYPE_KILL):
                        got_kill_signal = 1;
                        status = UCC_OK;
                        break;
                    default:
                        ucc_assert_always(0);
                }
                cur_task->tx_compls++;
                break;
            case (UCC_TL_SPIN_WORKER_TYPE_RX):
                switch (cur_task->coll_type) {
                    case (UCC_TL_SPIN_WORKER_TASK_TYPE_BCAST):
                        status = ucc_tl_spin_coll_worker_rx_bcast_start(ctx, cur_task);
                        break;
                    case (UCC_TL_SPIN_WORKER_TASK_TYPE_ALLGATHER):
                        status = ucc_tl_spin_coll_worker_rx_allgather_start(ctx, cur_task);
                        break;
                    case (UCC_TL_SPIN_WORKER_TASK_TYPE_KILL):
                        got_kill_signal = 1;
                        status = UCC_OK;
                        break;
                    default:
                        ucc_assert_always(0);
                }
                cur_task->rx_compls++;
                break;
            default:
                ucc_assert_always(0);
        }
        ucc_assert_always(status == UCC_OK);
        if (got_kill_signal) {
            break;
        }
        prev_task_idx = cur_task_idx;
    }

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "worker %u thread exits", ctx->id);

    return arg;
}