#include "tl_spin.h"
#include "tl_spin_coll.h"
#include "tl_spin_bcast.h"
#include "tl_spin_allgather.h"

static ucc_status_t ucc_tl_spin_coll_finalize(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t *task = ucc_derived_of(coll_task, ucc_tl_spin_task_t);

    tl_debug(UCC_TASK_LIB(task), "finalizing coll task ptr=%p gid=%u", 
             task, task->id);
    ucc_mpool_put(task);
    return UCC_OK;
}

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

    task->super.finalize = ucc_tl_spin_coll_finalize;

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

void *ucc_tl_spin_coll_worker_main(void *arg)
{
    ucc_tl_spin_worker_info_t *ctx       = (ucc_tl_spin_worker_info_t *)arg;
    int                        completed = 0;
    ucc_status_t               status;
    int                        signal;

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "worker %u thread started", ctx->id);

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

        ucc_assert_always(ctx->team->cur_task != NULL);

        switch (ctx->type) {
        case (UCC_TL_SPIN_WORKER_TYPE_TX):
            switch (ctx->team->cur_task->coll_type) {
            case (UCC_COLL_TYPE_BCAST):
                status = ucc_tl_spin_coll_worker_tx_bcast_start(ctx);
                break;
            case (UCC_COLL_TYPE_ALLGATHER):
                status = ucc_tl_spin_coll_worker_tx_allgather_start(ctx);
                break;
            default:
                ucc_assert_always(0);
            }
            break;
        case (UCC_TL_SPIN_WORKER_TYPE_RX):
            switch (ctx->team->cur_task->coll_type) {
            case (UCC_COLL_TYPE_BCAST):
                status = ucc_tl_spin_coll_worker_rx_bcast_start(ctx);
                break;
            case (UCC_COLL_TYPE_ALLGATHER):
                status = ucc_tl_spin_coll_worker_rx_allgather_start(ctx);
                break;
            default:
                ucc_assert_always(0);
            }
            ucc_tl_spin_bitmap_cleanup(&ctx->bitmap);
            break;
        default:
            tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "worker %u thread shouldn't be here", ctx->id);
            ucc_assert_always(0);
        }
        ucc_assert_always(status == UCC_OK);
        completed = 1;
        goto poll;

    case (UCC_TL_SPIN_WORKER_FIN):
        break;
    default:
        tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "worker %u thread shouldn't be here", ctx->id);
        ucc_assert_always(0);
        break;
    }

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx->team), "worker %u thread exits", ctx->id);

    return arg;
}