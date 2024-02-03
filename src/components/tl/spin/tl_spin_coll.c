#include "tl_spin.h"
#include "tl_spin_coll.h"

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
    ucc_tl_spin_task_t *task = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t *team = UCC_TL_SPIN_TASK_TEAM(task);
    coll_task->status = UCC_INPROGRESS;
    tl_debug(UCC_TASK_LIB(task), "start coll task ptr=%p tgid=%u", 
             task, task->dummy_task_id);
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_spin_bcast_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t *task = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
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

void * ucc_tl_spin_coll_worker_tx(void *arg)
{
    printf("hello from tx thread\n");
    return arg;
}

void * ucc_tl_spin_coll_worker_rx(void *arg)
{
    printf("hello from rx thread\n");
    return arg;
}