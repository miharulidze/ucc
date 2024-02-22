#include "tl_spin_allgather.h"

static ucc_status_t ucc_tl_spin_allgather_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t *task = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    ucc_tl_spin_team_t *team = UCC_TL_SPIN_TASK_TEAM(task);
    coll_task->status = UCC_INPROGRESS;
    tl_debug(UCC_TASK_LIB(task), "start coll task ptr=%p tgid=%u", task, task->id);
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

static void ucc_tl_spin_allgather_progress(ucc_coll_task_t *coll_task)
{
    coll_task->status = UCC_OK;
}

static ucc_status_t ucc_tl_spin_allgather_finalize(ucc_coll_task_t *coll_task)
{
    ucc_tl_spin_task_t    *task = ucc_derived_of(coll_task, ucc_tl_spin_task_t);
    tl_debug(UCC_TASK_LIB(task), "finalize coll task ptr=%p tgid=%u", task, task->id);
    ucc_mpool_put(task);
    return UCC_OK;
}

ucc_status_t ucc_tl_spin_allgather_init(ucc_tl_spin_task_t   *task,
                                        ucc_base_coll_args_t *coll_args,
                                        ucc_tl_spin_team_t   *team)
{
    task->super.post      = ucc_tl_spin_allgather_start;
    task->super.progress  = ucc_tl_spin_allgather_progress;
    task->super.finalize  = ucc_tl_spin_allgather_finalize;
    task->coll_type       = UCC_COLL_TYPE_BCAST;
    tl_debug(UCC_TL_SPIN_TEAM_LIB(team), "got new task");

    return UCC_OK;
}

ucc_status_t
ucc_tl_spin_coll_worker_tx_allgather_start(ucc_tl_spin_worker_info_t *ctx)
{
    return UCC_OK;
}

ucc_status_t
ucc_tl_spin_coll_worker_rx_allgather_start(ucc_tl_spin_worker_info_t *ctx)
{
    return UCC_OK;
}