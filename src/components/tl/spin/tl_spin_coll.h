#ifndef UCC_TL_SPIN_COLL_H_
#define UCC_TL_SPIN_COLL_H_

#include "tl_spin.h"

ucc_status_t ucc_tl_spin_coll_init(ucc_base_coll_args_t *coll_args,
                                   ucc_base_team_t *team,
                                   ucc_coll_task_t **task);

#endif