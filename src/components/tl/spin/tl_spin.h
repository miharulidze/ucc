#ifndef UCC_TL_SPIN_H_
#define UCC_TL_SPIN_H_

#include "components/tl/ucc_tl.h"
#include "components/tl/ucc_tl_log.h"
#include "utils/ucc_mpool.h"

#include <infiniband/verbs.h>
#include "components/tl/mlx5/mcast/tl_mlx5_mcast.h"

#ifndef UCC_TL_SPIN_DEFAULT_SCORE
#define UCC_TL_SPIN_DEFAULT_SCORE 42
#endif

typedef struct ucc_tl_spin_iface {
    ucc_tl_iface_t super;
} ucc_tl_spin_iface_t;

/* Extern iface should follow the pattern: ucc_tl_<tl_name> */
extern ucc_tl_spin_iface_t ucc_tl_spin;

typedef struct ucc_tl_spin_lib_config {
    ucc_tl_lib_config_t super;
    uint32_t            dummy_param; // TBD
} ucc_tl_spin_lib_config_t;

typedef struct ucc_tl_spin_context_config {
    ucc_tl_context_config_t super;
    uint32_t                dummy_param; // TBD
} ucc_tl_spin_context_config_t;

typedef struct ucc_tl_spin_lib {
    ucc_tl_lib_t            super;
    ucc_tl_spin_lib_config_t cfg;
} ucc_tl_spin_lib_t;
UCC_CLASS_DECLARE(ucc_tl_spin_lib_t, const ucc_base_lib_params_t *,
                  const ucc_base_config_t *);

typedef struct ucc_tl_spin_context {
    ucc_tl_context_t             super;
    ucc_tl_spin_context_config_t cfg;
    ucc_mpool_t                  req_mp;
} ucc_tl_spin_context_t;
UCC_CLASS_DECLARE(ucc_tl_spin_context_t, const ucc_base_context_params_t *,
                  const ucc_base_config_t *);

typedef struct ucc_tl_spin_team {
    ucc_tl_team_t super;
} ucc_tl_spin_team_t;
UCC_CLASS_DECLARE(ucc_tl_spin_team_t, ucc_base_context_t *,
                  const ucc_base_team_params_t *);

typedef struct ucc_tl_spin_task {
    ucc_coll_task_t   super;
    uint32_t          dummy_task_id;
} ucc_tl_spin_task_t;

#define UCC_TL_SPIN_SUPPORTED_COLLS (UCC_COLL_TYPE_BCAST)

#define UCC_TL_SPIN_TEAM_LIB(_team)                                            \
    (ucc_derived_of((_team)->super.super.context->lib, ucc_tl_spin_lib_t))

#define UCC_TL_SPIN_TEAM_CTX(_team)                                            \
    (ucc_derived_of((_team)->super.super.context, ucc_tl_spin_context_t))

#endif