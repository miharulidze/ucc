#ifndef UCC_TL_SPIN_H_
#define UCC_TL_SPIN_H_

#include "components/tl/ucc_tl.h"
#include "components/tl/ucc_tl_log.h"
#include "components/tl/mlx5/mcast/tl_mlx5_mcast_helper.h"
#include "utils/ucc_mpool.h"

#include <infiniband/verbs.h>
#include <pthread.h>

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
    char                   *ib_dev_name;
} ucc_tl_spin_context_config_t;

typedef struct ucc_tl_spin_lib {
    ucc_tl_lib_t            super;
    ucc_tl_spin_lib_config_t cfg;
} ucc_tl_spin_lib_t;
UCC_CLASS_DECLARE(ucc_tl_spin_lib_t, const ucc_base_lib_params_t *,
                  const ucc_base_config_t *);

typedef struct ucc_tl_spin_p2p_context {
    struct ibv_context *dev;
    struct ibv_pd      *pd;
} ucc_tl_spin_p2p_context_t;

/* Mostly resembles ucc_tl_mlx5_mcast_coll_context */
typedef struct ucc_tl_spin_mcast_context {
    struct ibv_context        *ctx;
    struct ibv_pd             *pd;
    int                        max_qp_wr;
    int                        ib_port;
    int                        pkey_index;
    int                        mtu;
    struct rdma_cm_id         *id;
    struct rdma_event_channel *channel;
    ucc_mpool_t                compl_objects_mp;
} ucc_tl_spin_mcast_context_t;

typedef struct ucc_tl_spin_context {
    ucc_tl_context_t             super;
    ucc_tl_spin_context_config_t cfg;
    ucc_mpool_t                  req_mp;
    char                        *devname;
    int                          ib_port;
    ucc_tl_spin_p2p_context_t    p2p;
    ucc_tl_spin_mcast_context_t  mcast;
} ucc_tl_spin_context_t;
UCC_CLASS_DECLARE(ucc_tl_spin_context_t, const ucc_base_context_params_t *,
                  const ucc_base_config_t *);

typedef enum
{
    TL_SPIN_TEAM_STATE_INIT,
    TL_SPIN_TEAM_STATE_POSTED,
    TL_SPIN_TEAM_READY,
} ucc_tl_spin_team_state_t;

typedef struct ucc_tl_spin_worker_info {
    struct ibv_cq *cq;
    struct ibv_qp *mcast_qps;
    uint32_t       n_qps;
    /* thread-local data wrt to the currently processed collective goes here  */
} ucc_tl_spin_worker_info_t;

#define UCC_TL_SPIN_MAX_MCGS    1
#define UCC_TL_SPIN_P2P_QPS_NUM 2 // 2 QPs to have ring (TODO: check service collectives)
#define UCC_TL_SPIN_MAX_CQS_NUM (UCC_TL_SPIN_P2P_QPS_NUM + 2 * (UCC_TL_SPIN_MAX_MCGS))
typedef struct ucc_tl_spin_team {
    ucc_tl_team_t             super;
    ucc_tl_spin_team_state_t  state;
    struct ibv_cq            *cqs         [UCC_TL_SPIN_MAX_CQS_NUM];
    pthread_t                *workers     [UCC_TL_SPIN_MAX_CQS_NUM];
    ucc_tl_spin_worker_info_t descrs      [UCC_TL_SPIN_MAX_CQS_NUM];
    struct ibv_qp            *p2p_qps     [UCC_TL_SPIN_P2P_QPS_NUM];
    struct ibv_qp            *mcast_txqps [UCC_TL_SPIN_MAX_MCGS];
    struct ibv_qp            *mcast_rxqps [UCC_TL_SPIN_MAX_MCGS];
    struct ibv_ah            *ahs         [UCC_TL_SPIN_MAX_MCGS];
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

#define UCC_TL_SPIN_TASK_TEAM(_task)                                           \
    (ucc_derived_of((_task)->super.team, ucc_tl_spin_team_t))

#define UCC_TL_SPIN_CHK_PTR(lib, func, ptr, status, err_code, err_handler) \
    {                                                                      \
        ptr = (func);                                                      \
        if (!ptr) {                                                        \
            tl_error(lib, "%s failed", #func);                             \
            status = (err_code);                                           \
            goto err_handler;                                              \
        } else {                                                           \
            status = UCC_OK;                                               \
        }                                                                  \
    }

#define UCC_TL_SPIN_CHK_ERR(lib, func, status, err_code, err_handler) \
    {                                                                 \
        if (func) {                                                   \
            tl_error(lib, "%s failed with errno %d", #func, errno);   \
            status = (err_code);                                      \
            goto err_handler;                                         \
        } else {                                                      \
            status = UCC_OK;                                          \
        }                                                             \
    }

#endif