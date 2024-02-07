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
    int                     n_mcgs;
    int                     n_tx_workers;
    int                     n_rx_workers;
    int                     mcast_cq_depth;
    int                     mcast_qp_depth;
    int                     p2p_cq_depth;
    int                     p2p_qp_depth;
} ucc_tl_spin_context_config_t;

typedef struct ucc_tl_spin_lib {
    ucc_tl_lib_t             super;
    ucc_tl_spin_lib_config_t cfg;
} ucc_tl_spin_lib_t;
UCC_CLASS_DECLARE(ucc_tl_spin_lib_t, const ucc_base_lib_params_t *,
                  const ucc_base_config_t *);

#define UCC_TL_SPIN_DEFAULT_PKEY 0 
#define UCC_TL_SPIN_GID_TBL_MAX_ENTRIES 32
typedef struct ucc_tl_spin_ib_dev_addr {
    int                 port_num;
    enum ibv_mtu        mtu;
    uint16_t            lid;
    uint8_t             gid_table_index;
	union ibv_gid       gid;
} ucc_tl_spin_ib_dev_addr_t;

typedef struct ucc_tl_spin_qp_addr {
    uint32_t                  qpn;
    ucc_tl_spin_ib_dev_addr_t dev_addr;
} ucc_tl_spin_qp_addr_t;

typedef struct ucc_tl_spin_p2p_context {
    struct ibv_context       *dev;
    struct ibv_pd            *pd;
    ucc_tl_spin_ib_dev_addr_t dev_addr;
} ucc_tl_spin_p2p_context_t;

/* Mostly resembles ucc_tl_mlx5_mcast_coll_context */
typedef struct ucc_tl_spin_mcast_context {
    struct ibv_context        *dev;
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
    UCC_TL_SPIN_WORKER_TYPE_CTRL,
    UCC_TL_SPIN_WORKER_TYPE_TX,
    UCC_TL_SPIN_WORKER_TYPE_RX
} ucc_tl_spin_worker_type_t;

typedef enum
{
    UCC_TL_SPIN_WORKER_INIT = 0,
    UCC_TL_SPIN_WORKER_START = 1,
    UCC_TL_SPIN_WORKER_IGNORE_TX = 2
} ucc_tl_spin_worker_signal_t;

typedef struct ucc_tl_spin_worker_info {
    ucc_tl_spin_context_t    *ctx;
    ucc_tl_spin_worker_type_t type;
    ucc_tl_spin_worker_signal_t *signal;
    pthread_t                 pthread;
    struct ibv_cq            *cq;
    struct ibv_qp           **qps;
    struct ibv_ah           **ahs;
    struct ibv_mr           **staging_rbuf_mr;
    char                    **staging_rbuf;
    uint32_t                  staging_rbuf_len;
    uint32_t                  n_mcgs;
    pthread_mutex_t          *signal_mutex;
    /* thread-local data wrt to the currently processed collective goes here */
} ucc_tl_spin_worker_info_t;

#define UCC_TL_SPIN_JOIN_MAGICNUM 0xDEADBEAF

typedef struct ucc_tl_spin_mcast_join_info {
    ucc_status_t              status;
    struct sockaddr_in6       saddr;
    ucc_tl_spin_ib_dev_addr_t mcsg_addr;
    ucc_tl_spin_ib_dev_addr_t mcsg_addr_dst;
    unsigned int              magic_num;
} ucc_tl_spin_mcast_join_info_t;

#define UCC_TL_SPIN_MAX_MCGS    1
#define UCC_TL_SPIN_P2P_QPS_NUM 2 // 2 QPs to have ring (TODO: check service collectives)
#define UCC_TL_SPIN_MAX_CQS_NUM (UCC_TL_SPIN_P2P_QPS_NUM + 2 * (UCC_TL_SPIN_MAX_MCGS))
typedef struct ucc_tl_spin_team {
    ucc_tl_team_t                  super;
    ucc_tl_spin_mcast_join_info_t *mcgs_infos;
    ucc_tl_spin_worker_info_t     *workers;
    ucc_tl_spin_worker_info_t     *ctrl_ctx;
    ucc_team_t                    *base_team;
    ucc_subset_t                   subset;
    ucc_rank_t                     size;
    ucc_tl_spin_worker_signal_t    workers_signal;
    pthread_mutex_t                signal_mutex;
} ucc_tl_spin_team_t;
UCC_CLASS_DECLARE(ucc_tl_spin_team_t, ucc_base_context_t *,
                  const ucc_base_team_params_t *);

typedef struct ucc_tl_spin_task {
    ucc_coll_task_t   super;
    uint32_t          dummy_task_id;
} ucc_tl_spin_task_t;

#define UCC_TL_SPIN_SUPPORTED_COLLS (UCC_COLL_TYPE_BCAST)

#define UCC_TL_SPIN_CTX_LIB(_ctx)                                          \
    (ctx->super.super.lib)

#define UCC_TL_SPIN_TEAM_LIB(_team)                                        \
    (ucc_derived_of((_team)->super.super.context->lib, ucc_tl_spin_lib_t))

#define UCC_TL_SPIN_TEAM_CTX(_team)                                        \
    (ucc_derived_of((_team)->super.super.context, ucc_tl_spin_context_t))

#define UCC_TL_SPIN_TASK_TEAM(_task)                                       \
    (ucc_derived_of((_task)->super.team, ucc_tl_spin_team_t))

#define UCC_TL_SPIN_CHK_PTR(lib, func, ptr, status, err_code, err_handler) \
    {                                                                      \
        ptr = (func);                                                      \
        if (!ptr) {                                                        \
            tl_error(lib, "%s failed with errno %d", #func, errno);        \
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