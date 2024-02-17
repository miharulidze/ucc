#include "coll_score/ucc_coll_score.h"
#include "core/ucc_service_coll.h"

#include "tl_spin.h"
#include "tl_spin_coll.h"
#include "tl_spin_p2p.h"
#include "tl_spin_mcast.h"

static ucc_status_t
ucc_tl_spin_team_service_bcast_post(ucc_tl_spin_team_t *ctx, 
                                    void *buf, size_t size, ucc_rank_t root,
                                    ucc_service_coll_req_t **bcast_req)
{
    ucc_status_t            status = UCC_OK;
    ucc_team_t             *team   = ctx->base_team;
    ucc_subset_t            subset = ctx->subset;
    ucc_service_coll_req_t *req    = NULL;

    tl_debug(UCC_TL_SPIN_TEAM_LIB(ctx), "bcasting");

    status = ucc_service_bcast(team, buf, size, root, subset, &req);
    if (ucc_unlikely(UCC_OK != status)) {
        tl_error(UCC_TL_SPIN_TEAM_LIB(ctx), "tl service team bcast failed");
        return status;
    }

    *bcast_req = req;

    return status;
}

static ucc_status_t
ucc_tl_spin_team_service_allgather_post(ucc_tl_spin_team_t *ctx, void *sbuf, void *rbuf,
                                        size_t size, ucc_service_coll_req_t **ag_req)
{
    ucc_status_t            status = UCC_OK;
    ucc_team_t             *team   = ctx->base_team;
    ucc_subset_t            subset = ctx->subset;
    ucc_service_coll_req_t *req    = NULL;

    status = ucc_service_allgather(team, sbuf, rbuf, size, subset, &req);
    if (ucc_unlikely(UCC_OK != status)) {
        tl_error(UCC_TL_SPIN_TEAM_LIB(ctx), "tl service team allgather failed");
        return status;
    }

    *ag_req = req;

    return status;
}

static ucc_status_t
ucc_tl_spin_team_service_coll_test(ucc_service_coll_req_t *req, int blocking)
{
    ucc_status_t status = UCC_OK;

test:
    status = ucc_service_coll_test(req);

    if (blocking && (status == UCC_INPROGRESS)) {
        goto test;
    }

    if (UCC_INPROGRESS != status) {
        ucc_service_coll_finalize(req);
    }

    return status;
}

UCC_CLASS_INIT_FUNC(ucc_tl_spin_team_t, ucc_base_context_t *tl_context,
                    const ucc_base_team_params_t *params)
{
    ucc_tl_spin_context_t     *ctx       = ucc_derived_of(tl_context, ucc_tl_spin_context_t);
    ucc_tl_spin_worker_info_t *worker    = NULL;
    ucc_status_t               status    = UCC_OK;
    int                        n_workers = ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers;
    int                        i, j;

    // TODO: support multiple QPs/multicast subgroups per CQ
    ucc_assert_always(ctx->cfg.n_mcg == ctx->cfg.n_tx_workers);
    ucc_assert_always(ctx->cfg.n_mcg == ctx->cfg.n_rx_workers);
    //ucc_assert_always(ctx->cfg.n_mcg % ctx->cfg.n_tx_workers == 0);
    //ucc_assert_always(ctx->cfg.n_mcg % ctx->cfg.n_rx_workers == 0);

    UCC_CLASS_CALL_SUPER_INIT(ucc_tl_team_t, &ctx->super, params);

    self->base_team     = params->team;
    self->subset.myrank = params->rank;
    self->subset.map    = params->map;
    self->size          = params->size;

    UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                        ucc_calloc(n_workers + 1, sizeof(ucc_tl_spin_worker_info_t)),
                        self->workers,
                        status, UCC_ERR_NO_MEMORY, ret);
    UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                        ucc_calloc(ctx->cfg.n_mcg, sizeof(ucc_tl_spin_mcast_join_info_t)),
                        self->mcg_infos,
                        status, UCC_ERR_NO_MEMORY, ret);

    self->tx_signal = UCC_TL_SPIN_WORKER_POLL;
    self->rx_signal = UCC_TL_SPIN_WORKER_POLL;
    pthread_mutex_init(&self->tx_signal_mutex, NULL);
    pthread_mutex_init(&self->rx_signal_mutex, NULL);
    self->tx_compls = 0;
    self->rx_compls = 0;
    pthread_mutex_init(&self->tx_compls_mutex, NULL);
    pthread_mutex_init(&self->rx_compls_mutex, NULL);

    // FIXME in case of error this will leak
    for (i = 0; i < n_workers; i++) {
        worker         = &self->workers[i];
        worker->ctx    = ctx;
        worker->type   = i < ctx->cfg.n_tx_workers ?
                         UCC_TL_SPIN_WORKER_TYPE_TX :
                         UCC_TL_SPIN_WORKER_TYPE_RX;
        worker->n_mcg = worker->type == UCC_TL_SPIN_WORKER_TYPE_TX ?
                         ctx->cfg.n_mcg / ctx->cfg.n_tx_workers :
                         ctx->cfg.n_mcg / ctx->cfg.n_rx_workers;
        worker->signal = worker->type == UCC_TL_SPIN_WORKER_TYPE_TX ?
                         &(self->tx_signal) :
                         &(self->rx_signal);
        worker->compls = worker->type == UCC_TL_SPIN_WORKER_TYPE_TX ? 
                         &(self->tx_compls) :
                         &(self->rx_compls);
        worker->signal_mutex = worker->type == UCC_TL_SPIN_WORKER_TYPE_TX ?
                               &(self->tx_signal_mutex) :
                               &(self->rx_signal_mutex);
        worker->compls_mutex = worker->type == UCC_TL_SPIN_WORKER_TYPE_TX ?
                               &(self->tx_compls_mutex) :
                               &(self->rx_compls_mutex);
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ucc_calloc(worker->n_mcg, sizeof(struct ibv_qp *)),
                            worker->qps, status, UCC_ERR_NO_MEMORY, ret);
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ucc_calloc(worker->n_mcg, sizeof(struct ibv_ah *)),
                            worker->ahs, status, UCC_ERR_NO_MEMORY, ret);
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ibv_create_cq(ctx->mcast.dev, ctx->cfg.mcast_cq_depth, NULL, NULL, 0), 
                            worker->cq, status, UCC_ERR_NO_MEMORY, ret);
        tl_debug(tl_context->lib, "worker %d created cq %p", i, worker->cq);
        if (worker->type == UCC_TL_SPIN_WORKER_TYPE_RX) {
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(worker->n_mcg, sizeof(char *)),
                                worker->staging_rbuf, status, UCC_ERR_NO_MEMORY, ret);
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(worker->n_mcg, sizeof(struct ibv_mr *)),
                                worker->staging_rbuf_mr, status, UCC_ERR_NO_MEMORY, ret);
            worker->staging_rbuf_len = ctx->mcast.mtu * ctx->cfg.mcast_qp_depth;
            for (j = 0; j < worker->n_mcg; j++) {
                if (posix_memalign((void **)&worker->staging_rbuf[j], 64, worker->staging_rbuf_len)) {
                    tl_error(tl_context->lib, "allocation of staging buffer failed");
                    return UCC_ERR_NO_MEMORY;
                }
                worker->staging_rbuf_mr[j] = ibv_reg_mr(ctx->mcast.pd, worker->staging_rbuf[j], 
                                                        worker->staging_rbuf_len,
                                                        IBV_ACCESS_REMOTE_WRITE |
                                                        IBV_ACCESS_LOCAL_WRITE  |
                                                        IBV_ACCESS_REMOTE_READ);
                if (!worker->staging_rbuf_mr[j]) {
                    tl_error(tl_context->lib, "registration of staging buffer failed");
                    return UCC_ERR_NO_MEMORY;
                }
            }
        }
    }

    // construct p2p worker
    self->ctrl_ctx       = &self->workers[n_workers];
    self->ctrl_ctx->type = UCC_TL_SPIN_WORKER_TYPE_CTRL;
    UCC_TL_SPIN_CHK_PTR(tl_context->lib, ucc_calloc(2, sizeof(struct ibv_qp *)),
                        self->ctrl_ctx->qps, status, UCC_ERR_NO_MEMORY, ret);
    UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                        ibv_create_cq(ctx->p2p.dev, ctx->cfg.p2p_cq_depth, NULL, NULL, 0), 
                        self->ctrl_ctx->cq,
                        status, UCC_ERR_NO_MEMORY, ret);

    tl_info(tl_context->lib, "posted tl team: %p, n threads: %d", self, n_workers);

ret:
    return status;
}

UCC_CLASS_CLEANUP_FUNC(ucc_tl_spin_team_t)
{
    ucc_tl_spin_lib_t         *lib       = UCC_TL_SPIN_TEAM_LIB(self);
    ucc_tl_spin_context_t     *ctx       = UCC_TL_SPIN_TEAM_CTX(self);
    int                        n_workers = ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers;
    ucc_tl_spin_worker_info_t *worker;
    int i, j, mcg_id = 0;
    
    if (self->ctrl_ctx->qps) {
        if (ibv_destroy_qp(self->ctrl_ctx->qps[0])) {
            tl_error(lib, "ctrl ctx failed to destroy qp 0, errno: %d", errno);
        }
        if (ibv_destroy_qp(self->ctrl_ctx->qps[1])) {
            tl_error(lib, "ctrl ctx failed to destroy qp 1, errno: %d", errno);
        }
        ucc_free(self->ctrl_ctx->qps);
    }

    if (self->ctrl_ctx->cq) {
        if (ibv_destroy_cq(self->ctrl_ctx->cq)) {
            tl_error(lib, "ctrl ctx failed to destroy cq, errno: %d", errno);
        }
    }

    pthread_mutex_lock(&self->tx_signal_mutex);
    self->tx_signal = UCC_TL_SPIN_WORKER_FIN;
    pthread_mutex_unlock(&self->tx_signal_mutex);

    pthread_mutex_lock(&self->rx_signal_mutex);
    self->rx_signal = UCC_TL_SPIN_WORKER_FIN;
    pthread_mutex_unlock(&self->rx_signal_mutex);

    for (i = 0; i < n_workers; i++) {
        worker = &self->workers[i];
        if (pthread_join(worker->pthread, NULL)) {
            tl_error(lib, "worker %d thread joined with error", i);
        }
        if (worker->qps) {
            for (j = 0; j < worker->n_mcg; j++) {
                if (ibv_detach_mcast(worker->qps[j], &self->mcg_infos[mcg_id].mcg_addr.gid, self->mcg_infos[mcg_id].mcg_addr.lid)) {
                    tl_error(lib, "worker %d failed to detach qp[%d] from mcg[%d], errno: %d", i, j, mcg_id, errno);
                }
                tl_debug(lib, "worker %d destroys qp %d %p", i, j, worker->qps[j]);
                if (ibv_destroy_qp(worker->qps[j])) {
                    tl_error(lib, "worker %d failed to destroy qp[%d], errno: %d", i, j, errno);
                }
                if (ibv_destroy_ah(worker->ahs[j])) {
                    tl_error(lib, "worker %d failed to destroy ah[%d], errno: %d", i, j, errno);
                }
                if (worker->type == UCC_TL_SPIN_WORKER_TYPE_RX) {
                    if (ibv_dereg_mr(worker->staging_rbuf_mr[j])) {
                        tl_error(lib, "worker %d failed to destroy staging_rbuf_mr[%d], errno: %d", i, j, errno);
                    }
                    free(worker->staging_rbuf[j]);
                }
            }
            ucc_free(worker->qps);
            ucc_free(worker->ahs);
            if (worker->type == UCC_TL_SPIN_WORKER_TYPE_RX) {
                ucc_free(worker->staging_rbuf);
                ucc_free(worker->staging_rbuf_mr);
            }
            mcg_id = (mcg_id + 1) % ctx->cfg.n_mcg;
        }
        if (worker->cq) {
            tl_debug(lib, "worker %d destroys cq %p", i, worker->cq);
            if (ibv_destroy_cq(worker->cq)) {
                tl_error(lib, "worker %d failed to destroy cq, errno: %d", i, errno);
            }
        }
    }

    pthread_mutex_destroy(&self->tx_signal_mutex);
    pthread_mutex_destroy(&self->rx_signal_mutex);
    pthread_mutex_destroy(&self->tx_compls_mutex);
    pthread_mutex_destroy(&self->rx_compls_mutex);

    ucc_free(self->workers);

    for (i = 0; i < ctx->cfg.n_mcg; i++) {
        ucc_tl_spin_team_fini_mcg(ctx, &self->mcg_infos[i].saddr);
    }
    ucc_free(self->mcg_infos);

    tl_info(self->super.super.context->lib, "finalizing tl team: %p", self);
}

UCC_CLASS_DEFINE_DELETE_FUNC(ucc_tl_spin_team_t, ucc_base_team_t);
UCC_CLASS_DEFINE(ucc_tl_spin_team_t, ucc_tl_team_t);

ucc_status_t ucc_tl_spin_team_destroy(ucc_base_team_t *tl_team)
{
    UCC_CLASS_DELETE_FUNC_NAME(ucc_tl_spin_team_t)(tl_team);
    return UCC_OK;
}

static ucc_status_t 
ucc_tl_spin_team_init_p2p_qps(ucc_base_team_t *tl_team)
{
    ucc_status_t               status      = UCC_OK;
    ucc_tl_spin_team_t        *team        = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_context_t     *ctx         = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_base_lib_t            *lib         = UCC_TL_TEAM_LIB(team);
    ucc_tl_spin_worker_info_t *ctrl_worker = team->ctrl_ctx;
    ucc_tl_spin_qp_addr_t     *recv_av     = NULL;
    ucc_tl_spin_qp_addr_t      send_av[2];
    ucc_rank_t                 l_neighbor, r_neighbor;
    struct ibv_qp_init_attr    qp_init_attr;
    ucc_service_coll_req_t    *req;
    int i;

    ucc_tl_spin_team_init_rc_qp_attr(&qp_init_attr, ctrl_worker->cq, 
                                     ctx->cfg.p2p_qp_depth);

    for (i = 0; i < 2; i++) {
        UCC_TL_SPIN_CHK_PTR(lib,
                            ibv_create_qp(ctx->p2p.pd, &qp_init_attr), ctrl_worker->qps[i],
                            status, UCC_ERR_NO_RESOURCE, ret);
        send_av[i].dev_addr = ctx->p2p.dev_addr;
        send_av[i].qpn      = ctrl_worker->qps[i]->qp_num;
    }

    // Collect team address vector (e.g., rank-to-qp-address mapping)
    UCC_TL_SPIN_CHK_PTR(lib,
                        ucc_calloc(team->size, sizeof(send_av)), recv_av,
                        status, UCC_ERR_NO_MEMORY, ret);
    UCC_TL_SPIN_CHK_ERR(lib,
                        ucc_tl_spin_team_service_allgather_post(team,
                                                                (void *)send_av,
                                                                (void *)recv_av,
                                                                sizeof(send_av),
                                                                &req),
                        status, UCC_ERR_NO_RESOURCE, ret);
    UCC_TL_SPIN_CHK_ERR(lib,
                        // TODO: remove blocking behaviour
                        ucc_tl_spin_team_service_coll_test(req, 1),
                        status, UCC_ERR_NO_RESOURCE, ret);

    // Connect QPs in a virtual ring
    l_neighbor = (team->subset.myrank + 1)              % team->size;
    r_neighbor = (team->subset.myrank - 1 + team->size) % team->size;
    assert(team_size == 2);
    status = ucc_tl_spin_team_connect_rc_qp(lib, ctrl_worker->qps[0], &send_av[0],
                                            &recv_av[l_neighbor * 2 + 1]);
    ucc_assert_always(status == UCC_OK);
    status = ucc_tl_spin_team_prepost_rc_qp(ctx, ctrl_worker, 0);
    ucc_assert_always(status == UCC_OK);
    status = ucc_tl_spin_team_connect_rc_qp(lib, ctrl_worker->qps[1], &send_av[1],
                                            &recv_av[r_neighbor * 2]);
    ucc_assert_always(status == UCC_OK);
    status = ucc_tl_spin_team_prepost_rc_qp(ctx, ctrl_worker, 1);
    ucc_assert_always(status == UCC_OK);
 
    tl_debug(lib,
             "connected p2p context of rank %d, "
             "left neighbor rank %d (lid:qpn): %d:%d to %d:%d, "
             "right neighbor rank %d (lid:qpn): %d:%d to %d:%d",
             team->subset.myrank,
             l_neighbor, send_av[0].qpn, send_av[0].dev_addr.lid, 
             recv_av[l_neighbor * 2 + 1].qpn, recv_av[l_neighbor * 2 + 1].dev_addr.lid,
             r_neighbor, send_av[1].qpn, send_av[1].dev_addr.lid,
             recv_av[r_neighbor * 2].qpn, recv_av[r_neighbor * 2].dev_addr.lid);

    ucc_free(recv_av);
ret:
    return status;
}

static ucc_status_t ucc_tl_spin_team_prepare_mcg(ucc_base_team_t *tl_team,
                                                  ucc_tl_spin_mcast_join_info_t *worker_mcg_info,
                                                  unsigned int mcg_gid)
{
    ucc_tl_spin_team_t            *team       = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_context_t         *ctx        = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_base_lib_t                *lib        = UCC_TL_SPIN_CTX_LIB(ctx);
    ucc_status_t                   status     = UCC_OK;
    struct sockaddr_in6            mcg_saddr = {.sin6_family = AF_INET6};
    char                           saddr_str[40];
    ucc_tl_spin_mcast_join_info_t  mcg_info;
    ucc_service_coll_req_t        *bcast_req;

    memset(&mcg_info, 0, sizeof(mcg_info));

    if (team->subset.myrank == 0) {                 // root obtains group gid/lid
        ucc_assert_always(mcg_gid < 65536);        // number of mcg'es that can be encoded in the last on ipv6 four-tet
        ucc_assert_always(snprintf(saddr_str, 40, "0:0:0::0:0:%x", mcg_gid) > 0);
        ucc_assert_always(inet_pton(AF_INET6, saddr_str, &(mcg_saddr.sin6_addr)));
        //mcg_saddr.sin6_flowinfo = subgroup_id++; // TODO: enumerate multicast subgroup in IP address
        status = ucc_tl_spin_team_join_mcg(ctx, &mcg_saddr, &mcg_info, 1);
        (void)status;                               // send status to non-root ranks first and then fail
        mcg_info.magic_num = UCC_TL_SPIN_JOIN_MAGICNUM;
    }

    status = ucc_tl_spin_team_service_bcast_post(team,
                                                 &mcg_info,
                                                 sizeof(ucc_tl_spin_mcast_join_info_t), 
                                                 0,
                                                 &bcast_req);
    ucc_assert_always(status == UCC_OK);
    status = ucc_tl_spin_team_service_coll_test(bcast_req, 1); // TODO: make non-blocking
    ucc_assert_always(status == UCC_OK);
    if (mcg_info.status != UCC_OK) {
        return UCC_ERR_NO_RESOURCE;
    } else {
        ucc_assert_always(mcg_info.magic_num == UCC_TL_SPIN_JOIN_MAGICNUM);
    }

    memcpy(&mcg_saddr.sin6_addr, mcg_info.mcg_addr.gid.raw, sizeof(mcg_info.mcg_addr.gid.raw));
    status = ucc_tl_spin_team_join_mcg(ctx, &mcg_saddr, &mcg_info, 0);

    if (mcg_info.status != UCC_OK) {
        return UCC_ERR_NO_RESOURCE;
    }

    mcg_info.saddr   = mcg_saddr;
    *worker_mcg_info = mcg_info;

    tl_debug(lib, "team: %p, rank: %d joined multicast group: %d",
             tl_team, team->subset.myrank, mcg_gid);

    return UCC_OK;
}

static ucc_status_t ucc_tl_spin_team_init_mcast_qps(ucc_base_team_t *tl_team)
{
    ucc_tl_spin_team_t        *team      = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_context_t     *ctx       = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_base_lib_t            *lib       = UCC_TL_TEAM_LIB(team);
    ucc_tl_spin_worker_info_t *worker    = NULL;
    ucc_status_t               status    = UCC_OK;
    int                        n_workers = ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers;
    int                        i, j, mcg_id = 0;

    for (i = 0; i < n_workers; i++) {
        worker = &team->workers[i];
        for (j = 0; j < worker->n_mcg; j++) {
            if (worker->type == UCC_TL_SPIN_WORKER_TYPE_TX) {
                status = ucc_tl_spin_team_setup_mcast_qp(ctx, worker, &team->mcg_infos[mcg_id], 1, j);
                assert(status == UCC_OK);
            } else {
                ucc_assert(worker->type == UCC_TL_SPIN_WORKER_TYPE_RX);
                status = ucc_tl_spin_team_setup_mcast_qp(ctx, worker, &team->mcg_infos[mcg_id], 0, j);
                ucc_assert(status == UCC_OK);
                status = ucc_tl_spin_team_prepost_mcast_qp(ctx, worker, j);
                ucc_assert(status == UCC_OK);
            }
            tl_debug(lib, "worker %d qp %d %p attached to cq %p", i, j, worker->qps[j], worker->cq);
            mcg_id = (mcg_id + 1) % ctx->cfg.n_mcg;
        }
    }
    tl_debug(lib, "initialized multicast QPs");

    return status;
}

static ucc_status_t ucc_tl_spin_team_spawn_workers(ucc_base_team_t *tl_team)
{
    ucc_tl_spin_team_t        *team      = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_context_t     *ctx       = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_base_lib_t            *lib       = UCC_TL_TEAM_LIB(team);
    ucc_tl_spin_worker_info_t *worker    = NULL;
    ucc_status_t               status    = UCC_OK;
    int                        n_workers = ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers;
    int                        core_id   = ctx->cfg.start_core_id;
    int                        num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    int                        i;
    cpu_set_t                  cpuset;
   
    for (i = 0; i < n_workers; i++) {
        worker    = &(team->workers[i]);
        UCC_TL_SPIN_CHK_ERR(lib,
                            pthread_create(&worker->pthread, NULL, ucc_tl_spin_coll_worker_main, (void *)worker), 
                            status, UCC_ERR_NO_RESOURCE, err);

        /* Round-robin workers pinning */
        if (core_id > num_cores) {
            core_id = ctx->cfg.start_core_id;
            tl_warn(lib, "number of workers is larger than number of available cores");
        }
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        UCC_TL_SPIN_CHK_ERR(lib,
                            pthread_setaffinity_np(worker->pthread, sizeof(cpu_set_t), &cpuset),
                            status, UCC_ERR_NO_RESOURCE, err);
        tl_debug(lib, "worker %d is pinned to core %d", i, core_id);
        core_id++;
    }

err:
    return status;
}

ucc_status_t ucc_tl_spin_team_create_test(ucc_base_team_t *tl_team)
{
    ucc_tl_spin_team_t        *team      = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_context_t     *ctx       = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_base_lib_t            *lib       = UCC_TL_TEAM_LIB(team);
    ucc_status_t               status;
    int                        i;

    // Create and connect RC QPs
    ucc_tl_spin_team_init_p2p_qps(tl_team);

    for (i = 0; i < ctx->cfg.n_mcg; i++) {
        status = ucc_tl_spin_team_prepare_mcg(tl_team, &team->mcg_infos[i], ctx->mcast.gid++);
        if (status != UCC_OK) {
            return status;
        }
    }

    status = ucc_tl_spin_team_init_mcast_qps(tl_team);
    if (status != UCC_OK) {
        return status;
    }
    
    // Spawn worker threads
    ucc_tl_spin_team_spawn_workers(tl_team);

    tl_info(lib, "initialized tl team: %p", team);
    return UCC_OK;
}

ucc_status_t ucc_tl_spin_team_get_scores(ucc_base_team_t *tl_team,
                                         ucc_coll_score_t **score_p)
{
    ucc_tl_spin_team_t *team  = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_base_context_t *ctx   = UCC_TL_TEAM_CTX(team);
    ucc_base_lib_t     *lib   = UCC_TL_TEAM_LIB(team);
    ucc_memory_type_t   mt[1] = {UCC_MEMORY_TYPE_HOST};
    ucc_coll_score_t          *score;
    ucc_status_t               status;
    ucc_coll_score_team_info_t team_info;

    team_info.alg_fn              = NULL;
    team_info.default_score       = UCC_TL_SPIN_DEFAULT_SCORE;
    team_info.init                = ucc_tl_spin_coll_init;
    team_info.num_mem_types       = 1;
    team_info.supported_mem_types = mt;
    team_info.supported_colls     = UCC_TL_SPIN_SUPPORTED_COLLS;
    team_info.size                = UCC_TL_TEAM_SIZE(team);

    status = ucc_coll_score_build_default(
        tl_team, UCC_TL_SPIN_DEFAULT_SCORE, ucc_tl_spin_coll_init,
        UCC_TL_SPIN_SUPPORTED_COLLS, mt, 1, &score);
    if (UCC_OK != status) {
        tl_debug(lib, "failed to build score map");
        return status;
    }
    if (strlen(ctx->score_str) > 0) {
        status = ucc_coll_score_update_from_str(ctx->score_str, &team_info,
                                                &team->super.super, score);
        /* If INVALID_PARAM - User provided incorrect input - try to proceed */
        if ((status < 0) && (status != UCC_ERR_INVALID_PARAM) &&
            (status != UCC_ERR_NOT_SUPPORTED)) {
            goto err;
        }
    }

    *score_p = score;
    return UCC_OK;

err:
    ucc_coll_score_free(score);
    *score_p = NULL;
    return status;
}