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

    ucc_assert_always(ctx->cfg.n_mcgs % ctx->cfg.n_tx_workers == 0);
    ucc_assert_always(ctx->cfg.n_mcgs % ctx->cfg.n_rx_workers == 0);

    UCC_CLASS_CALL_SUPER_INIT(ucc_tl_team_t, &ctx->super, params);

    self->base_team     = params->team;
    self->subset.myrank = params->rank;
    self->subset.map    = params->map;
    self->size          = params->size;

    UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                        ucc_calloc(n_workers + 1, sizeof(ucc_tl_spin_worker_info_t)),
                        self->workers,
                        status, UCC_ERR_NO_MEMORY, ret);

    // FIXME in case of error this will leak
    for (i = 0; i < n_workers; i++) {
        worker         = &self->workers[i];
        worker->ctx    = ctx;
        worker->type   = i < ctx->cfg.n_tx_workers ?
                         UCC_TL_SPIN_WORKER_TYPE_TX : 
                         UCC_TL_SPIN_WORKER_TYPE_RX;
        worker->n_mcgs = i < ctx->cfg.n_tx_workers ?
                         ctx->cfg.n_mcgs / ctx->cfg.n_tx_workers :
                         ctx->cfg.n_mcgs / ctx->cfg.n_rx_workers;
        worker->signal = &(self->workers_signal);
        worker->signal_mutex = &(self->signal_mutex);
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ucc_calloc(worker->n_mcgs, sizeof(ucc_tl_spin_mcast_join_info_t)),
                            self->mcgs_infos, status, UCC_ERR_NO_MEMORY, ret);
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ucc_calloc(worker->n_mcgs, sizeof(struct ibv_qp *)),
                            worker->qps, status, UCC_ERR_NO_MEMORY, ret);
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ucc_calloc(worker->n_mcgs, sizeof(struct ibv_ah *)),
                            worker->ahs, status, UCC_ERR_NO_MEMORY, ret);
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ibv_create_cq(ctx->mcast.dev, ctx->cfg.mcast_cq_depth, NULL, NULL, 0), 
                            worker->cq, status, UCC_ERR_NO_MEMORY, ret);

        if (worker->type == UCC_TL_SPIN_WORKER_TYPE_RX) {
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(worker->n_mcgs, sizeof(char *)),
                                worker->staging_rbuf, status, UCC_ERR_NO_MEMORY, ret);
            UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                                ucc_calloc(worker->n_mcgs, sizeof(struct ibv_mr *)),
                                worker->staging_rbuf_mr, status, UCC_ERR_NO_MEMORY, ret);
            worker->staging_rbuf_len = ctx->mcast.mtu * ctx->cfg.mcast_qp_depth;
            for (j = 0; j < worker->n_mcgs; j++) {
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

    pthread_mutex_init(&self->signal_mutex, NULL);

    tl_info(tl_context->lib, "posted tl team: %p, n threads: %d", self, n_workers);

ret:
    return status;
}

UCC_CLASS_CLEANUP_FUNC(ucc_tl_spin_team_t)
{
    ucc_tl_spin_context_t *ctx       = UCC_TL_SPIN_TEAM_CTX(self);
    int                    n_workers = ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers;
    int i, j;

    if (self->ctrl_ctx->qps) {
        ibv_destroy_qp(self->ctrl_ctx->qps[0]);
        ibv_destroy_qp(self->ctrl_ctx->qps[1]);
        ucc_free(self->ctrl_ctx->qps);
    }

    for (i = 0; i < n_workers; i++) {
        pthread_join(self->workers[i].pthread, NULL);
        if (self->workers[i].qps) {
            for (j = 0; j < self->workers[i].n_mcgs; j++) {
                ibv_destroy_qp(self->workers[i].qps[j]);
                ibv_destroy_ah(self->workers[i].ahs[j]);
                ucc_free(self->workers[i].qps);
                ucc_free(self->workers[i].ahs);
                if (self->workers[i].type == UCC_TL_SPIN_WORKER_TYPE_RX) {
                    ibv_dereg_mr(self->workers[i].staging_rbuf_mr[j]);
                    free(self->workers[i].staging_rbuf[j]);
                }
                free(self->workers[i].staging_rbuf);
            }
        }
        if (self->workers[i].cq) {
            ibv_destroy_cq(self->workers[i].cq);
        }
    }

    for (i = 0; i < ctx->cfg.n_mcgs; i++) {
        ucc_tl_spin_team_fini_mcgs(ctx, &self->mcgs_infos[i].saddr);
    }

    pthread_mutex_destroy(&self->signal_mutex);

    ucc_free(self->workers);

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

static int subgroup_id = 0;

static ucc_status_t ucc_tl_spin_team_prepare_mcgs(ucc_base_team_t *tl_team,
                                                  ucc_tl_spin_mcast_join_info_t *worker_mcgs_info,
                                                  int mcgs_id)
{
    ucc_tl_spin_team_t            *team       = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_context_t         *ctx        = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_base_lib_t                *lib        = UCC_TL_SPIN_CTX_LIB(ctx);
    ucc_status_t                   status     = UCC_OK;
    struct sockaddr_in6            mcgs_saddr = {.sin6_family = AF_INET6};
    ucc_tl_spin_mcast_join_info_t  mcgs_info;
    ucc_service_coll_req_t        *bcast_req;

    memset(&mcgs_info, 0, sizeof(mcgs_info));

    if (team->subset.myrank == 0) { // root obtains group gid/lid
        mcgs_saddr.sin6_flowinfo = subgroup_id++; // TODO: enumerate multicast subgroup in IP address
        status = ucc_tl_spin_team_join_mcgs(ctx, &mcgs_saddr, &mcgs_info, 1);
        (void)status; // send status to non-root ranks first and then fail
        mcgs_info.magic_num = UCC_TL_SPIN_JOIN_MAGICNUM;
    }

    status = ucc_tl_spin_team_service_bcast_post(team,
                                                 &mcgs_info,
                                                 sizeof(ucc_tl_spin_mcast_join_info_t), 
                                                 0,
                                                 &bcast_req);
    ucc_assert_always(status == UCC_OK);
    status = ucc_tl_spin_team_service_coll_test(bcast_req, 1); // TODO: make non-blocking
    ucc_assert_always(status == UCC_OK);
    if (mcgs_info.status != UCC_OK) {
        return UCC_ERR_NO_RESOURCE;
    } else {
        ucc_assert_always(mcgs_info.magic_num == UCC_TL_SPIN_JOIN_MAGICNUM);
    }


    memcpy(&mcgs_saddr.sin6_addr, mcgs_info.mcsg_addr.gid.raw, sizeof(mcgs_info.mcsg_addr.gid.raw));
    status = ucc_tl_spin_team_join_mcgs(ctx, &mcgs_saddr, &mcgs_info, 0);
    assert(status == UCC_OK);

    if (mcgs_info.status != UCC_OK) {
        return UCC_ERR_NO_RESOURCE;
    }

    mcgs_info.saddr   = mcgs_saddr;
    *worker_mcgs_info = mcgs_info;

    tl_debug(lib, "team: %p, rank: %d joined multicast group: %d",
             tl_team, team->subset.myrank, mcgs_id);

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
    int                        tx_qpn;
    int                        rx_qpn;
    int                        i, j;

    for (i = 0; i < n_workers; i++) {
        worker = &team->workers[i];
        tx_qpn = rx_qpn = 0;
        for (j = 0; j < worker->n_mcgs; j++) {
            if (worker->type == UCC_TL_SPIN_WORKER_TYPE_TX) {
                status = ucc_tl_spin_team_setup_mcast_qp(ctx, worker, &team->mcgs_infos[tx_qpn], 1, tx_qpn);
                assert(status == UCC_OK);
                tx_qpn++;
            } else {
                ucc_assert(worker->type == UCC_TL_SPIN_WORKER_TYPE_RX);
                status = ucc_tl_spin_team_setup_mcast_qp(ctx, worker, &team->mcgs_infos[rx_qpn], 0, rx_qpn);
                ucc_assert(status == UCC_OK);
                ucc_debug("preposting worker_id=%d rx_qpn=%d", i, j);
                status = ucc_tl_spin_team_prepost_mcast_qp(ctx, worker, rx_qpn);
                ucc_assert(status == UCC_OK);
                rx_qpn++;
            }
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
    void *(*worker_fn)(void *);
    int i;

    team->workers_signal = UCC_TL_SPIN_WORKER_INIT;

    // TODO: Init workers context & signaling between ctrl_ctx
    for (i = 0; i < n_workers; i++) {
        worker    = &team->workers[i];
        worker_fn = worker->type == UCC_TL_SPIN_WORKER_TYPE_TX ? 
                    ucc_tl_spin_coll_worker_tx : 
                    ucc_tl_spin_coll_worker_rx;
        UCC_TL_SPIN_CHK_ERR(lib,
                            pthread_create(&worker->pthread, NULL, worker_fn, (void *)worker), 
                            status, UCC_ERR_NO_RESOURCE, err);
    }

err:
    return status;
}

ucc_status_t ucc_tl_spin_team_create_test(ucc_base_team_t *tl_team)
{
    ucc_tl_spin_team_t        *team      = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_context_t     *ctx       = UCC_TL_SPIN_TEAM_CTX(team);
    ucc_base_lib_t            *lib       = UCC_TL_TEAM_LIB(team);
    int i;

    // Create and connect RC QPs
    ucc_tl_spin_team_init_p2p_qps(tl_team);

    for (i = 0; i < ctx->cfg.n_mcgs; i++) {
        ucc_tl_spin_team_prepare_mcgs(tl_team, &team->mcgs_infos[i], i);
        ucc_tl_spin_team_init_mcast_qps(tl_team);
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