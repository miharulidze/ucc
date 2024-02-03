#include "coll_score/ucc_coll_score.h"
#include "core/ucc_service_coll.h"

#include "tl_spin.h"
#include "tl_spin_coll.h"

/*
static ucc_status_t 
ucc_tl_spin_team_service_bcast_post(ucc_tl_spin_team_t *ctx, 
                                    void *buf, size_t size, ucc_rank_t root,
                                    ucc_service_coll_req_t **bcast_req)
{
    ucc_status_t            status = UCC_OK;
    ucc_team_t             *team   = ctx->base_team;
    ucc_subset_t            subset = ctx->subset;
    ucc_service_coll_req_t *req    = NULL;

    status = ucc_service_bcast(team, buf, size, subset.myrank, subset, &req);
    if (ucc_unlikely(UCC_OK != status)) {
        tl_error(UCC_TL_SPIN_TEAM_LIB(ctx), "tl service team bcast failed");
        return status;
    }

    *bcast_req = req;

    return status;
}
*/

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
    ucc_context_t             *core_ctx  = tl_context->ucc_context;
    ucc_tl_spin_worker_info_t *worker    = NULL;
    ucc_status_t               status    = UCC_OK;
    int                        n_workers = ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers;
    int i;

    ucc_assert(ctx->cfg.n_mcgs % ctx->cfg.n_tx_workers == 0);
    ucc_assert(ctx->cfg.n_mcgs % ctx->cfg.n_rx_workers == 0);

    if (!core_ctx->service_team) {
        tl_debug(tl_context->lib, "failed to init ctx: need service team");
        return UCC_ERR_NO_MESSAGE;
    }
    ucc_assert(tl_context->params.mask & UCC_CONTEXT_PARAM_FIELD_OOB);
    self->base_team     = params->team;
    self->subset.myrank = params->rank;
    self->subset.map    = params->map;
    self->size          = params->size;

    UCC_CLASS_CALL_SUPER_INIT(ucc_tl_team_t, &ctx->super, params);
    UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                        ucc_calloc(n_workers + 1, sizeof(ucc_tl_spin_worker_info_t)),
                        self->workers,
                        status, UCC_ERR_NO_MEMORY, ret);

    // FIXME in case of error this will leak
    // construct multicast subgroups workers
    for (i = 0; i < n_workers; i++) {
        worker         = &self->workers[i];
        worker->type   = i < ctx->cfg.n_tx_workers ? 
                         UCC_TL_SPIN_WORKER_TYPE_TX : 
                         UCC_TL_SPIN_WORKER_TYPE_RX;
        worker->n_mcgs = i < ctx->cfg.n_tx_workers ? 
                         ctx->cfg.n_mcgs / ctx->cfg.n_tx_workers : 
                         ctx->cfg.n_mcgs / ctx->cfg.n_rx_workers;
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ucc_calloc(worker->n_mcgs, sizeof(struct ibv_qp *)),
                            worker->qps, status, UCC_ERR_NO_MEMORY, ret);
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ucc_calloc(worker->n_mcgs, sizeof(struct rdma_cm_event *)), 
                            worker->mcast_events, status, UCC_ERR_NO_MEMORY, ret);
        UCC_TL_SPIN_CHK_PTR(tl_context->lib,
                            ibv_create_cq(ctx->mcast.dev, ctx->cfg.mcast_cq_depth, NULL, NULL, 0), 
                            worker->cq, status, UCC_ERR_NO_MEMORY, ret);
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
    ucc_tl_spin_context_t *ctx       = UCC_TL_SPIN_TEAM_CTX(self);
    int                    n_workers = ctx->cfg.n_tx_workers + ctx->cfg.n_rx_workers;
    int i;

    if (self->ctrl_ctx->qps) {
        ibv_destroy_qp(self->ctrl_ctx->qps[0]);
        ibv_destroy_qp(self->ctrl_ctx->qps[1]);
        ucc_free(self->ctrl_ctx->qps);
    }

    for (i = 0; i < n_workers; i++) {
        pthread_join(self->workers[i].pthread, NULL);
        if (self->workers[i].qps) {
            ucc_free(self->workers[i].qps);
        }
        if (self->workers[i].mcast_events) {
            ucc_free(self->workers[i].mcast_events);
        }
        if (self->workers[i].cq) {
            ibv_destroy_cq(self->workers[i].cq);
        }
    }
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

static ucc_status_t ucc_tl_spin_team_prepare_mcgs(ucc_base_team_t *tl_team)
{
    (void)tl_team;
    return UCC_OK;
}

static ucc_status_t ucc_tl_spin_team_init_mcast_qps(ucc_base_team_t *tl_team)
{
    (void)tl_team;
    return UCC_OK;
}

static void ucc_tl_spin_team_init_rc_qp_attr(struct ibv_qp_init_attr *attr,
                                             struct ibv_cq *cq,
                                             uint32_t qp_depth)
{
    memset(attr, 0, sizeof(*attr));
    attr->qp_type          = IBV_QPT_RC;
    attr->sq_sig_all       = 0;
    attr->send_cq          = cq;
    attr->recv_cq          = cq;
    attr->cap.max_send_wr  = qp_depth;
    attr->cap.max_recv_wr  = qp_depth;
    attr->cap.max_send_sge = 1;
    attr->cap.max_recv_sge = 1;
    //attr.cap.max_inline_data = 128;
}

static ucc_status_t 
ucc_tl_spin_team_connect_rc_qp(ucc_base_lib_t *lib, struct ibv_qp *qp, 
                               ucc_tl_spin_qp_addr_t *local_addr,
                               ucc_tl_spin_qp_addr_t *remote_addr)
{
    ucc_status_t       status = UCC_OK;
    struct ibv_qp_attr attr;

    memset(&attr, 0, sizeof(struct ibv_qp_attr));
	attr.qp_state        = IBV_QPS_INIT;
    attr.pkey_index      = UCC_TL_SPIN_DEFAULT_PKEY;
    attr.port_num        = local_addr->dev_addr.port_num;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE   |
                           IBV_ACCESS_REMOTE_WRITE  |
                           IBV_ACCESS_REMOTE_ATOMIC |
                           IBV_ACCESS_REMOTE_READ;
    UCC_TL_SPIN_CHK_ERR(lib,
                        ibv_modify_qp(qp, &attr,
                                      IBV_QP_STATE      |
                                      IBV_QP_PKEY_INDEX |
                                      IBV_QP_PORT       |
                                      IBV_QP_ACCESS_FLAGS),
                        status, UCC_ERR_NO_RESOURCE, ret);

    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state                  = IBV_QPS_RTR;
    attr.path_mtu		           = local_addr->dev_addr.mtu;
    attr.dest_qp_num	           = remote_addr->qpn;
    attr.rq_psn                    = 0;
    attr.max_dest_rd_atomic        = 16;
    attr.min_rnr_timer             = 30;
    attr.ah_attr.is_global         = 1;
    attr.ah_attr.grh.flow_label    = 0;
    attr.ah_attr.grh.hop_limit     = 1;
    attr.ah_attr.grh.traffic_class = 0;
    attr.ah_attr.dlid              = remote_addr->dev_addr.lid;
    attr.ah_attr.sl	     	       = 0;
    attr.ah_attr.src_path_bits     = 0;
    attr.ah_attr.port_num	       = local_addr->dev_addr.port_num;
    //attr.ah_attr.grh.dgid          = remote_addr.dev_addr.gid;
    //attr.ah_attr.grh.sgid_index    = remote_addr.dev_addr.gid_table_index;
    UCC_TL_SPIN_CHK_ERR(lib,
                        ibv_modify_qp(qp, &attr,
                                      IBV_QP_STATE    | IBV_QP_AV     | IBV_QP_PATH_MTU |
                                      IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                                      IBV_QP_MIN_RNR_TIMER), 
                        status, UCC_ERR_NO_RESOURCE, ret);

    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state      = IBV_QPS_RTS;
    attr.sq_psn        = 0;
    attr.timeout       = 0;
    attr.retry_cnt     = 0; //rnr_retry ? 7 : 0;
    attr.rnr_retry     = 0; //rnr_retry ? 7 : 0;
    attr.max_rd_atomic = 16; //ok
    UCC_TL_SPIN_CHK_ERR(lib,
                        ibv_modify_qp(qp, &attr,
                                      IBV_QP_STATE     | IBV_QP_SQ_PSN    | IBV_QP_TIMEOUT |
                                      IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC),
                        status, UCC_ERR_NO_RESOURCE, ret);

ret:
    return status;
}

static ucc_status_t ucc_tl_spin_team_init_p2p_qps(ucc_base_team_t *tl_team)
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
        ucc_assert(ctx->p2p.pd);
        ucc_assert(ctrl_worker->qps[i]);
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
    ucc_assert(status == UCC_OK);
    status = ucc_tl_spin_team_connect_rc_qp(lib, ctrl_worker->qps[1], &send_av[1],
                                            &recv_av[r_neighbor * 2]);
    ucc_assert(status == UCC_OK);

    tl_debug(lib,
             "connected p2p context of rank %d, "
             "left neighbor rank %d (lid:qpn): %d:%d to %d:%d, "
             "right neighbor rank %d (lid:qpn): %d:%d to %d:%d\n",
             team->subset.myrank,
             l_neighbor, send_av[0].qpn, send_av[0].dev_addr.lid, 
             recv_av[l_neighbor * 2 + 1].qpn, recv_av[l_neighbor * 2 + 1].dev_addr.lid,
             r_neighbor, send_av[1].qpn, send_av[1].dev_addr.lid, 
             recv_av[r_neighbor * 2].qpn, recv_av[r_neighbor * 2].dev_addr.lid);

    ucc_free(recv_av);
ret:
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
    //ucc_tl_spin_context_t     *ctx       = UCC_TL_SPIN_TEAM_CTX(team);
    //ucc_base_lib_t            *lib       = UCC_TL_TEAM_LIB(team);

    // Setup multicast subgroups
    ucc_tl_spin_team_prepare_mcgs(tl_team);
    ucc_tl_spin_team_init_mcast_qps(tl_team);

    // Create QPs and transit QP state
    ucc_tl_spin_team_init_p2p_qps(tl_team);

    // Spawn worker threads
    ucc_tl_spin_team_spawn_workers(tl_team);

    tl_info(tl_team->context->lib, "initialized tl team: %p", team);
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