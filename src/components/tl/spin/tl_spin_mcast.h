#ifndef UCC_TL_SPIN_MCAST_H_
#define UCC_TL_SPIN_MCAST_H_

#include "tl_spin.h"

ucc_status_t
ucc_tl_spin_team_fini_mcg(ucc_tl_spin_context_t *ctx, struct sockaddr_in6 *mcg_addr);
ucc_status_t ucc_tl_spin_mcast_join_mcast_post(ucc_tl_spin_context_t *ctx,
                                               struct sockaddr_in6 *net_addr,
                                               int is_root);
ucc_status_t ucc_tl_spin_mcast_join_mcast_test(ucc_tl_spin_context_t *ctx,
                                               struct rdma_cm_event **event,
                                               int is_root,
                                               int is_blocking);
ucc_status_t
ucc_tl_spin_team_join_mcg(ucc_tl_spin_context_t *ctx, struct sockaddr_in6 *mcg_saddr, 
                           ucc_tl_spin_mcast_join_info_t *info, int is_root);
ucc_status_t
ucc_tl_spin_team_setup_mcast_qp(ucc_tl_spin_context_t *ctx,
                                ucc_tl_spin_worker_info_t *worker,
                                ucc_tl_spin_mcast_join_info_t *mcg_info,
                                int is_tx_qp, int qp_id);
ucc_status_t 
ucc_tl_spin_team_prepost_mcast_qp(ucc_tl_spin_context_t *ctx,
                                  ucc_tl_spin_worker_info_t *worker,
                                  int qp_id);

void
ib_qp_ud_post_mcast_send(struct ibv_qp *qp, struct ibv_ah *ah,
                         struct ibv_mr *mr, void *buf, uint32_t len, uint64_t id);
void ib_qp_post_recv(struct ibv_qp *qp, struct ibv_mr *mr,
                     void *buf, uint32_t len, uint64_t id);
int ib_cq_poll(struct ibv_cq *cq, int max_batch_size, struct ibv_wc *wcs);

#endif