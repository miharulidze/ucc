#ifndef UCC_TL_SPIN_P2P_H_
#define UCC_TL_SPIN_P2P_H_

#include "tl_spin.h"

ucc_status_t 
ucc_tl_spin_team_rc_ring_barrier(ucc_rank_t rank, ucc_tl_spin_worker_info_t *ctx);
ucc_status_t 
ucc_tl_spin_team_connect_rc_qp(ucc_base_lib_t *lib, struct ibv_qp *qp, 
                               ucc_tl_spin_qp_addr_t *local_addr,
                               ucc_tl_spin_qp_addr_t *remote_addr);
void ucc_tl_spin_team_init_rc_qp_attr(struct ibv_qp_init_attr *attr,
                                      struct ibv_cq *cq,
                                      uint32_t qp_depth);
ucc_status_t 
ucc_tl_spin_team_prepost_rc_qp(ucc_tl_spin_context_t *ctx,
                               ucc_tl_spin_worker_info_t *worker,
                               int qp_id);
void ib_qp_rc_post_send(struct ibv_qp *qp, struct ibv_mr *mr, void *buf, 
                        uint32_t len, uint32_t imm_data, uint64_t id);

#endif