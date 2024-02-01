#include "tl_spin.h"
#include "tl_spin_coll.h"

UCC_CLASS_INIT_FUNC(ucc_tl_spin_context_t,
                    const ucc_base_context_params_t *params,
                    const ucc_base_config_t *config)
{
    ucc_tl_spin_context_config_t *tl_spin_config =
        ucc_derived_of(config, ucc_tl_spin_context_config_t);
    ucc_status_t status = UCC_OK;
    UCC_CLASS_CALL_SUPER_INIT(ucc_tl_context_t, &tl_spin_config->super,
                              params->context);
    memcpy(&self->cfg, tl_spin_config, sizeof(*tl_spin_config));

    status = ucc_mpool_init(&self->req_mp, 0, sizeof(ucc_tl_spin_task_t), 0,
                            UCC_CACHE_LINE_SIZE, 8, UINT_MAX,
                            &ucc_coll_task_mpool_ops, params->thread_mode,
                            "tl_sharp_req_mp");
    if (status != UCC_OK) {
        tl_error(self->super.super.lib,
                 "failed to initialize tl_spin mpool");
        return UCC_ERR_NO_MEMORY;
    }

    tl_info(self->super.super.lib, "initialized tl context: %p", self);
    return UCC_OK;
}

UCC_CLASS_CLEANUP_FUNC(ucc_tl_spin_context_t)
{
    tl_info(self->super.super.lib, "finalizing tl context: %p", self);
    ucc_mpool_cleanup(&self->req_mp, 1);
}

UCC_CLASS_DEFINE(ucc_tl_spin_context_t, ucc_tl_context_t);

ucc_status_t
ucc_tl_spin_get_context_attr(const ucc_base_context_t *context, /* NOLINT */
                             ucc_base_ctx_attr_t *     attr)
{
    if (attr->attr.mask & UCC_CONTEXT_ATTR_FIELD_CTX_ADDR_LEN) {
        attr->attr.ctx_addr_len = 0;
    }
    attr->topo_required = 0;
    return UCC_OK;
}