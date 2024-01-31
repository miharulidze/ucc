#include "coll_score/ucc_coll_score.h"

#include "tl_spin.h"
#include "tl_spin_coll.h"

UCC_CLASS_INIT_FUNC(ucc_tl_spin_team_t, ucc_base_context_t *tl_context,
                    const ucc_base_team_params_t *params)
{
    ucc_tl_spin_context_t *ctx =
        ucc_derived_of(tl_context, ucc_tl_spin_context_t);
    UCC_CLASS_CALL_SUPER_INIT(ucc_tl_team_t, &ctx->super, params);
    tl_info(tl_context->lib, "posted tl team: %p", self);
    return UCC_OK;
}

UCC_CLASS_CLEANUP_FUNC(ucc_tl_spin_team_t)
{
    tl_info(self->super.super.context->lib, "finalizing tl team: %p", self);
}

UCC_CLASS_DEFINE_DELETE_FUNC(ucc_tl_spin_team_t, ucc_base_team_t);
UCC_CLASS_DEFINE(ucc_tl_spin_team_t, ucc_tl_team_t);

ucc_status_t ucc_tl_spin_team_destroy(ucc_base_team_t *tl_team)
{
    UCC_CLASS_DELETE_FUNC_NAME(ucc_tl_spin_team_t)(tl_team);
    return UCC_OK;
}

ucc_status_t ucc_tl_spin_team_create_test(ucc_base_team_t *tl_team)
{
    ucc_tl_spin_team_t    *team = ucc_derived_of(tl_team, ucc_tl_spin_team_t);
    ucc_tl_spin_context_t *ctx  = UCC_TL_SPIN_TEAM_CTX(team);

    /* ... TEST OOB Exchange for completion and proceed */
    (void)ctx;

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