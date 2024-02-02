#include "tl_spin.h"

static ucc_config_field_t ucc_tl_spin_lib_config_table[] = {
    {"", "", NULL, ucc_offsetof(ucc_tl_spin_lib_config_t, super),
     UCC_CONFIG_TYPE_TABLE(ucc_tl_lib_config_table)},

    {"DUMMY_PARAM", "42", "Dummy parameter that affects the behavior or sPIN library",
     ucc_offsetof(ucc_tl_spin_lib_config_t, dummy_param),
     UCC_CONFIG_TYPE_UINT},

    {NULL}};

UCC_CLASS_DEFINE_NEW_FUNC(ucc_tl_spin_lib_t, ucc_base_lib_t,
                          const ucc_base_lib_params_t *,
                          const ucc_base_config_t *);

UCC_CLASS_DEFINE_DELETE_FUNC(ucc_tl_spin_lib_t, ucc_base_lib_t);

ucc_status_t ucc_tl_spin_get_lib_attr(const ucc_base_lib_t *lib,
                                      ucc_base_lib_attr_t  *base_attr);

ucc_status_t ucc_tl_spin_get_lib_properties(ucc_base_lib_properties_t *prop);

static ucc_config_field_t ucc_tl_spin_context_config_table[] = {
    {"", "", NULL, ucc_offsetof(ucc_tl_spin_context_config_t, super),
     UCC_CONFIG_TYPE_TABLE(ucc_tl_context_config_table)},

    {"IB_DEV_NAME", "", "IB device that will be used for context creation",
     ucc_offsetof(ucc_tl_spin_context_config_t, ib_dev_name),
     UCC_CONFIG_TYPE_STRING},

    {NULL}};

UCC_CLASS_DEFINE_NEW_FUNC(ucc_tl_spin_context_t, ucc_base_context_t,
                          const ucc_base_context_params_t *,
                          const ucc_base_config_t *);

UCC_CLASS_DEFINE_DELETE_FUNC(ucc_tl_spin_context_t, ucc_base_context_t);

ucc_status_t ucc_tl_spin_get_context_attr(const ucc_base_context_t *context,
                                          ucc_base_ctx_attr_t *     base_attr);

UCC_CLASS_DEFINE_NEW_FUNC(ucc_tl_spin_team_t, ucc_base_team_t,
                          ucc_base_context_t *, const ucc_base_team_params_t *);

ucc_status_t ucc_tl_spin_team_create_test(ucc_base_team_t *tl_team);

ucc_status_t ucc_tl_spin_team_destroy(ucc_base_team_t *tl_team);

ucc_status_t ucc_tl_spin_coll_init(ucc_base_coll_args_t *coll_args,
                                   ucc_base_team_t *team,
                                   ucc_coll_task_t **task);

ucc_status_t ucc_tl_spin_team_get_scores(ucc_base_team_t   *tl_team,
                                         ucc_coll_score_t **score_p);

UCC_TL_IFACE_DECLARE(spin, SPIN);

__attribute__((constructor)) static void tl_spin_iface_init(void)
{
}