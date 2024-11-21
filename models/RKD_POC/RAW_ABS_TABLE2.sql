-- model 

{{ config(materialized="table",
        schema=var("sch"),
        transient= false,
        alias=var("tbl_nm"),
        pre_hook="{% set status = insert_data_into_audit_table(
        model.config.ops_ins,
        model.name,
        model.config.src_name,
        model.config.status_start,
        model.config.proc_typ_msg_start,
        model.config.integration_id,
        model.config.flag,
        model.config.src_sch,
        model.config.src_obj,
        model.config.tgt_sch,
        model.config.tgt_obj
    ) %} {{ log('Pre-hook status: ' ~ status, info=True) }}",
        post_hook=["{% set status = insert_data_into_audit_table(
        model.config.ops_upd,
        model.name,
        model.config.src_name,       
        model.config.status_success,
        model.config.proc_typ_msg_success,
        model.config.integration_id,
        ''
    ) %} {{ log('Post-hook status: ' ~ status, info=True) }}"]

) }}

{{ log("Configured schema: BRNZ_ABS", info=True) }}

{% set clt_id = var("clnt_id") %}
{% set mdl_nm = var("mn") %}

{# Call the macro to get source data #}
{% if execute %}
    {% set source_data = macro_to_create_source_query(clt_id, mdl_nm) %}
{% endif %}

{# Debug log to check the source_data #}
{{ log("Source Data: " ~ source_data, info=True) }}

{# Ensure the source_data is not none and has the expected keys #}
{% if source_data is not none and "p_src_sch" in source_data and "p_src_obj" in source_data %}
    {% set p_clnt_vertical_bu_id = source_data["p_clnt_vertical_bu_id"] %}
    {% set p_job_layer = source_data["p_job_layer"] %}
    {% set p_model_name = source_data["p_model_name"] %}
    {% set p_src_db = source_data["p_src_db"] %}
    {% set p_src_sch = source_data["p_src_sch"] %}
    {% set p_src_obj = source_data["p_src_obj"] %}
    {% set p_src_cdc_column = source_data["p_src_cdc_column"] %}
    {% set p_src_query = source_data["p_src_query"] %}
    {% set p_tgt_db = source_data["p_tgt_db"] %}
    {% set p_tgt_sch = source_data["p_tgt_sch"] %}
    {% set p_tgt_obj = source_data["p_tgt_obj"] %}
    {% set p_flie_path = source_data["p_flie_path"] %}
    {% set p_file_name = source_data["p_file_name"] %}
    {% set p_file_type = source_data["p_file_type"] %}
    {% set p_job_type = source_data["p_job_type"] %}
    {% set p_is_full_load = source_data["p_is_full_load"] %}
    {% set p_enable_flag = source_data["p_enable_flag"] %}

    {{ p_src_query }}

{% else %} {{ log("Source data is missing or incomplete.", info=True) }}
{# Handle the case where source data is missing or incomplete #}
{% endif %}