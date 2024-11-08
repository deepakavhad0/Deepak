{{
    config(
        materialized="table",
        schema=var("sch"),
        transient=false,
        alias=var("tbl_nm"),
    )
}}

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
    {# Use the returned values in your SQL statement #}

    {{ p_src_query }}

{% else %} {{ log("Source data is missing or incomplete.", info=True) }}
{# Handle the case where source data is missing or incomplete #}
{% endif %}
