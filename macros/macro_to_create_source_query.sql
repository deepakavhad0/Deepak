{% macro macro_to_create_source_query(client_id, model_name) %}
    {%- set my_quote = "'" -%}
    {%- set query -%}
        select *
        from meta.job_cnf_tbl_info
        where
            clnt_vertical_bu_id in (
                select clnt_vertical_bu_id
                from meta.meta_bu_vertical_client_info
                where 
                    client_name = {{ my_quote ~ client_id ~ my_quote }}
                    and model_name = {{ my_quote ~ model_name ~ my_quote }}
                    and JOB_LAYER='SRC-BRNZ' 
            )
            and enable_flag = 1 
        order by edw_job_id
    {%- endset -%}

    {%- set result = run_query(query) -%}
    {%- if result is not none and result.columns is not none and result.rows | length > 0 -%}
        {%- for row in result.rows -%}
            {{ log("Processing row: " ~ row, info=True) }}
            {% set return_values = {
                "p_clnt_vertical_bu_id": row[1],
                "p_job_layer": row[2],
                "p_model_name": row[3],
                "p_src_db": row[4],
                "p_src_sch": row[5],
                "p_src_obj": row[6],
                "p_src_cdc_column": row[7],
                "p_src_query": row[8],
                "p_tgt_db": row[11],
                "p_tgt_sch": row[12],
                "p_tgt_obj": row[13],
                "p_flie_path": row[14],
                "p_file_name": row[15],
                "p_file_type": row[16],
                "p_job_type": row[17],
                "p_is_full_load": row[18],
                "p_enable_flag": row[22],
            } %}
            {{ return(return_values) }}
        {%- endfor -%}
    {%- else -%}
        {{ log("No results found or query execution failed.", info=True) }}
        {% set return_values = {
            "p_clnt_vertical_bu_id": "vertical_id",
            "p_job_layer": "default_job_layer",
            "p_model_name": "default_model_name",
            "p_src_db": "default_src_db",
            "p_src_sch": "default_src_sch",
            "p_src_obj": "default_src_obj",
            "p_src_cdc_column": "",
            "p_src_query": "",
            "p_tgt_db": "default_trg_db",
            "p_tgt_sch": "default_trg_sch",
            "p_tgt_obj": "default_trg_obj",
            "p_flie_path": "default_file_path",
            "p_file_name": "default_file_name",
            "p_file_type": "default_file_type",
            "p_job_type": "default_job_type",
            "p_is_full_load": "default_load_type",
            "p_enable_flag": "default_enable_flag",
        } %}
        {{ return(return_values) }}
    {%- endif -%}
{% endmacro %}