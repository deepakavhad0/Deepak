{% macro insert_data_into_audit_table(
    p_load_type,
    p_job_name,
    p_src_name,
    p_status,
    p_proc_typ_p_msg,
    p_integration_id,
    p_increment_flag,
    p_src_sch,
    p_src_obj,
    p_tgt_sch,
    p_tgt_obj
) %}
    -- {{ print("Result: "~ p_src_sch) }}
    {% set v_table_name = "ADT_AUDIT" %}

    {% set v_audit_table = (
        env_var("DBT_SRC_DB")
        ~ "."
        ~ env_var("DBT_LOG_SCH")
        ~ "."
        ~ env_var("DBT_LOG_OBJ")
    ) %}
    {% set v_load_type = "INS" if p_load_type is none else p_load_type %}
    {% set v_job_name = "NA" if p_job_name is none else p_job_name %}
    {% set v_src_sch = "NA" if p_src_sch is none else p_src_sch %}
    {% set v_stage = "NA" if p_stage is none else p_stage %}
    {% set v_increment_flag = "Y" if p_increment_flag is none else p_increment_flag %}
    {% set v_source = p_src_name if p_src_name is not none else null %}
    {{ log("Source Schema:" ~ v_src_sch, info=True) }}
    {{ log("Source table:" ~ p_src_obj, info=True) }}
    {{ log("target Schema:" ~ p_tgt_sch, info=True) }}
    {{ log("target table:" ~ p_tgt_obj, info=True) }}

    {% if p_job_name is none or p_src_name is none %}
        {{
            exceptions.raise_compiler_error(
                "Job Name and Source Name are required parameters."
            )
        }}
    {% endif %}

    {% if execute %}
        {% if p_load_type.upper() == "INS" %}
            {% set v_process_type = (
                p_proc_typ_p_msg if p_proc_typ_p_msg is not none else null
            ) %}
            {% set v_integration_id = (
                p_integration_id if p_integration_id is not none else null
            ) %}

            {%- call statement("get_audit_id", fetch_result=True) -%}
                select nvl(max(id), 0) + 1 from {{ v_audit_table }}
            {%- endcall -%}
            {%- set v_audit_id = load_result("get_audit_id")["data"][0][0] -%}

            {%- call statement("get_load_id", fetch_result=True) -%}
                select
                    decode(
                        '{{v_increment_flag}}',
                        'N',
                        nvl(max(load_id), 0),
                        nvl(max(load_id), 0) + 1
                    ) as load_id
                from {{ v_audit_table }}
                where data_source = '{{v_source}}' and lower(status) = 'success'
            {%- endcall -%}
            {%- set v_load_id = load_result("get_load_id")["data"][0][0] -%}

            {%- set insert_query -%}
        Insert into {{v_audit_table}} (id , job_name , data_source , SRC_DB , SRC_SCH , SRC_OBJ , TGT_DB , TGT_SCH , TGT_OBJ , start_ts , load_id, insert_ts , integration_id ) values
        ({{v_audit_id}},'{{v_job_name}}','{{v_source}}','{{env_var("DBT_SRC_DB")}}','{{v_src_sch}}','{{p_src_obj}}','{{env_var("DBT_SRC_DB")}}','{{p_tgt_sch}}','{{p_tgt_obj}}',current_timestamp,{{v_load_id}},current_timestamp,'{{v_integration_id}}')
            {%- endset -%}
            {{ run_query(insert_query) }}

            {{ return("Audit created") }}

        {% elif p_load_type.upper() == "UPD" %}
            {% set v_status = p_status if p_status is not none else null %}
            {% set v_message = (
                p_proc_typ_p_msg if p_proc_typ_p_msg is not none else null
            ) %}

            {%- set update_query -%}
        update {{v_audit_table}}
        set status = '{{v_status}}',
            message = '{{v_message}}',
            end_ts = current_timestamp 
        where (job_name = '{{v_job_name}}' --and stage = '{{v_stage}}'
            and id = (select max(id) from {{v_audit_table}} where data_source = '{{v_source}}' ))
            {%- endset -%}
            {{ run_query(update_query) }}
            {{ return("Audit updated") }}
        {% else %} {{ exceptions.raise_compiler_error("Invalid Load Type") }}
        {% endif %}
    {% endif %}
{% endmacro %}