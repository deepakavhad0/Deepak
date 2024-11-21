-- macros/audit_logging.sql
{% macro audit_logging(SRC_DB, SRC_SCH, SRC_TBL, START_DT, END_DT, TGT_DB, TGT_SCH, TGT_TBL) %}
insert into LOG.TBL_AUDIT_LOG (
    SRC_DB,
    SRC_SCH,
    SRC_TBL,
    START_DT,
    END_DT,
    TGT_DB,
    TGT_SCH,
    TGT_TBL
) values (
    '{{ SRC_DB }}',
    '{{ SRC_SCH }}',
    '{{ SRC_TBL }}',
    '{{ START_DT }}',
    '{{ END_DT }}',
    '{{ TGT_DB }}',
    '{{ TGT_SCH }}',
    '{{ TGT_TBL }}'
);
{% endmacro %}
