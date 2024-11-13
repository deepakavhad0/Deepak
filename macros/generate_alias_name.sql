<<<<<<< HEAD
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name -%} {{ custom_alias_name | trim }}
    {%- elif node.version -%}
        {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}
    {%- else -%} {{ node.name }}
    {%- endif -%}
{%- endmacro %}
=======
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name -%} {{ custom_alias_name | trim }}
    {%- elif node.version -%}
        {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}
    {%- else -%} {{ node.name }}
    {%- endif -%}
{%- endmacro %}
>>>>>>> e81ff59ad9371d01535cf18332c2fb1b7f5b0771
