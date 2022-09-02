{# surrogate_key  ----------------------------------     #}

{% macro oracle__surrogate_key(field_list) -%}
    ora_hash(
        {%- for field in field_list %}
            {% if not loop.last %}
                {{ field }} || '~' ||
            {% else %}
                {{ field }}
            {% endif %}
        {%- endfor %}
    )
{%- endmacro %}

{% macro clickhouse__surrogate_key(field_list) -%}
    assumeNotNull(hex(MD5(
        {%- for field in field_list %}
            {% if not loop.last %}
                toString({{ field }}) || '~' ||
            {% else %}
                toString({{ field }})
            {% endif %}
        {%- endfor %}
    )))
{%- endmacro %}

{% macro firebolt__surrogate_key(field_list) -%}
    md5(
        {%- for field in field_list %}
            {% if not loop.last %}
                ifnull(cast({{ field }} as string), '') || '~' ||
            {% else %}
                ifnull(cast({{ field }} as string), '')
            {% endif %}
        {%- endfor %}
    )
{%- endmacro %}
