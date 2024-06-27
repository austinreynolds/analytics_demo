-- Insert comma-separated currency fields from SF1 table
-- Optionally impute NULL to 0
{% macro get_sf1_currency_cols(coalesce_to_zero=True) %}
{% for c in var('sf1_currency_fields') %}
{% if coalesce_to_zero %}
COALESCE({{c}}, 0) AS {{c}}
{% else %}
{{c}}
{% endif %}{% if not loop.last %},{% endif %}
{% endfor %}
{% endmacro %}

-- Insert comma-separated ratio fields from SF1 table
{% macro get_sf1_ratio_cols() %}
{% for c in var('sf1_ratio_fields') %}
{{c}}{% if not loop.last %},{% endif %}
{% endfor %}
{% endmacro %}