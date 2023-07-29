{% macro find_line_average(numerator, denominator, decimal_places=2) -%}
round(1.0 * sum({{ numerator }}) / count(distinct {{ denominator }}), {{ decimal_places }})
{%- endmacro %}