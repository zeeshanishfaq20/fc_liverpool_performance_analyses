{% macro replace_zero_with_avg(column_name, avg_column_name) %}
    case 
        when {{ column_name }} = 0 then {{ avg_column_name }}
        else {{ column_name }}
    end
{% endmacro %}