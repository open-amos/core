{% test null_for_fund_type(model, column_name, fund_type_value) %}

select *
from {{ model }}
where fund_type = '{{ fund_type_value }}'
    and {{ column_name }} is not null

{% endtest %}
