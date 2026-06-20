{% set cols = [
    ('trip_id', 'count_distinct', 'total_trips'),
    ('fare_amount', 'sum', 'lifetime_revenue'),
    ('fare_amount', 'avg', 'avg_fare'),
    ('trip_start_time', 'max', 'last_trip_date')
] %}

select 
    c.customer_id,

    c.full_name,

    c.city,

    {% for col, agg, alias in cols  %}

        {% if agg == 'count_distinct' %}

            COUNT(DISTINCT F.{{col}}) as {{alias}}

        {% else %}

            {{agg}}(f.{{col}}) as {{alias}}

        {% endif %}

        {% if not loop.last %}
            ,
        {% endif %}

    {% endfor %}
    
from 

{{ref("FactTrips")}} f 

left join {{ref("DimCustomers")}} c

on f.customer_id = c.customer_id

group by c.customer_id, c.full_name, c.city
