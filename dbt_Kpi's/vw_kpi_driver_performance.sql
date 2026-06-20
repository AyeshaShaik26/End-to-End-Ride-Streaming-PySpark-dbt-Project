-- select sum(fare_amount), avg(distance_km), count(trip_id), driver_id
-- from facttrips f 
-- left join dimdrivers dd
-- on f.driver_id = dd.driver_id
-- group by f.driver_id

{% set cols = [('fare_amount', 'sum'), ('distance_km', 'avg'), ('trip_id', 'count')] %}

select 

    f.driver_id,


    {% for col, agg in cols  %}

        {{agg}}({{col}}) AS {{agg}}_{{col}}

            {% if not loop.last %}
                ,
            {% endif %}

    {% endfor %}

from  {{ref('FactTrips')}} f

left join {{ref("DimDrivers")}} dd

on f.driver_id = dd.driver_id

group by f.driver_id

order by f.driver_id asc
