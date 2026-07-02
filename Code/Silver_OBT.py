# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

# COMMAND ----------

# MAGIC %md 
# MAGIC # JINJA Template for OBT 

# COMMAND ----------

pip install Jinja2

# COMMAND ----------

jinja_config = [
    {
    'table': 'uber.silver.streaming_rides streaming_rides',
    'select': '''
        streaming_rides.ride_id,
        streaming_rides.confirmation_number,
        streaming_rides.passenger_id,
        streaming_rides.driver_id,
        streaming_rides.vehicle_id,
        streaming_rides.pickup_location_id,
        streaming_rides.dropoff_location_id,
        streaming_rides.vehicle_type_id,
        streaming_rides.vehicle_make_id,
        streaming_rides.payment_method_id,
        streaming_rides.ride_status_id,
        streaming_rides.pickup_city_id,
        streaming_rides.dropoff_city_id,
        streaming_rides.cancellation_reason_id,
        streaming_rides.passenger_name,
        streaming_rides.passenger_email,
        streaming_rides.passenger_phone,
        streaming_rides.driver_name,
        streaming_rides.driver_rating,
        streaming_rides.driver_phone,
        streaming_rides.driver_license,
        streaming_rides.vehicle_model,
        streaming_rides.vehicle_color,
        streaming_rides.license_plate,
        streaming_rides.pickup_address,
        streaming_rides.pickup_latitude,
        streaming_rides.pickup_longitude,
        streaming_rides.dropoff_address,
        streaming_rides.dropoff_latitude,
        streaming_rides.dropoff_longitude,
        streaming_rides.distance_miles,
        streaming_rides.duration_minutes,
        streaming_rides.booking_timestamp,
        streaming_rides.pickup_timestamp,
        streaming_rides.dropoff_timestamp,
        streaming_rides.base_fare,
        streaming_rides.distance_fare,
        streaming_rides.time_fare,
        streaming_rides.surge_multiplier,
        streaming_rides.subtotal,
        streaming_rides.tip_amount,
        streaming_rides.total_fare,
        streaming_rides.rating
    ''',
    'where': ''
    }, 
    {
        'table' : 'uber.bronze.map_vehicle_makes map_vehicle_makes', 
        'select' : 'map_vehicle_makes.vehicle_make',
        'where' : '',
        'on' : 'streaming_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id'
    }, 
    {
       'table' : 'uber.bronze.map_vehicle_types map_vehicle_types', 
        'select' : 'map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,       map_vehicle_types.per_minute',
        'where' : '',
        'on' : 'streaming_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id' 
    },
    {
         'table' : 'uber.bronze.map_ride_statuses map_ride_statuses', 
        'select' : 'map_ride_statuses.ride_status',
        'where' : '',
        'on' : 'streaming_rides.ride_status_id = map_ride_statuses.ride_status_id' 
    }, 
    {
        "table" : "uber.bronze.map_payment_methods map_payment_methods",
        "select" : "map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth",
        "where" : "",
        "on" : "streaming_rides.payment_method_id = map_payment_methods.payment_method_id"
    }, 
    {
        "table" : "uber.bronze.map_cities map_cities",
        "select" : "map_cities.city as pickup_city, map_cities.state, map_cities.region,map_cities.updated_at",
        "where" : "",
        "on" : "streaming_rides.pickup_city_id = map_cities.city_id"
    },
    {
         "table" : "uber.bronze.map_cancellation_reasons map_cancellation_reasons",
        "select" : "map_cancellation_reasons.cancellation_reason",
        "where" : "",
        "on" : "streaming_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id"

    }
    
]

# COMMAND ----------

from jinja2 import Template
jinja_str = '''
    SELECT 
        {% for config in jinja_config %}
            {{ config.select }}
                {% if not loop.last %}
                    ,
                {% endif %}
        {% endfor %}

    FROM  
        {% for config in jinja_config %}
            {% if loop.first %}
                {{config.table}}
            {% else %}
                LEFT JOIN {{config.table}} ON {{config.on}}
            {% endif %}
        {% endfor %}

        {% for config in jinja_config %}
            {% if loop.first %}
                {% if config.where != '' %}
                    WHERE 
                {% endif %}

            {% endif %}
            {{config.where}}
            {% if not loop.last %}
                {% if config.where != '' %}
                    AND
                {% endif %} 
            {% endif %}    
        {% endfor %}



'''

template = Template(jinja_str)
rendered_template = template.render(jinja_config=jinja_config)
print(rendered_template)


# COMMAND ----------

display(spark.sql(rendered_template))

# COMMAND ----------

