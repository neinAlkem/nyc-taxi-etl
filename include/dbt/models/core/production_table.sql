WITH combine AS (
    SELECT code_trip,
        'Green' AS taxi_type,
        VendorID AS vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        RatecodeID AS ratecodeid,
        store_and_fwd_flag,
        PULocationID AS pickup_location_id,
        DOLocationID AS dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        CAST(NULL AS FLOAT64) AS airport_fee,
        total_amount,
        trip_type
    FROM { { ref(`staging_table_green`) } }
    UNION ALL
    SELECT code_trip,
        'Yellow' AS taxi_type,
        vendorid AS vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid,
        store_and_fwd_flag,
        pulocationid AS pickup_location_id,
        dolocationid AS dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        total_amount,
        CAST(NULL AS int64) AS trip_type
    FROM { { ref(`staging_table_yellow`) } }
)
SELECT *
FROM combine c
WHERE code_trip IS NOT IN (
        SELECT code_trip
        FROM { { ref(`production_table`) } }
    )
