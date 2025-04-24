SELECT *
FROM UNNEST(
        [
  STRUCT('Y' AS payment_type, 'Store and forward trip' AS payment_desc),
  STRUCT('N', 'Not a store and forward trip')
]
    )
