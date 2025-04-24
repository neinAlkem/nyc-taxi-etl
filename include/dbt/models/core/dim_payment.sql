SELECT *
FROM UNNEST(
        [
  STRUCT(0 AS payment_type, 'Flex Fare trip' AS payment_desc),
  STRUCT(1, 'Credit card'),
  STRUCT(2, 'Cash'),
  STRUCT(3, 'No charge'),
  STRUCT(4, 'Dispute'),
  STRUCT(5, 'Unknown'),
  STRUCT(6, 'Voided trip')
]
    )
