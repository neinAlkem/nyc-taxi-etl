SELECT *
FROM UNNEST(
        [
  STRUCT(1 AS VendorID, 'Creative Mobile Technologies, LLC' AS vendor_name),
  STRUCT(2, 'Curb Mobility, LLC'),
  STRUCT(6, 'Myle Technologies Inc'),
  STRUCT(7, 'Helix')
]
    )
