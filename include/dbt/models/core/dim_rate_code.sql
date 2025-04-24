SELECT *
FROM UNNEST(
        [
  STRUCT(1 AS ratecodeid, 'Standard rate' AS rateid_desc),
  STRUCT(2, 'JFK'),
  STRUCT(3, 'Newark'),
  STRUCT(4, 'Nassau or Westchester'),
  STRUCT(5, 'Negotiated fare'),
  STRUCT(6, 'Group ride'),
  STRUCT(99, 'Null/unknown')
]
    )
