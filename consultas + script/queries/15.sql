SELECT
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
FROM
  supplier,
  revenue_view
WHERE
  s_suppkey = supplier_no
  AND total_revenue = (
    SELECT
      MAX(total_revenue)
    FROM
      revenue_view
  )
ORDER BY
  s_suppkey;