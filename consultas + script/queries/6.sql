SELECT
  SUM(l_extendedprice * l_discount) AS revenue
FROM
  lineitem
WHERE
  l_shipdate >= DATE '1994-01-01' -- substituindo :1
  AND l_shipdate < DATE '1995-01-01' -- :1 + 1 ano
  AND l_discount BETWEEN 0.05 - 0.01 AND 0.05 + 0.01 -- substituindo :2
  AND l_quantity < 24; -- substituindo :3