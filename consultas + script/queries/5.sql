SELECT
  n_name,
  SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
  customer,
  orders,
  lineitem,
  supplier,
  nation,
  region
WHERE
  c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA' -- substituindo :1
  AND o_orderdate >= DATE '1994-01-01' -- substituindo :2
  AND o_orderdate < DATE '1995-01-01' -- :2 + 1 ano
GROUP BY
  n_name
ORDER BY
  revenue DESC;