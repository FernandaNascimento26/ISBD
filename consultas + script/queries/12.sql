SELECT
  l_shipmode,
  SUM(CASE
    WHEN o_orderpriority = '1-URGENT'
      OR o_orderpriority = '2-HIGH'
      THEN 1
    ELSE 0
  END) AS high_line_count,
  SUM(CASE
    WHEN o_orderpriority <> '1-URGENT'
      AND o_orderpriority <> '2-HIGH'
      THEN 1
    ELSE 0
  END) AS low_line_count
FROM
  orders,
  lineitem
WHERE
  o_orderkey = l_orderkey
  AND l_shipmode IN ('MAIL', 'SHIP') -- substituindo :1 e :2
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= DATE '1994-01-01' -- substituindo :3
  AND l_receiptdate < DATE '1995-01-01' -- :3 + 1 ano
GROUP BY
  l_shipmode
ORDER BY
  l_shipmode;