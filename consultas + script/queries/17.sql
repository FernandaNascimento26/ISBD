SELECT
  SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM
  lineitem,
  part
WHERE
  p_partkey = l_partkey
  AND p_brand = 'Brand#23' -- substituindo :1
  AND p_container = 'MED BOX' -- substituindo :2
  AND l_quantity < (
    SELECT
      0.2 * AVG(l_quantity)
    FROM
      lineitem
    WHERE
      l_partkey = p_partkey
  );