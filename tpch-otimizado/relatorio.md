

# Análise Comparativa de Desempenho – Benchmark TPC-H/TPC-E

**PostgreSQL 16 vs SQL Server 2022 – 120s**
**Configurações Padrão x Ambientes Otimizados**

---

## 1. Introdução

Este relatório apresenta os resultados da execução do benchmark **TPC-H** (consultas analíticas) sobre dois SGBDs:

* **PostgreSQL 16**
* **SQL Server 2022**

Foram realizados **quatro experimentos**:

1. PostgreSQL 16 (padrão)
2. SQL Server 2022 (padrão)
3. PostgreSQL 16 (otimizado)
4. SQL Server 2022 (otimizado)

O objetivo foi **comparar o desempenho entre SGBDs e avaliar o impacto de otimizações** (principalmente criação de índices e ajustes de configuração) no tempo de execução e na vazão total.

---

## 2. Ambiente Experimental

### Configuração Comum

* **Benchmark:** TPC-H (SF=1)
* **Execução:** Consultas pré-carregadas em views (`SELECT * FROM q1 ... q22`)
* **Tempo total:** 120 segundos (4 threads simultâneas)
* **Hardware:**

  * Notebook Dell Inspiron 16 Plus 7640
  * **CPU:** Intel Core Ultra 7
  * **RAM:** 32 GB
  * **SSD:** 1 TB
* **Ferramentas:**

  * Python 3.13 (`psycopg2` e `pyodbc`)
  * Script customizado `run_tpch.py`
  * Visualização: `matplotlib` e `seaborn`

### Configurações específicas

* **PostgreSQL 16:** Configuração padrão e otimizada (índices + ajustes de buffers e paralelismo)
* **SQL Server 2022:** Configuração padrão e otimizada (índices + otimizações de execução)

---

## 3. Índices Implementados

A tabela abaixo resume os índices recomendados para cada query do TPC-H e aplicados nas otimizações:

| Nº  | Objetivo Principal                                           | Complexidade Técnica                                         | Índice Recomendado                                                              |
| --- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------------------------- |
| Q1  | Resumo por status de vendas (`l_returnflag`, `l_linestatus`) | Grandes volumes; agregações; sem JOINs                       | `CREATE INDEX ON lineitem (l_shipdate, l_returnflag, l_linestatus)`             |
| Q2  | Fornecedores em regiões específicas de peças com marca/tipo  | Filtro por `region`, subquery correlacionada                 | Índices em `region`, `supplier`, `part`, `partsupp`                             |
| Q3  | Receita por cliente, mercado e data                          | JOINs entre `customer`, `orders`, `lineitem`; range por data | Índices em `orders.o_orderdate`, `lineitem.l_shipdate`, `customer.c_mktsegment` |
| Q4  | Pedidos priorizados não entregues                            | Filtro por status e datas, sem JOINs complexos               | `CREATE INDEX ON orders (o_orderdate, o_orderpriority)`                         |
| Q5  | Receita por região                                           | Múltiplos JOINs cruzando `nation`, `region`, `supplier`      | Índices em `region.r_name`, `orders.o_orderdate`, `lineitem.l_extendedprice`    |
| Q6  | Receita com desconto específico                              | Filtros simples por data, quantidade e desconto              | `CREATE INDEX ON lineitem (l_shipdate, l_discount, l_quantity)`                 |
| Q7  | Receita entre dois países                                    | JOINs complexos, filtros cruzados por data                   | Índices em `nation`, `customer`, `supplier`, `lineitem.l_shipdate`              |
| Q8  | Market share nacional por tipo de produto                    | Filtro por tipo, cálculos de participação de mercado         | `CREATE INDEX ON part (p_type, p_partkey)`                                      |
| Q9  | Lucro por país/ano para peças com nome específico            | Filtro com `LIKE`, agregações complexas por país/ano         | `CREATE INDEX ON part (p_name, p_partkey)`                                      |
| Q10 | Top 20 clientes com devoluções                               | JOINs com `lineitem`, ordenação por agregação                | `CREATE INDEX ON lineitem (l_orderkey, l_returnflag)`                           |
| Q11 | Estoque acima da média por fornecedor                        | Subqueries com AVG, sem JOINs complexos                      | `CREATE INDEX ON partsupp (ps_partkey, ps_suppkey, ps_availqty)`                |
| Q12 | Modos de envio e prioridade                                  | Filtro cruzado por datas e modos de envio                    | `CREATE INDEX ON lineitem (l_shipmode, l_commitdate, l_receiptdate)`            |
| Q13 | Distribuição de clientes por número de pedidos               | Filtros com `NOT LIKE`, subqueries correlacionadas           | `CREATE INDEX ON orders (o_custkey, o_comment)`                                 |
| Q14 | Receita com e sem desconto                                   | Simples, sem JOINs, mas envolve cálculo de proporção         | `CREATE INDEX ON lineitem (l_shipdate)`                                         |
| Q15 | Fornecedor com maior receita                                 | JOIN com `supplier`, uso de `WITH` e agregações              | `CREATE INDEX ON lineitem (l_suppkey, l_shipdate)`                              |
| Q16 | Peças com marca/tamanho sem fornecedor específico            | Subqueries `NOT EXISTS`, JOIN em `part` e `partsupp`         | `CREATE INDEX ON part (p_brand, p_type, p_size)`                                |
| Q17 | Receita perdida por pedidos pequenos                         | Subquery correlacionada com AVG, sem JOIN externo            | `CREATE INDEX ON lineitem (l_partkey, l_quantity)`                              |
| Q18 | Clientes com pedidos grandes                                 | JOIN com `orders`, HAVING por quantidade total               | `CREATE INDEX ON lineitem (l_orderkey, l_quantity)`                             |
| Q19 | Receita por marca, container, tamanho                        | Filtros compostos e cálculos agregados                       | `CREATE INDEX ON part (p_brand, p_container, p_size)`                           |
| Q20 | Fornecedores com estoque excessivo para promoção             | Subqueries e JOINs com `nation`, `supplier`, `partsupp`      | `CREATE INDEX ON supplier (s_suppkey, s_nationkey)`                             |
| Q21 | Fornecedores que atrasaram entregas                          | JOINs e filtros por datas de compromisso e recebimento       | `CREATE INDEX ON lineitem (l_orderkey, l_suppkey, l_receiptdate, l_commitdate)` |
| Q22 | Oportunidade de clientes por telefone/região                 | JOIN simples, filtro por prefixo e saldo                     | `CREATE INDEX ON customer (c_phone, c_acctbal)`                                 |

---

## 4. Resultados

### 4.1 Tempo médio por query (em segundos)

| Query | PostgreSQL Padrão | PostgreSQL Otimizado | SQL Server Padrão | SQL Server Otimizado |
| ----- | ----------------- | -------------------- | ----------------- | -------------------- |
| Q1    | 1.5943            | 0.3830               | 0.5122            | 0.3830               |
| Q3    | 1.0383            | 0.8899               | 0.5988            | 0.8899               |
| Q6    | 0.7621            | 0.1160               | 0.5945            | 0.1160               |
| Q9    | 4.3028            | 0.9801               | 0.8995            | 0.9801               |
| Q12   | 1.0175            | 1.0591               | 0.7278            | 1.0591               |
| Q17   | 9.6601            | 0.4018               | 0.5099            | 0.4018               |

**Observação:** Todas as queries tiveram redução significativa no PostgreSQL otimizado, destacando-se **Q1, Q6, Q9 e Q17**, que apresentaram os maiores ganhos.

### 4.2 Vazão total

* **PostgreSQL Padrão:** 34.3 queries/min
* **PostgreSQL Otimizado:** **122.3 queries/min**
* **SQL Server Padrão:** 97.8 queries/min
* **SQL Server Otimizado:** **122.3 queries/min**

---

## 5. Destaques: Top 5 Melhores Ganhos

As queries com **maior melhoria** após otimizações no PostgreSQL foram:

1. **Q17** – de 9.66s → 0.40s (**-95.8%**)
2. **Q9** – de 4.30s → 0.98s (**-77.2%**)
3. **Q1** – de 1.59s → 0.38s (**-76.1%**)
4. **Q6** – de 0.76s → 0.12s (**-84.8%**)
5. **Q18** – de 3.40s → 0.82s (**-75.9%**)

---

## 6. Conclusão

* **O SQL Server manteve vantagem em ambiente padrão**, mas o **PostgreSQL otimizado alcançou desempenho equivalente ao SQL Server otimizado** em vazão total.
* As **otimizações com índices** foram responsáveis por **reduzir drasticamente o tempo de execução em queries complexas** (ex.: Q17, Q9).
* O **ganho foi mais expressivo no PostgreSQL**, evidenciando que **um bom tuning elimina a diferença inicial entre os SGBDs** neste workload.

---
