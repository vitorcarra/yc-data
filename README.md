# yc-data

## Task #1

1.

```sql
with base as (
  SELECT
    state_bottle_retail,
    sale_dollars,
    ROUND(sale_dollars, 2) as revenue,
    bottles_sold,
    format_date('(%Y)-Q%Q', date(date)) as quarter
  FROM `bigquery-public-data.iowa_liquor_sales.sales`
),
totals as (
    select
    sum(bottles_sold) as total_products_sold,
    ROUND(sum(revenue),2) as total_revenue,
    quarter
  from base
  group by quarter
)
select
  *,
  case
    when total_revenue > (avg(total_revenue) over (partition by null)) * 1.1 then TRUE
    else FALSE
  end as ten_percent_above_avg
from totals
order by quarter
```

2.

```sql
select
  distinct county
from `bigquery-public-data.iowa_liquor_sales.sales`
where sale_dollars > 100000;
```

3.

```sql
with base as (
  SELECT
    store_name,
    max(sale_dollars) as max_revenue,
    min(sale_dollars) as min_revenue
  FROM `bigquery-public-data.iowa_liquor_sales.sales`
  group by store_name
),
top_10_biggest_revenue as (
  select
    store_name,
    ROUND(max_revenue, 2) as revenue,
    'biggest_revenue' as rank_type,
    rank() over (order by max_revenue desc) as position
  from base
  qualify position <= 10
),
top_10_lowest_revenue as (
  select
    store_name,
    ROUND(min_revenue, 2) as revenue,
    'lowest_revenue' as rank_type,
    rank() over (order by min_revenue asc) as position
  from base
  qualify position <= 10
),
final as (
  select
    * except(position)
  from top_10_biggest_revenue
  union all
  select
    * except(position)
  from top_10_lowest_revenue
)
select *
from final
order by rank_type, revenue desc
```
