With first_order_data as (
  select 
     purchasing_household_id 
     ,order_id 
     ,order_merchant_ids as merchant_id
     ,ORDER_MERCHANT_NAMES as merchant_name
     ,case when total_orders = 1 then false else true end as customer_retained 
  from 
    PROD_MART_GOLDBELLY_DB.ANALYTICS_MART.ORDERS_PURCHASING_HOUSEHOLDS_FLAGS
  where
    order_rank_ = 1  
    and cohort_year >= 2022
    -- and cohort_month in (11,12)
    and order_merchant_ids not like '%,%'
    and order_is_gift = 'self'
    -- and merchant_id::int not in (26945, 24586) 
),
merchant_agg as (
  select 
    merchant_id 
    ,merchant_name
    ,1 as join_on_me 
    ,count(distinct PURCHASING_HOUSEHOLD_ID) as total_first_time_purchasers 
    ,count(distinct case when customer_retained then PURCHASING_HOUSEHOLD_ID end) as total_retained
    ,total_retained/total_first_time_purchasers as pct_retained
   from first_order_data 
   group by 1,2,3
   having 
     total_first_time_purchasers > 100
),
max_counts as (
  select 
    1 as join_on_me 
    ,max(total_first_time_purchasers) as max_purchasers 
    ,max(pct_retained) as max_retained  
  from 
    merchant_agg
  group by 1
),
join_max_counts_to_merchant_level_counts as (
  select 
    merchant_id
    ,merchant_name
    ,total_first_time_purchasers
    ,total_first_time_purchasers/max_purchasers as volume_factor
    ,total_retained
    ,pct_retained
    ,pct_retained/max_retained as retention_factor
    ,round(((volume_factor
              +volume_factor
              +retention_factor
              +retention_factor
              +retention_factor)/5),2)*100 as crs  
  from 
    merchant_agg 
  join
    max_counts on merchant_agg.join_on_me = max_counts.join_on_me
)


select * from join_max_counts_to_merchant_level_counts
