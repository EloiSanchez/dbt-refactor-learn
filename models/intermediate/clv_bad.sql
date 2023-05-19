with 

paid_orders as (
    select * from {{ ref('paid_orders') }}
),

clv_bad as (
    select
        paid_orders.order_id,
        sum(paid_orders_2.total_amount_paid) as clv_bad
    from paid_orders
    left join paid_orders paid_orders_2 
        on 1=1
            and paid_orders.customer_id = paid_orders_2.customer_id 
            and paid_orders.order_id >= paid_orders_2.order_id
    group by 1
)

select * from clv_bad