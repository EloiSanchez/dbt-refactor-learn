WITH 

-- import CTEs

paid_orders as (
    select * from {{ ref('paid_orders') }}
),

customer_orders as (
    select * from {{ ref('customer_orders') }}
),

clv_bad as (
    select * from {{ ref('clv_bad') }}
),

fct_customer_orders as (
    select
        paid_orders.*,
        ROW_NUMBER() OVER (ORDER BY paid_orders.order_id)                                       as transaction_seq,
        ROW_NUMBER() OVER (PARTITION BY paid_orders.customer_id ORDER BY paid_orders.order_id)  as customer_sales_seq,
        CASE 
            WHEN customer_orders.first_order_date = paid_orders.order_placed_at THEN 'new'
            ELSE 'return' 
        END                                                                                     as nvsr,
        clv_bad.clv_bad                                                                         as customer_lifetime_value,
        customer_orders.first_order_date                                                        as fdos
    FROM paid_orders
    left join customer_orders
        on customer_orders.customer_id = paid_orders.customer_id
    left outer join clv_bad 
        on clv_bad.order_id = paid_orders.order_id
    ORDER BY order_id
)

select * from fct_customer_orders