with

customers as (
    select * from {{ ref('customers') }}
),

payments as (
    select * from {{ ref('payments') }}
),

orders as (
    select * from {{ ref('orders') }}
),

paid_orders as (
    select 
        orders.id                                    as order_id,
        orders.user_id	                             as customer_id,
        orders.order_date                            as order_placed_at,
        orders.status                                as order_status,
        successful_payments.total_amount_paid,
        successful_payments.payment_finalized_date,
        customers.first_name                          as customer_first_name,
        customers.last_name                           as customer_last_name
    FROM orders
    left join successful_payments ON orders.id = successful_payments.order_id
    left join customers on orders.user_id = customers.id 
)

select * from paid_orders