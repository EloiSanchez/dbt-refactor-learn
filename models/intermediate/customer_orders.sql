WITH 

customers as (
    select * from {{ ref('customers') }}
),

orders as (
    select * from {{ ref('orders') }}
),

customer_orders as (
    select 
        customers.id                as customer_id, 
        min(order_date)             as first_order_date, 
        max(order_date)             as most_recent_order_date, 
        count(orders.id)            as number_of_orders
    from customers 
    left join orders
        on orders.user_id = customers.id 
    group by 1
)

select * from customer_orders