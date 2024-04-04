with customer as (
    select customer_id, store_id, first_name, last_name, address_id,create_date, last_update, active
    from {{ ref('stg_customer') }}
), 
address as (
    select address_id, address, city_id, last_update
    from {{ ref('stg_address') }}
),
city as (
    select city_id, city, country_id
    from {{ ref('stg_city') }}
),
country as (
    select country_id, country, last_update
    from {{ ref('stg_country') }}
),
cust_address as (
    select cm.customer_id, cm.first_name, cm.last_name, a.address, c.city, co.country, cm.last_update
    from customer as cm left join address as a on cm.address_id = a.address_id
    inner join city as c on c.city_id = a.city_id
    inner join country as co on co.country_id = c.country_id
    group by customer_id, first_name, last_name, address, city, country, cm.last_update
    order by first_name
)

select * from cust_address
