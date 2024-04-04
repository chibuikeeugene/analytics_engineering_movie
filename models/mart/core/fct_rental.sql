with rental as (
    select rental_id, rental_date, return_date, inventory_id, customer_id, staff_id, last_update
    from {{ ref('stg_rental') }}
),
customer as (
    select customer_id, first_name, last_name, city, country
    from {{ ref('dim_customer') }}
),
staff as (
    select staff_id, first_name, last_name
    from {{ ref('dim_staff') }}
),
inventory as (
    select inventory_id, film_id
    from {{ ref('stg_inventory') }}
),
final_rental as (
    select rental_id, rental_date, return_date, i.film_id, c.first_name, c.last_name, city, country, concat(s.first_name,' ', s.last_name) as staff_name, last_update
    from inventory as i left join rental as r on r.inventory_id =  i.inventory_id
    inner join customer as c on c.customer_id =  r.customer_id
    inner join staff as s on s.staff_id =  r.staff_id
    group by rental_id, rental_date, return_date, i.film_id, c.first_name, c.last_name, city, country,staff_name, last_update
    order by rental_id
)
select * from final_rental