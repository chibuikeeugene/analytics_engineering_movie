with inventory as (
    select inventory_id, film_id, last_update
    from {{ ref('stg_inventory') }}
),
film as (
    select film_id, title, release_year, rental_duration, rental_rate, length, replacement_cost, rating, special_features
    from {{ ref('stg_film') }}
),
film_inventory as (
    select group_concat(distinct cast(inventory_id as char)) as inventory_ids,
    Coalesce((length(group_concat(distinct cast(inventory_id as char))) - length(replace(group_concat(distinct cast(inventory_id as char)), ',', '')) + 1), 0) as no_of_inventory_ids, 
    title, release_year, rental_duration, rental_rate, length, replacement_cost, rating, special_features, last_update
    from inventory as i right join film  as f on f.film_id = i.film_id
    group by  title, release_year, rental_duration, rental_rate, length, replacement_cost, rating, special_features, last_update

)
select * from film_inventory