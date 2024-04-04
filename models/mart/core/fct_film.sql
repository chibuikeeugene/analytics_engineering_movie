with film as (
    select film_id, title, description, release_year, language_id, rental_duration, rental_rate, replacement_cost, rating, last_update,
    special_features, fulltext
    from {{ ref('stg_film') }}
),
film_category as (
    select film_id, category_id
    from {{ ref('stg_film_category') }}
),
category as (
    select category_id, name
    from {{ ref('stg_category') }}
),
language as (
    select language_id, name
    from {{ ref('stg_language') }}
),
final_film_attributes as (
    select f.film_id, f.title, c.name as genre, f.release_year, l.name as language, f.rental_duration, f.rental_rate, f.rating, f.replacement_cost, f.last_update
    from film  as f left join film_category as fc on fc.film_id = f.film_id
    inner join  category as c on c.category_id = fc.category_id
    inner join language as l on l.language_id =  f.language_id
    group by f.film_id, f.title, c.name, f.release_year, l.name, f.rental_duration, f.rental_rate, f.rating, f.replacement_cost, f.last_update
    order by f.title
)

select * from final_film_attributes