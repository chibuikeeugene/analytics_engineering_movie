with film_actor as (
    select actor_id, film_id
    from {{ ref('stg_film_actor') }}
),
actor as (
    select actor_id, first_name, last_name
    from {{ ref('stg_actor') }}
),
film as (
    select film_id, title, description, release_year, language_id, rental_duration, rental_rate, replacement_cost, rating, last_update,
    special_features, fulltext
    from {{ ref('stg_film') }}
),
films_featured_by_actors as (
    select fa.actor_id, group_concat( distinct a.first_name) as first_name , group_concat(distinct a.last_name) as last_name, 
    group_concat(f.title order by title asc) as movies_featured_in
    from film_actor as fa left join actor as a on a.actor_id = fa.actor_id
    inner join film as f on f.film_id =  fa.film_id
    group by fa.actor_id
    order by first_name
)
select * from films_featured_by_actors