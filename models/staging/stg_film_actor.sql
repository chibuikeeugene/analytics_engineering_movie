select * 
from {{ source('sakila', 'film_actor') }}