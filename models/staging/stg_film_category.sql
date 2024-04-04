select * 
from {{ source('sakila', 'film_category') }}