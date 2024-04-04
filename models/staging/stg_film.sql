select * 
from {{ source('sakila', 'film') }}