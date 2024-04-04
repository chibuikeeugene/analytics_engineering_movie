select * 
from {{ source('sakila', 'category') }}