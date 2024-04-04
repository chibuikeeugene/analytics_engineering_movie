select *
from {{ source('sakila', 'actor') }}