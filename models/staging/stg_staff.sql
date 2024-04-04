select *
from {{ source('sakila', 'staff') }}