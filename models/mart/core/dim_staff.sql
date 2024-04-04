with staff as (
    select staff_id, first_name, last_name, address_id, store_id, last_update
    from {{ ref('stg_staff') }}
),
address as (
    select address_id, address
    from {{ ref('stg_address') }}
),
store as (
    select store_id, manager_staff_id
    from {{ ref('stg_store') }}
),
final_staff as (
    select staff_id, first_name, last_name, address, manager_staff_id
    from staff as s left join address as a on s.address_id = a.address_id
    inner join store as st on st.store_id = s.store_id
    group by staff_id, first_name, last_name, address, manager_staff_id
    order by first_name
)
select * from final_staff