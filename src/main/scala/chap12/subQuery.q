select username, name, count(*)
from (
    select sales.username, things.name
    from sales join things on sales.id = things.id
) st
group by username, name;