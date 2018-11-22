create view st
as
select sales.username, things.name
from sales join things on sales.id = things.id;

show tables;

describe extended st;

select username, name, count(*)
from st
group by username, name;