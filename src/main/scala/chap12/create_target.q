drop table if exists target_new;

create table if not exists target_new
as
select c1, c2, dt
from sources;