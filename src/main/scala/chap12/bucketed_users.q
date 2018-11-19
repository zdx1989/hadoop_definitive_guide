drop table if exists bucketed_users;
create table if not exists bucketed_users (
    id int,
    name string
)
clustered by (id) into 4 buckets
row format delimited
fields terminated by '\t';