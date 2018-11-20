drop table if exists managed_table;

create table if not exists managed_table (dummy string)
row format delimited
fields terminated by '\t';

load data inpath '/user/zhoudunxiong/data.txt' into table managed_table;
