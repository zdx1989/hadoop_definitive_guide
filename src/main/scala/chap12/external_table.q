drop table if exists external_table;
create external table external_table (dummy string)
row format delimited
fields terminated by '\t'
location '/user/zhoudunxiong/external_table';

load data inpath '/user/zhoudunxiong/data.txt' into table external_table;