drop table if exists sales;

create table if not exists sales (
    username string,
    id int
)
row format delimited
    fields terminated by ',';

load data local inpath '/Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/input/hive/sales_data'
into table sales;

drop table if exists things;

create table if not exists things (
    id int,
    name string
)
row format delimited
    fields terminated by ',';

load data local inpath '/Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/input/hive/things_data'
into table things;

select * from sales;

select * from things;

select sales.*, things.*
from sales join things
on sales.id = things.id;

explain
select sales.*, things.*
from sales join things
on sales.id = things.id;
