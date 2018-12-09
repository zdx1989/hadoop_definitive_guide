drop table if exists sales;
create table if not exists sales (
    widget_id int,
    qty int,
    street string,
    city string,
    state string,
    zip int,
    sale_date string)
row format delimited
    fields terminated by ',';

load data local inpath '/Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/src/main/scala/chap15/sales.q'
into table sales;