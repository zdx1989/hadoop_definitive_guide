drop table if exists records;

create table if not exists records (
    year string,
    temperature int
)
row format delimited
    fields terminated by ',';

load data local inpath '/Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/input/hive/records_data'
into table records;

from records
select year, temperature
distribute by year
sort by year asc, temperature desc;