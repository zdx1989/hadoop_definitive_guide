drop table if exists arrays;

create table if not exists arrays (
    username string,
    hobby Array<string>
)
row format delimited
    fields terminated by '\t'
    collection items terminated by ',';

load data local inpath '/Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/input/hive/arrays_data'
into table arrays;

select explode(hobby) from arrays;