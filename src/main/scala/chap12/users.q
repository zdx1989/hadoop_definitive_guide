drop table if exists users;
create table if not exists users (
    id int,
    name string
)
row format delimited
fields terminated by '\t';

load data local inpath '/Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/input/hive/user_data'
into table users;