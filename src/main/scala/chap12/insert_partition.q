drop table if exists sources;

create table if not exists sources (
    c1 string,
    c2 int,
    dt string
)
row format delimited
    fields terminated by '\t';

load data local inpath '/Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/input/hive/sources_data'
into table sources;

drop table if exists target;

create table if not exists target (
    name string,
    age int
)
partitioned by (dt string)
row format delimited
    fields terminated by '\t';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table target
partition (dt)
select c1, c2, dt
from sources;