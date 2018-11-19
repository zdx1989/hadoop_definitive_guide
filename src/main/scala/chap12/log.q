create table log (
    ts bigint,
    line string
)
row format delimited
fields terminated by '\t'
partitioned by (dt string, country string);

load data local inpath 'file1' into table log
partition (dt = '2018-11-12', country = 'GB');