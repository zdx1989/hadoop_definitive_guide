--partitioned by 紧跟着字段的定义，放在格式定义后面会报错
create table log (
    ts bigint,
    line string
)
partitioned by(dt string, country string)
row format delimited
fields terminated by '\t';

load data local inpath '/Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/input/hive/file1' into table log
partition (dt = '2018-11-12', country = 'GB');

load data local inpath '/Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/input/hive/file2' into table log
partition (dt = '2018-11-12', country = 'US');

load data local inpath '/Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/input/hive/file3' into table log
partition (dt = '2018-11-13', country = 'GB');