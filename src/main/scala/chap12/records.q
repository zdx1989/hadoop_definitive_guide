create table records (
    year string,
    temperature int)
row format delimited
    fields terminated by '\t';

load data local inpath '/Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/input/ncdc/micro/simple.txt'
overwrite into table records;