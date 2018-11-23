add jar /Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/jar/hadoop_definitive_guide-assembly-1.0.jar

create temporary function maximum as 'chap12.Maximum';

select maximum(temperature) from records;

create temporary function jmaximum as 'chap12.JMaximum';

select jmaximum(temperature) from records;