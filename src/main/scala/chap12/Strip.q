add jar /Users/zhoudunxiong/Code/Hadoop_Definitive_Guide/jar/hadoop_definitive_guide-assembly-1.0.jar;

create temporary function strip as 'chap12.Strip';

select strip(' zdx ') from sales;

select strip('zdxxdz', 'z') from sales;