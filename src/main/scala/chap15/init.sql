create database hadoopguide;
grant all privileges on hadoopguide.* to '%'@'localhost';
grant all privileges on hadoopguide.* to ''@'localhost';

create table widgets(
    id int not null primary key auto_increment,
    widget_name varchar(64) not null,
    price decimal(10, 2),
    design_date date,
    version int,
    design_comment varchar(100)
);

insert into widgets values (null, 'sprocket', 0.25, '2010-02-10', 1, 'Connects two gizmos');
insert into widgets values (null, 'gizmo', 4.00, '2009-10-30', 4, null);
insert into widgets values (null, 'gadget', 99.99, '1989-08-13', 13, 'Our flagship product');