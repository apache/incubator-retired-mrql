Hive:
======

CREATE TABLE customer (CUSTKEY int, NAME string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE orders (ORDERKEY string, CUSTKEY int, ORDERSTATUS string, TOTALPRICE float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/share/apps/hadoop/tpch/customer5.tbl'
OVERWRITE INTO TABLE customer;

LOAD DATA LOCAL INPATH '/share/apps/hadoop/tpch/orders5.tbl'
OVERWRITE INTO TABLE orders;

insert overwrite directory 'qqq' select CUSTKEY, sum(TOTALPRICE) from orders group by CUSTKEY;

insert overwrite directory 'qqq' select CUSTKEY, sum(TOTALPRICE) from orders group by CUSTKEY having sum(TOTALPRICE) > 10000;

insert overwrite directory 'qqq' select CUSTKEY, TOTALPRICE from orders order by TOTALPRICE;

insert overwrite directory 'qqq' select c.NAME, sum(o.TOTALPRICE) from Orders o join Customer c on (o.CUSTKEY=c.CUSTKEY) group by c.NAME;

insert overwrite directory 'qqq' select c.CUSTKEY, sum(o.TOTALPRICE) from Orders o join Customer c on (o.CUSTKEY=c.CUSTKEY) group by c.CUSTKEY;

insert overwrite directory 'qqq' select c.NAME, o.TOTALPRICE from Orders o join Customer c on (o.CUSTKEY=c.CUSTKEY);

select count(*) from orders;

select avg(TOTALPRICE) from orders;

insert overwrite directory 'qqq' select distinct o.CUSTKEY from Orders o;

insert overwrite directory 'qqq' select c.NAME, avg(o.TOTALPRICE) from Orders o join Customer c on (o.CUSTKEY=c.CUSTKEY) group by c.CUSTKEY, c.NAME;

MRQL:
=======

C = source(line,"/user/hadoop/customer5.tbl","|",type(<CUSTKEY:int,NAME:string>));
O = source(line,"/user/hadoop/orders5.tbl","|",type(<ORDERKEY:string,CUSTKEY:int,ORDERSTATUS:any,TOTALPRICE:float>));

C = source(line,"/user/fegaras/customer.tbl","|",type(<CUSTKEY:int,NAME:string>));
O = source(line,"/user/fegaras/orders.tbl","|",type(<ORDERKEY:string,CUSTKEY:int,ORDERSTATUS:any,TOTALPRICE:float>));

select (k,sum(o.TOTALPRICE)) from o in O group by k: o.CUSTKEY;

select (k,sum(o.TOTALPRICE)) from o in O group by k: o.CUSTKEY having sum(o.TOTALPRICE) > 10000;

select (o.CUSTKEY,o.TOTALPRICE) from o in O order by o.TOTALPRICE;

select (k,sum(o.TOTALPRICE)) from o in O, c in C where o.CUSTKEY=c.CUSTKEY group by k: c.NAME;

select (k,sum(o.TOTALPRICE)) from o in O, c in C where o.CUSTKEY=c.CUSTKEY group by k: c.CUSTKEY;

select (c.NAME,o.TOTALPRICE) from o in O, c in C where o.CUSTKEY=c.CUSTKEY;

count(O);

avg(select o.TOTALPRICE from o in O);

select distinct o.CUSTKEY from o in O;

select (k#1,avg(o.TOTALPRICE)) from o in O, c in C where o.CUSTKEY=c.CUSTKEY group by k: (c.CUSTKEY,c.NAME);
select (c.NAME,avg(select o.TOTALPRICE from o in O where o.CUSTKEY=c.CUSTKEY)) from c in C;
