## MiniOB

本仓库主要实现了2021 OceanBase 数据库大赛中[赛题](https://github.com/OceanBase-Partner/lectures-on-dbms-implementation/blob/main/miniob-primary.md)需求的必做题项目

| 名称 | 描述 | 测试用例示例 |
| ---- | ---- | -------------|
| 优化buffer pool | 必做。实现LRU淘汰算法或其它淘汰算法 | |
| drop table | 必做。删除表。清除表相关的资源。 |create table t(id int, age int);<br/>create table t(id int, name char);<br/>drop table t;<br/>create table t(id int, name char);|
| 实现update功能 | 必做。update单个字段即可。 |update t set age =100 where id=2;<br/>update set age=20 where id>100;|
| 增加date字段 | 必做。date测试不会超过2038年2月。注意处理非法的date输入。 |create table t(id int, birthday date);<br/>insert into t values(1, '2020-09-10');<br/>insert into t values(2, '2021-1-2');<br/>select * from t;|
| 查询元数据校验 | 必做。查询语句中存在不存在的列名、表名等，需要返回失败。需要检查代码，判断是否需要返回错误的地方都返回错误了。 |create table t(id int, age int);<br/>select * from t where name='a'; <br/>select address from t where id=1;<br/>select * from t_1000;|
| 多表查询 | 必做。支持多张表的笛卡尔积关联查询。需要实现select * from t1,t2; select t1.*,t2.* from t1,t2;以及select t1.id,t2.id from t1,t2;查询可能会带条件。查询结果展示格式参考单表查询。每一列必须带有表信息，比如:<br/>t1.id \|  t2.id <br/>1 \| 1 |select * from t1,t2;<br/>select * from t1,t2 where t1.id=t2.id and t1.age > 10;<br/>select * from t1,t2,t3;|
| 聚合运算 | 需要实现max/min/count/avg.<br/>包含聚合字段时，只会出现聚合字段。聚合函数中的参数不会是表达式，比如age +1 |select max(age) from t1; select count(*) from t1; select count(1) from t1; select count(id) from t1;|

## Build & Run

Based on Ubuntu 20.04.2

```bash
sudo apt install libevent-dev
sudo apt install googletest
sudo apt install libjsoncpp-dev
mkdir build
cd build
cmake ..
make -j4
```