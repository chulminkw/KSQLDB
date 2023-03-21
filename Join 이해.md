# Join 이해

### Data 준비

- simple_user_stream Stream을 재생성.

```sql
drop table simple_user_table;

drop stream simple_user_stream delete topic;

create stream simple_user_stream 
(
	user_id integer key,
	name varchar,
	email varchar
) with (
  KAFKA_TOPIC = 'simple_user_topic',
  KEY_FORMAT = 'KAFKA', 
	VALUE_FORMAT ='JSON',
  PARTITIONS = 3
);

insert into simple_user_stream(user_id, name, email) values (1, 'John', 'test_email_01@test.domain');
insert into simple_user_stream(user_id, name, email) values (2, 'Merry', 'test_email_02@test.domain');
insert into simple_user_stream(user_id, name, email) values (3, 'Elli', 'test_email_03@test.domain');
insert into simple_user_stream(user_id, name, email) values (4, 'Mike', 'test_email_04@test.domain');
insert into simple_user_stream(user_id, name, email) values (5, 'Tom', 'test_email_05@test.domain');
insert into simple_user_stream(user_id, name, email) values (5, 'Tommy', 'test_email_05@test.domain');
insert into simple_user_stream(user_id, name, email) values (6, 'Michell', 'test_email_06@test.domain');
select * from simple_user_stream;
```

- simple_user_table Table을 기존에 생성된 topic을 기반으로 생성.

```sql
drop table if exists simple_user_table;

create table simple_user_table
(
	user_id integer primary key,
	name varchar,
	email varchar
) with (
  KAFKA_TOPIC = 'simple_user_topic',
  KEY_FORMAT = 'KAFKA', 
	VALUE_FORMAT ='JSON'
);

select * from simple_user_table emit changes;

```

- user_id를 key로 가지는 user_activity_stream 생성.  user_activity_stream은 하나의 user_id로 여러개의 레코드를 가짐(m 레벨)

```sql
drop stream if exists user_activity_stream delete topic;

CREATE STREAM user_activity_stream (
    USER_ID INTEGER KEY,
    ACTIVITY_ID INTEGER,
    ACTIVITY_TYPE VARCHAR,
    ACTIVITY_POINT DOUBLE
   ) WITH (
    KAFKA_TOPIC = 'user_activity_stream_topic',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
);

INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (1, 1,'branch_visit',0.4);
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (2, 2,'deposit',0.56);
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (3, 3,'web_open',0.33);
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (1, 4,'deposit',0.41);
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (2, 5,'deposit',0.44);
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (3, 6,'web_open',0.33);
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (1, 7,'mobile_open', 0.97);
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (2, 8,'deposit',0.83);
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (4, 1,'mobile_open', 0.45);
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (4, 2,'deposit', 0.34);
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (5, 1,'web_open',0.66);
```

### join 실습- stream-stream 조인

[https://docs.ksqldb.io/en/latest/developer-guide/joins/join-streams-and-tables/](https://docs.ksqldb.io/en/latest/developer-guide/joins/join-streams-and-tables/)

- KSQL의 조인은 여러 제약 조건이 있음.
    1. 조인키에 대한 제약. 조인 키는 같은 데이터 타입이어야 하며 다른 데이터 타입일 경우 CAST()함수를 이용하여 데이터 타입을 변환
    2. 조인 키는 Stream의 경우 Key로, Table의 경우 Primary Key로 지정되어야 함. 
    3. 조인 키의 파티션 갯수는 서로 같아야 함. 
    4. 조인키의 파티션별 값 분포는 서로 동일하게 분포되어 야 함. 
    5. Stream과 Stream 조인은 within 절로 window 시간을 정해줘야 함. 전체 stream에 대한 조인이 되지 않음. 
    6. Stream과 Stream, Stream과 table 조인은 pull 쿼리가 지원되지 않으며 Push 쿼리가 지원되지 않음. 즉 조인을 하게 되면 이는 rocksdb의 도움이 필요함.  단 Stream과 Stream 조인을 CSAS로 Mview 생성 후에는 select * from mview로 pull 쿼리도 가능. 
- simple_user_stream과 user_activity_stream의 조인. Stream과 Stream의 조인.  조인 쿼리는 pull 쿼리를 지원하지 않음. push 쿼리로 수행 필요.  즉 조인을 하게 되면 이는 rocksdb의 도움이 필요함.
- stream과 stream의 조인은 m대m 조인이 될 수 있으며, 이 경우 기존 데이터 레벨에 중복된 결과도 추출될 수 있음.

```sql
-- 아래는 pull 쿼리로 수행되지 않음. 
select a.*, b.*
from simple_user_stream a 
inner join user_activity_stream b on a.id= b.user_id;

-- 아래는 push 쿼리지만 within 절이 없어서 오류
select a.*, b.*
from simple_user_stream a 
inner join user_activity_stream b on a.id= b.user_id emit changes;

select a.id, a.name, b.*
from simple_user_stream a 
inner join user_activity_stream b within 1 hours on a.id= b.user_id emit changes;

```

### join 실습- stream-table 조인

**Semantics of Stream-Table Joins 부분 참조 할것.** 

- stream-table 조인은 순서가 중요함. 조인절에 먼저 stream이 오고 다음에 table이 와야함. table→ stream 조인은 수행되지 않고 stream→table 조인은 수행됨.
- left side의 stream에 input record가 join을 trigger함. right side의 table에 들어오는 input record는 내부 조인 state만 update함.
- Input records for the table with a NULL value are interpreted as *tombstones*
 for the corresponding key, which indicate the deletion of the key from the table. Tombstones don't trigger the join.
- stream과 table 조인은 within 절을 적용해서는 안됨.

```sql
select b.id, b.name, a.* 
from user_activity_stream a
inner join simple_user_table b on a.user_id= b.id emit changes;

select b.id, b.name, a.* 
from user_activity_stream a
join simple_user_table b on a.user_id= b.id emit changes;

-- 아래는 table을 기준으로 stream을 조인하므로 수행되지 않음. 
select a.id, a.name, b.*
from simple_user_table a
inner join user_activity_stream b on a.id= b.user_id emit changes;

-- 아래는 stream-table 조인 시 within 절을 적용하면 수행되지 않음.  
select b.id, b.name, a.* 
from user_activity_stream a
inner join simple_user_table b within 2 hours on a.user_id= b.id emit changes;
```

- 조인 시 a.* 과 같이 alias와 *를 같이 사용하게 되면 컬럼명을 alias_기존컬럼명으로 만들기 때문에 주의가 필요함. 즉 A_USER_ID, A_ACTIVITY_ID와 같이 만들어짐.

```sql
select b.id, b.name, a.* 
from user_activity_stream a
inner join simple_user_table b on a.user_id= b.id emit changes;
```

- 조인 결과를 MView로 생성.  Stream→table 조인은 Stream으로 생성되어야 하며, Table로 생성할 수 없음.  with 절에 topic 속성값을 주지 않으면 자동으로 join key값을 key로, topic 명은 Mview명을 가져옴.  partition 갯수는 조인 시 partition 갯수가 같아야 하므로 그대로 적용가능, **만약 replication factor가 서로 다르면 어떻게 적용되는가?**

```sql
drop stream if exists user_activity_join_mv delete topic;

create stream user_activity_join_mv
as
select b.id, b.name, a.user_id, a.activity_id, a.activity_type, a.activity_point
from user_activity_stream a
inner join simple_user_table b on a.user_id= b.id emit changes;

-- MView에 pull 쿼리, push 쿼리 모두 가능. 
select * from user_activity_join_mv;
```

### Join 대상 Stream과 Table에 신규 데이터 입력 시 조인 결과 Mview의 push 쿼리 실시간 반영.

- stream에 신규 데이터가 입력 시 table에 해당 key로 존재하는 경우라면 조인 결과 Mview를 실시간으로 push query시 이를 반영함.
- stream에 신규 데이터가 입력 되더라도 table에 해당 key가 존재하지 않으면 inner join일 경우 mview에서 실시간 push query시 이를 반영하지 않음. 단 stream→ table left outer 조인일 경우 stream 데이터만 실시간으로 push 쿼리에서 반영됨.
- table에 신규 데이터가 기존에 primary key로 존재하는 데이터여서 table update가 발생하는 경우 Mview에 emit cchanges 로 push query로 실시간으로 반영되지는 않음.  Mview에 다시 push query를 적용해야 반영됨.
- table에 신규 데이터가 기존의 primary key로 존재하지 않는 신규 데이터여서 table insert가 발생할 경우 조인시 stream 조인키에도 없다면 이를 반영하지 않음.  만약 stream 에 새로운 데이터가 추가되고 table에 신규 데이터와 조인이 되는 키값이라면 mview emit changes의 push 쿼리로 바로 반영됨.
- 
- stream-join 시 stream과 table에 신규 데이터 발생시 조인 결과  확인.  아래 쿼리 수행

```sql
select * from user_activity_join_mv emit changes;
```

- 다른 창에서 ksql cli 실행 후 아래와 같이 stream에 신규 데이터 입력후 조인 결과 mview 확인.

```sql
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (5, 2,'deposit',0.78);
```

- ksql cli에서  아래와 같이 table에 신규 데이터 입력후 조인 결과 mview 확인.   table의 데이터가 변경되어도 조인에 반영되지 않음(만약 반영되면 emit changes에 레코드를 추가로 해야 되므로 맞는 설정으로 보임)

```sql
insert into simple_user_table(id, name, email) values (5, 'Tomminn', 'test_email_05@test.domain');
```

- 아래와 같이 새로운 id 6인 데이터를 추가하면 inner join이 되지 않으므로 당연히 조인 결과에 반영되지 않음.

```sql
insert into simple_user_table(id, name, email) values (6, 'Brandon', 'test_email_06@test.domain');
```

- 새로운 user_id 6으로 user_activity_stream 데이터를 입력 한 후 조인 결과 확인.  조인 결과에 반영됨.

```sql
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (6, 1,'branch_visit',0.3);
```

- 아래는 stream에 먼저 신규 데이터를 입력 후 table에 신규 데이터(기존 update아님) 입력 하여 mview push 쿼리 실시간 반영 사항 확인.  join을 실시간 trigger하는 것은 stream임. 먼저 stream에 데이터가 입력되면서 조인이 실시간 trigger가 되었으나 inner join으로 조인이 되지 않아 실시간 mview push 쿼리에 반영되지 않음.  이후 테이블에 신규 데이터가 입력되어도 mview push 쿼리에 실시간 반영되지 않음.  **조인 자체에 반영되지 않음. 왜 그럴까?**

```sql
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (7, 1,'branch_visit', 0.42);
insert into simple_user_table(id, name, email) values (7, 'Michael', 'test_email_07@test.domain');

```

### join 실습- table-table 조인.

- table-table 조인도 기존 stream-table 조인과 큰 차이는 없음(다만 1:1 관계의 조인, 그리고 1:m 관계의 조인에 대해서는 생각해볼 사항들이 있음)

```sql
--ctas mview로 새로운 table 생성. 
create table user_activity_mv01_tab
as
select user_id, count(*) as user_cnt, sum(activity_point) as sum_point
from user_activity_stream
group by user_id;

-- table-table 조인 수행. push 쿼리만 가능. 
select a.id, a.name, b.user_id, b.user_cnt, b.sum_point
from simple_user_table a
    join user_activity_mv01_tab b on a.id = b.user_id emit changes;
```

- table - table 조인 시 1: 1이 아닌 1:m 조인 수행.  stream - table과 마찬가지로 반드시 m이 되는 테이블이 조인 선두, 1이 되는 집합이 조인 후미에 와야 함.

```sql
--ctas mview로 새로운 table 생성. 
-- 여러개의 컬럼이 Primary Key가 될 경우 KEY_FORMAT은 KAFKA-FORMAT 과 같은 primitive 타입이 될 수가 없으며 JSON Format등으로 설정해야함. 
create table user_activity_mv02_tab
with
(
-- 아래는 FORMAT='JSON'으로 대체 될 수 있음. 
KEY_FORMAT='JSON',
VALUE_FORMAT='JSON'
)
as
select user_id, activity_type, count(*) as user_cnt, sum(activity_point) as sum_point
from user_activity_stream
group by user_id, activity_type;

-- 아래는 1레벨의 집합이 조인 선두에 오므로 수행 실패
select a.id, a.name, b.user_id, b.user_cnt, b.sum_point
from simple_user_table a
    join user_activity_mv02_tab b on a.id = b.user_id emit changes;

--아래는 m레벨의 집합이 조인 선두에 오므로 수행 가능. 
select b.id, b.name, a.user_id, a.user_cnt, a.sum_point
from user_activity_mv02_tab  a
    join simple_user_table b on a.user_id = b.id emit changes;
```

### 조인시 Co-partitioning 제약 조건

- 조인되는 두개의 Stream/Table은 동일한 파티션 개수, 동일한 조인(파티션) key 타입, 동일한 파티션 key 분배를 가져야만 정상적인 조인이 가능.
- 아래와 같이 파티션 개수가 1개인 simple_user_stream_test을 생성.

```sql
drop stream simple_user_stream_test delete topic;

create stream simple_user_stream_test
(
	user_id integer key,
	name varchar,
	email varchar
) with (
  KAFKA_TOPIC = 'simple_user_test_topic',
  KEY_FORMAT = 'KAFKA', 
  VALUE_FORMAT ='JSON',
  PARTITIONS = 1
);

insert into simple_user_stream_test(user_id, name, email) values (1, 'John', 'test_email_01@test.domain');
insert into simple_user_stream_test(user_id, name, email) values (2, 'Merry', 'test_email_02@test.domain');
insert into simple_user_stream_test(user_id, name, email) values (3, 'Elli', 'test_email_03@test.domain');
insert into simple_user_stream_test(user_id, name, email) values (4, 'Mike', 'test_email_04@test.domain');
--이름을 Tom 그리고 Tommy로 변경시 데이터 추가 입력. 
insert into simple_user_stream_test(user_id, name, email) values (5, 'Tom', 'test_email_05@test.domain');
insert into simple_user_stream_test(user_id, name, email) values (5, 'Tommy', 'test_email_05@test.domain');

select * from simple_user_stream_test;
```

- simple_user_stream_test과 user_activity_stream을 조인하면 partition 갯수가 맞지 않기 때문에 조인 수행 불가.

```sql
select a.user_id, a.name, b.*
from simple_user_stream_test a 
inner join user_activity_stream b within 1 hours on a.user_id= b.user_id emit changes;
```

- siple_user_stream_test의 파티션 갯수를 3으로 하되, key 컬럼을 int가 아닌 varchar로 변경

```sql
drop stream if exists simple_user_stream_test delete topic;

create stream simple_user_stream_test
(
	user_id varchar key,
	name varchar,
	email varchar
) with (
  KAFKA_TOPIC = 'simple_user_test',
  KEY_FORMAT = 'KAFKA', 
  VALUE_FORMAT ='JSON',
  PARTITIONS = 3
);

insert into simple_user_stream_test(user_id, name, email) values ('1', 'John', 'test_email_01@test.domain');
insert into simple_user_stream_test(user_id, name, email) values ('2', 'Merry', 'test_email_02@test.domain');
insert into simple_user_stream_test(user_id, name, email) values ('3', 'Elli', 'test_email_03@test.domain');
insert into simple_user_stream_test(user_id, name, email) values ('4', 'Mike', 'test_email_04@test.domain');
--이름을 Tom 그리고 Tommy로 변경시 데이터 추가 입력. 
insert into simple_user_stream_test(user_id, name, email) values ('5', 'Tom', 'test_email_05@test.domain');
insert into simple_user_stream_test(user_id, name, email) values ('5', 'Tommy', 'test_email_05@test.domain');

select * from simple_user_stream_test;
```

- simple_user_stream_test과 user_activity_stream을 조인하면  조인키 컬럼 타입이 맞지 않기 때문에 조인 실패

```sql
select a.user_id, a.name, b.*
from simple_user_stream_test a 
inner join user_activity_stream b within 1 hours on a.id= b.user_id emit changes;
```

- 조인키 컬럼 타입을 변환하여 조인이 가능한 수준이라면 cast() 함수를 이용하여 조인 수행.  만약 user_activity_stream을 생성한지가 오래 되었으면 within 절을 생성된 시점에 맞게 오랜 기간으로 설정할 것.  그렇지 않으면 방금 생성한 simple_user_stream_test와 within 기간이 맞지 않아서 조인 불가함.
- 아래는 simple_user_stream_test의 id 타입을 cast()를 이용하여 integer로 변환 후 조인.

```sql
select a.user_id, a.name, b.*
from simple_user_stream_test a 
inner join user_activity_stream b within 10 days on cast(a.user_id as integer) = b.user_id emit changes;
```

- 아래는 user_activity_stream의 user_id타입을 cast()를 이용하여 varchar로 변환 후 조인.

```sql
select a.user_id, a.name, b.*
from simple_user_stream_test a 
inner join user_activity_stream b within 10 days on a.user_id = cast(b.user_id as varchar) emit changes;
```

- 이렇게 cast()로 조인키 타입을 변경하면 단순히 key값의 타입을 변경하는 것에 그치지 않고, 조인 전에 해당 cast(조인키) 기반으로 repartition이 일어남.  왜냐하면 key값의 타입이 변경되는 key값에 따른 파티션 분배 전략이 달라져야 하기 때문. 예를 들어 문자열 ‘1’과 숫자값 1은 서로 다른 값으로 파티션에 할당되기 때문에 조인전에 repartition을 수행해야함.
- cast(조인키)로 조인키 변환을 수행하여 조인을 수행할 때 data/kafka-logs에 생기는 임시 topic을 보면 cast()가 적용된 토픽에 대해서 repartition이 발생함을 알 수 있음.
- stream과 table 조인시에 조인 키 컬럼 타입 변경에 유의가 필요. table의 pk 컬럼은 조인 시 가공되어서는 안되므로 cast() 적용을 반드시 stream쪽 조인 키 컬럼에 적용해야 함.

```sql
-- primary key 컬럼인 id를 varchar 타입으로 새로운 table 생성. 
create table simple_user_table_test
(
	user_id varchar primary key,
	name varchar,
	email varchar
) with (
  KAFKA_TOPIC = 'simple_user_test',
  KEY_FORMAT = 'KAFKA', 
	VALUE_FORMAT ='JSON',
  PARTITIONS = 3
);

select * from simple_user_table emit changes;

select b.user_id b.name, a.* 
from user_activity_stream a
inner join simple_user_table_test b on cast(a.user_id as varchar) = b.id emit changes;

-- 아래는 table의 primary key를 가공하여 조인 시도하나 수행 안됨. 
select b.user_id, b.name, a.* 
from user_activity_stream a
inner join simple_user_table_test b on a.user_id = cast(b.user_id as integer) emit changes;
```

### 조인 후 Group by 수행 시 이슈

- 조인과 Group by를 같이 수행은 할 수 있지만, 조인을 먼저한 mview에 group by를 적용한 결과와 달라 질 수 있음.
- 조인과 group by를 하나의 KSQL에 적용하면 조인이 다 되지 않은 상황에서 GROUP BY 가 됨. 즉 특정값과 조인되는 다른 값이 10개라면 이중에 처음 4개만 조인하고 GROUP BY 가 수행되고, 이후 다시 6개가 조인되고 GROUP BY 가 수행되면서 결과가 달라지게 되는 문제가 발생.  때문에 조인과 GROUP BY를 같이 수행해서는 안됨.
- 특히 GROUP BY에서만 사용할 수 있는 WINDOWS절을 조인과 함께 사용시 원치않는 부작용이 발생할 수 있음.

```sql
select a.id, count(*) as cnt, avg(activity_point) as avg_point
from simple_user_stream a
inner join user_activity_stream b within 1 hours on a.id= b.user_id
group by a.id emit changes;

-- 만약 사용자의 이름까지 함께 보고 싶다면 SQL은 아래와 같이 사용하면 되나 KSQL은 MAX(a.name)과 같이 문자열에 max()적용을 할 수 없으므로 오류 발생. 
select a.id, max(a.name) as name, count(*) as cnt, avg(activity_point) as avg_point
from simple_user_stream a
inner join user_activity_stream b within 1 hours on a.id= b.user_id
group by a.id emit changes;

-- 아래와 같이 latest_by_offset()을 사용하여 name을 가져옴. 
select a.id, latest_by_offset(a.name) as name, count(*) as cnt, avg(activity_point) as avg_point
from simple_user_stream a
inner join user_activity_stream b within 1 hours on a.id= b.user_id
group by a.id emit changes;
```
