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
inner join user_activity_stream b on a.user= b.user_id;

-- 아래는 push 쿼리지만 within 절이 없어서 오류
select a.*, b.*
from simple_user_stream a 
inner join user_activity_stream b on a.user= b.user_id emit changes;

select a.user_id, a.name, b.*
from simple_user_stream a 
inner join user_activity_stream b within 2 hours on a.user_id= b.user_id emit changes;

```

### join 실습- stream-table 조인

**Semantics of Stream-Table Joins 부분 참조 할것.** 

- stream-table 조인은 순서가 중요함. 조인절에 먼저 stream이 오고 다음에 table이 와야함. table→ stream 조인은 수행되지 않고 stream→table 조인은 수행됨.
- left side의 stream에 input record가 join을 trigger함. right side의 table에 들어오는 input record는 내부 조인 state만 update함.
- Input records for the table with a NULL value are interpreted as *tombstones*
 for the corresponding key, which indicate the deletion of the key from the table. Tombstones don't trigger the join.
- stream과 table 조인은 within 절을 적용해서는 안됨.

```sql
select b.user_id, b.name, a.* 
from user_activity_stream a
inner join simple_user_table b on a.user_id= b.user_id emit changes;

-- 아래는 table을 기준으로 stream을 조인하므로 수행되지 않음. 
select a.user_id, a.name, b.*
from simple_user_table a
inner join user_activity_stream b on a.user_id= b.user_id emit changes;

-- 아래는 stream-table 조인 시 within 절을 적용하면 수행되지 않음.  
select b.user_id, b.name, a.* 
from user_activity_stream a
inner join simple_user_table b within 2 hours on a.user_id= b.user_id emit changes;
```

- 조인 시 a.* 과 같이 alias와 *를 같이 사용하게 되면 컬럼명을 alias_기존컬럼명으로 만들기 때문에 주의가 필요함. 즉 A_USER_ID, A_ACTIVITY_ID와 같이 만들어짐.

```sql
select b.user_id, b.name, a.* 
from user_activity_stream a
inner join simple_user_table b on a.user_id= b.user_id emit changes;
```

- 조인 결과를 MView로 생성.  Stream→table 조인은 Stream으로 생성되어야 하며, Table로 생성할 수 없음.  with 절에 topic 속성값을 주지 않으면 자동으로 join key값을 key로, topic 명은 Mview명을 가져옴.  partition 갯수는 조인 시 partition 갯수가 같아야 하므로 그대로 적용가능,

```sql
drop stream if exists user_activity_join_mv delete topic;

create stream user_activity_join_mv
as
select b.user_id, b.name, a.user_id, a.activity_id, a.activity_type, a.activity_point
from user_activity_stream a
inner join simple_user_table b on a.user_id= b.user_id emit changes;

-- MView에 pull 쿼리, push 쿼리 모두 가능. 
select * from user_activity_join_mv;

drop stream user_activity_join_mv delete topic;
```

### Stream - Table 조인 시 Event 생성 시점에 따른 조인 처리

- 기존 simple_user_stream과 simple_user_table, user_activity_stream 재 생성.
- user_activity_stream과 simple_user table 조인한 CSAS Stream 생성.

```sql
drop stream if exists user_activity_join_mv delete topic;

create stream user_activity_join_mv
as
select b.user_id, b.name, a.user_id, a.activity_id, a.activity_type, a.activity_point
from user_activity_stream a
inner join simple_user_table b on a.user_id= b.user_id emit changes;

select * from user_activity_join_mv emit changes;
```

- 다른 창에서 ksql cli 실행 후 아래와 같이 stream에 신규 데이터 입력후 조인 결과 mview 확인.

```sql
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (5, 2,'deposit',0.78);
```

- ksql cli에서  아래와 같이 table에 신규 데이터 입력후 조인 결과 mview 확인.   table의 데이터가 변경되어도 조인 결과에 반영되지 않음

```sql
insert into simple_user_table(user_id, name, email) values (5, 'Tomminn', 'test_email_05@test.domain');
```

- 아래와 같이 새로운 user_id 6인 데이터를 추가하면 inner join이 되지 않으므로 당연히 조인 결과에 반영되지 않음.

```sql
insert into simple_user_table(user_id, name, email) values (6, 'Brandon', 'test_email_06@test.domain');
```

- 새로운 user_id 6으로 user_activity_stream 데이터를 입력 한 후 조인 결과 확인.  조인 결과에 반영됨.

```sql
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (5, 3,'mobile_open',0.42);

INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (6, 1,'branch_visit',0.3);
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
inner join user_activity_stream b within 1 hours on a.user_id= b.user_id emit changes;
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
inner join simple_user_table_test b on cast(a.user_id as varchar) = b.user_id emit changes;

-- 아래는 table의 primary key를 가공하여 조인 시도하나 수행 안됨. 
select b.user_id, b.name, a.* 
from user_activity_stream a
inner join simple_user_table_test b on a.user_id = cast(b.user_id as integer) emit changes;
```

### join 실습- table-table 조인.

- 기존 simple_user_stream과 simple_user_table, user_activity_stream 재 생성.
- table-table 조인은 빈번하게 사용되지는 않음. 1:1 조인만 사용권장하며 1:M 조인 사용시 매우 주의가 필요함.

```sql
--ctas mview로 새로운 table 생성. 
create table user_activity_mv01_tab
as
select user_id, count(*) as user_cnt, sum(activity_point) as sum_point
from user_activity_stream
group by user_id;

-- table-table 조인 수행. push 쿼리만 가능. 
select a.user_id, a.name, b.user_id, b.user_cnt, b.sum_point
from simple_user_table a
    join user_activity_mv01_tab b on a.user_id = b.user_id emit changes;
```

- simple_user_table에 신규 데이터 입력

```sql
insert into simple_user_table(user_id, name, email) values (5, 'Tomsons', 'test_email_05@test.domain');

-- table 위치를 변경해서 다시 조인. 
select a.user_id, a.name, b.user_id, b.user_cnt, b.sum_point
from user_activity_mv01_tab b
    join simple_user_table a on a.user_id = b.user_id emit changes;

insert into simple_user_table(user_id, name, email) values (5, 'Tommaa', 'test_email_05@test.domain');
```

- table - table m:1 조인 수행.  stream - table과 마찬가지로 반드시 m이 되는 테이블이 조인 선두, 1이 되는 집합이 조인 후미에 와야 함.
- 무엇보다도 m:1 조인 시 M쪽 KEY가 JSON 포맷으로 되면서 조인 대상 테이블의 조인 키별 파티션 분배가 서로 달라지면서 조인 결과가 제대로 생성되지 않는 문제가 발생.

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
select a.user_id, a.name, b.user_id, b.user_cnt, b.sum_point
from simple_user_table a
    join user_activity_mv02_tab b on a.user_id = b.user_id emit changes;

--아래는 m레벨의 집합이 조인 선두에 오므로 수행 가능. 
--하지만 조인 키로 조인 테이블들이 서로 다른 파티션에 존재하므로 제대로 조인이 되지 않음. 
select b.user_id, b.name, a.user_id, a.user_cnt, a.sum_point
from user_activity_mv02_tab  a
    join simple_user_table b on a.user_id = b.user_id emit changes;
```

- M:1 조인을 반드시 해야할 경우 파티션의 갯수는 양쪽 테이블 모두 1개가 되어야 함.

```sql
--ctas mview로 새로운 table 생성. 
-- 여러개의 컬럼이 Primary Key가 될 경우 KEY_FORMAT은 KAFKA-FORMAT 과 같은 primitive 타입이 될 수가 없으며 JSON Format등으로 설정해야함. 
create table user_activity_mv03_tab
with
(
-- 아래는 FORMAT='JSON'으로 대체 될 수 있음. 
KEY_FORMAT='JSON',
VALUE_FORMAT='JSON',
PARTITIONS = 1
)
as
select user_id, activity_type, count(*) as user_cnt, sum(activity_point) as sum_point
from user_activity_stream
group by user_id, activity_type;

create stream simple_user_onep_table
(
	user_id integer key,
	name varchar,
	email varchar
) with (
  KAFKA_TOPIC = 'simple_user_onep_table_topic',
  KEY_FORMAT = 'KAFKA', 
  VALUE_FORMAT ='JSON',
  PARTITIONS = 1
);

insert into simple_user_onep_table(user_id, name, email) values (1, 'John', 'test_email_01@test.domain');
insert into simple_user_onep_table(user_id, name, email) values (2, 'Merry', 'test_email_02@test.domain');
insert into simple_user_onep_table(user_id, name, email) values (3, 'Elli', 'test_email_03@test.domain');
insert into simple_user_onep_table(user_id, name, email) values (4, 'Mike', 'test_email_04@test.domain');
insert into simple_user_onep_table(user_id, name, email) values (5, 'Tommy', 'test_email_05@test.domain');

select b.user_id, b.name, a.user_id, a.user_cnt, a.sum_point
from user_activity_mv03_tab  a
    join simple_user_table b on a.user_id = b.user_id emit changes;
```

- Group by CTAS로 생성된 테이블과 Master성 테이블 조인을 하는 경우 Stream과 Table 조인 후 Group by로 변경하는것 이 더 효율적임

```sql
create stream user_activity_join_stream_mv
as
select a.user_id as user_id, b.name as name, 
   a.activity_type as activity_type, a.activity_point as activity_point
from user_activity_stream a
  join simple_user_table b on a.user_id = b.user_id emit changes;

create table user_activity_summary_table_mv01
as
select user_id, latest_by_offset(name) as name, 
   count(*) as user_cnt, sum(activity_point) as sum_point
from user_activity_join_stream_mv
group by user_id emit changes;

-- user_activity_stream 이후에 table의 user_id 5번에 대한 이름이 변경된 데이터는 출력되지 않음. 
select * from user_activity_summary_table_mv01 emit changes;
select * from user_activity_join_stream_mv emit changes;

-- user_activity_stream에 신규 데이터 입력후에 출력 확인. 
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (5, 2,'deposit',0.78);

create table user_activity_summary_table_mv02
as
select user_id, activity_type, latest_by_offset(name) as name, 
   count(*) as user_cnt, sum(activity_point) as sum_point
from user_activity_join_stream_mv
group by user_id, activity_type emit changes;

select * from user_activity_summary_table_mv02 emit changes;

drop table user_activity_mv03_tab delete topic;
drop table user_activity_summary_table_mv02 delete topic;
drop table user_activity_summary_table_mv01 delete topic;
drop stream user_activity_join_stream_mv delete topic;
drop stream simple_user_onep_table delete topic;
```

### Outer Join

- Stream-Stream Outer Join 수행.

```sql
select from_unixtime(rowtime) as rowtime_ts, * from simple_user_stream;

select from_unixtime(rowtime) as rowtime_ts, * from user_activity_stream;

-- stream to stream outer join 수행. 
select a.user_id, a.name, b.*
from simple_user_stream a 
left outer join user_activity_stream b within 2 hours on a.user_id= b.user_id emit changes;

-- 다른 CLI 창에서 신규 데이터 입력 후 outer join 결과 확인. 
insert into simple_user_stream(user_id, name, email) values (7, 'Kelly', 'test_email_07@test.domain');

INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (7, 1,'web_open',0.61);
```

- Stream-Table Outer Join 수행.

```sql
select b.user_id, b.name, a.* 
from user_activity_stream a
left join simple_user_table b on a.user_id= b.user_id emit changes;

-- 다른 CLI 창에서 신규 데이터 입력 후 outer join 결과 확인. 
INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (8, 1,'web_open',0.56);

INSERT INTO simple_user_stream(user_id, name, email) values (8, 'Mario', 'test_email_08@test.domain');

INSERT INTO user_activity_stream (user_id, activity_id, activity_type, activity_point) VALUES (8, 2,'mobile_open',0.36);
```

- Table-Table Outer Join 수행.

```sql

select b.user_id, b.name, a.user_id, a.user_cnt, a.sum_point
from user_activity_mv01_tab  a
left join simple_user_table b on a.user_id = b.user_id emit changes;
```

### 파티션 Key컬럼이 아닌 조인 Key로 조인

### Data 준비

- simple_customers_table Stream을 생성.

```sql
drop table simple_customers_table delete topic;

create table simple_customers_table
(
  customer_id integer primary key,
  name varchar,
  email varchar
) with (
  KAFKA_TOPIC = 'simple_customers_topic',
  KEY_FORMAT = 'KAFKA', 
  VALUE_FORMAT ='JSON',
  PARTITIONS = 3
);

INSERT INTO simple_customers_table (customer_id, name, email) VALUES(1, 'Tammy Bryant', 'tammy.bryant@internalmail');
INSERT INTO simple_customers_table (customer_id, name, email) VALUES(2, 'Roy White', 'roy.white@internalmail');
INSERT INTO simple_customers_table (customer_id, name, email) VALUES(3, 'Gary Jenkins', 'gary.jenkins@internalmail');
INSERT INTO simple_customers_table (customer_id, name, email) VALUES(4, 'Victor Morris', 'victor.morris@internalmail');
INSERT INTO simple_customers_table (customer_id, name, email) VALUES(5, 'Beverly Hughes', 'beverly.hughes@internalmail');
INSERT INTO simple_customers_table (customer_id, name, email) VALUES(6, 'Evelyn Torres', 'evelyn.torres@internalmail');
INSERT INTO simple_customers_table (customer_id, name, email) VALUES(7, 'Carl Lee', 'carl.lee@internalmail');
INSERT INTO simple_customers_table (customer_id, name, email) VALUES(8, 'Douglas Flores', 'douglas.flores@internalmail');
INSERT INTO simple_customers_table (customer_id, name, email) VALUES(9, 'Norma Robinson', 'norma.robinson@internalmail');
INSERT INTO simple_customers_table (customer_id, name, email) VALUES(10, 'Gregory Sanchez', 'gregory.sanchez@internalmail');
```

- sale_orders_stream stream생성.

```sql
create stream sale_orders_stream
(
  order_id integer key,
  order_ts timestamp,
  customer_id integer,
  order_status varchar,
  store_id integer
) with (
  KAFKA_TOPIC = 'sale_orders_stream_topic',
  KEY_FORMAT = 'KAFKA', 
  VALUE_FORMAT ='JSON',
  PARTITIONS = 3
);

INSERT INTO sale_orders_stream (order_id, order_ts, customer_id, order_status, store_id)
	VALUES(2, '2023-02-08T20:58:10.000', 1, 'COMPLETE', 1);
INSERT INTO sale_orders_stream (order_id, order_ts, customer_id, order_status, store_id)
	VALUES(3, '2023-02-08T23:17:07.000', 2, 'COMPLETE', 1);
INSERT INTO sale_orders_stream (order_id, order_ts, customer_id, order_status, store_id)
	VALUES(4, '2023-02-09T13:43:36.000', 1, 'COMPLETE', 3);
INSERT INTO sale_orders_stream (order_id, order_ts, customer_id, order_status, store_id)
	VALUES(5, '2023-02-11T18:01:30.000', 3, 'COMPLETE', 2);
INSERT INTO sale_orders_stream (order_id, order_ts, customer_id, order_status, store_id)
	VALUES(6, '2023-02-11T20:11:48.000', 3, 'COMPLETE', 1);
INSERT INTO sale_orders_stream (order_id, order_ts, customer_id, order_status, store_id)
	VALUES(7, '2023-02-22T00:57:11.000', 5, 'COMPLETE', 2);

SELECT * FROM sale_orders_stream;
```

- sale_orders_stream과 simple_customers_table 을 Stream-Table 조인 수행.

```sql
select a.*, b.*
from sale_orders_stream a
  join simple_customers_table b on a.customer_id = b.customer_id emit changes;
```

- cd ~/data/kafka-logs로 이동하여 repartition 내부 토픽이 생성됨을 확인.
