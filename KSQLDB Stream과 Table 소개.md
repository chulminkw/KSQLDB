# KSQLDB Stream과 Table 소개

## 처음 실행하는 KSQLDB Stream

- 아래와 같이 simple_user_stream 토픽을 생성.

```sql
kafka-topics --bootstrap-server localhost:9092 --create --topic simple_user_stream
```

- kafka-console-producer를 이용하여 simple_user_stream 토픽에 json 형태로 메시지 전송.

```sql
kafka-console-producer --bootstrap-server localhost:9092 --topic simple_user_stream
-- 전송될 메시지들 
{"id": 1, "name": "test_name_01", "email": "test_email_01@test.domain"}
{"id": 2, "name": "test_name_02", "email": "test_email_02@test.domain"}
{"id": 3, "name": "test_name_03", "email": "test_email_03@test.domain"}
{"id": 4, "name": "test_name_04", "email": "test_email_04@test.domain"}
```

- kafka-console-consumer로 토픽에 전송된 메시지 확인.

```sql
kafka-console-consumer --bootstrap-server localhost:9092 --topic simple_user_stream --from-beginning
```

- ksql을 기동하고 CLI에서 아래 명령어로 Stream을 생성하고 Select SQL을 수행하여 Stream 내용 조회.

```sql
create stream simple_user_stream 
(
  id int,
  name varchar,
  email varchar
) with (
  KAFKA_TOPIC = 'simple_user_stream',
  VALUE_FORMAT ='JSON'
);

select * from simple_user_stream;
```

- 추가 메시지를 kafka-console-producer를 통해 전송하고 Stream을 push query로 조회

```sql
set 'auto.offset.reset' = 'earliest';
select * from simple_user_stream emit changes;
```

- stream들의 목록을 확인하고, 생성한 simple_user_stream을 삭제. stream 삭제시 topic까지 함께 삭제.

```sql
show streams;

drop stream simple_user_stream delete topic;

show streams;
```

## Stream

- Create stream/table 명령어의 With 절은 Topic과 Format에 대한 설정을 부여할 수 있음. 만약 Topic이 미리 만들어져 있지 않으면 해당 Topic명과 설정으로 새롭게 Topic 생성.
- 새로운 TOPIC을 생성할 때는 반드시 With 절로 KAFKA_TOPIC, VALUE_FORMAT 그리고 생성될 TOPIC의 PARTITIONS 갯수를 반드시 지정해 줘야 함.
- Stream은 Topic과 마찬가지로 Key값을 가질 수도, 그렇지 않을수도 있음.

```sql
create stream simple_user_stream 
(
  id int,
  name varchar,
  email varchar
) with (
  KAFKA_TOPIC = 'simple_user_stream',
  VALUE_FORMAT ='JSON',
  PARTITIONS = 1
);
```

- Stream은 varchar(10)과 같이 varchar의 길이를 설정할 수 없고 varchar로만 설정. 가능.

[Data Types Overview - ksqlDB Documentation](https://docs.ksqldb.io/en/latest/reference/sql/data-types/)

- Stream 생성 후 CLI를 통해서 Stream과 생성된 Topic을 확인

```sql
show streams;
show topics;
```

- 새롭게 생성된 Stream에 데이터를 insert 문으로 입력 후 Stream과 Topic 메시지 확인.

```sql
insert into simple_user_stream(id, name, email) values (1, 'test_name_01', 'test_email_01@test.domain');
insert into simple_user_stream(id, name, email) values (2, 'test_name_02', 'test_email_02@test.domain');

select * from simple_user_stream;
```

- ksql cli 상에서 simple_user_stream 토픽 메시지 확인

```sql
set 'auto.offset.reset'='earliest';

print simple_user_stream from beginning;
```

- 토픽 메시지의 json 메시지는 별도의 Schema 정보를 가지고 있지 않음.

### Describe 로 Stream 메타 정보 확인

- describe stream/table명 extended를 이용하여 Stream/Table의 DDL, 데이터 타입등 주요 메타 정보에 대한 정보를 알수 있음.

```sql
describe simple_user_stream extended;
```

### Stream/Table/CSAS/CTAS에 대한 metastore 정보

- Stream/Table/CSAS/CTAS에 대한 metastore 정보는 _confluent-ksql-default__command_topic 토픽 정보에 저장됨.

```sql
kafka-console-consumer --bootstrap-server localhost:9092 --topic _confluent-ksql-default__command_topic --from-beginning | jq '.'
```

### Key를 가지는 Stream

- 대부분의 Topic은 key값을 가지고 있음(성능상 Partition별 분할등을 위해). Stream역시 이를 반영하여 구성 필요. Stream의 Key는 topic의 Key와 동일한 개념이며 중복 값을 가질 수 있음. 반면에 Table의 PK는 Stream의 Key와는 다르게 중복값을 가질 수 없음.
- key로 사용될 컬럼에 key 키워드를 부여하여 stream 생성.

```sql
//기존 stream과 topic을 삭제
drop stream simple_user_stream delete topic;

//아래는 topic이 삭제되었으므로 오류가 발생. 
print simple_user_stream;

//새롭게 id를 key로 부여하여 simple_user_stream
create stream simple_user_stream 
(
  id int key,
  name varchar,
  email varchar
) with (
  KAFKA_TOPIC = 'simple_user_stream',
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT ='JSON',
  PARTITIONS = 1
);

```

- describe 명령어로 확인하면 id 컬럼이 key로 되었음을 확인 할 수 있음.

```sql
describe simple_user_stream extended;
```

- stream에 데이터를 입력 후 토픽 메시지 확인.

```sql
insert into simple_user_stream(id, name, email) values (1, 'test_name_01', 'test_email_01@test.domain');
insert into simple_user_stream(id, name, email) values (2, 'test_name_02', 'test_email_02@test.domain');

select * from simple_user_stream;

print simple_user_stream;
```

### pull query와 push query

- Stream에 데이터 추가가 있을 때 이를 실시간으로 Client로 전송 반영하는 Push Query 수행. push query는 emit changes 절로 수행.

```sql
SET 'auto.offset.reset'='earliest';

--pull query
select * from simple_user_stream;

-- 다른 CLI 창에서 아래 PUSH 쿼리 수행. 
SET 'auto.offset.reset'='earliest';

select * from simple_user_stream emit changes;
```

- 새로운 데이터를 추가하고 push 쿼리의 변경 사항을 확인

```sql
insert into simple_user_stream(id, name, email) values (3, 'test_name_03', 'test_email_03@test.domain');

-- PUSH 쿼리 CLI 창에서 내용 확인. 

-- 한번 더 데이터 입력하고 PUSH 쿼리 CLI 창에서 내용 확인
insert into simple_user_stream(id, name, email) values (4, 'test_name_04', 'test_email_04@test.domain');
```

- PUSH QUERY 수행 시 Stream Thread가 Consumer를 기반으로 데이터를 계속 가져오게 됨. 이를 위해 새롭게 Consumer Group에서 Active Consumer를 생성.  kafka-consumer-groups 명령어로 consumer group 리스트 확인.

```sql
kafka-consumer-groups --bootstrap-server localhost:9092 --list

# 아래 명령어는 특정 consumer group 으로 consumer 들 정보를 보다 상세하게 조회
kafka-consumer-groups --bootstrap-server localhost:9092 --group consumer_group_명 --describe
```

### Query 오브젝트

- KSQLDB를 재기동하고도 Consumer Group이 여전히 존재하면 아래와 같이 삭제

```bash
# 재 기동후 consumer group 조회
kafka-consumer-groups --bootstrap-server localhost:9092 --list

# consumer group에 No active members 확인. 
kafka-consumer-groups --bootstrap-server localhost:9092 --group consumer그룹명 --describe

# consumer group 삭제
kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group consumer그룹명
```

- ksql cli를 다른 터미널에서 기동한 후 현재 수행중인 Query에 대한 정보 확인. push 쿼리는 계속 특정 Query가 수행되고 있음을 알 수 있음.

```sql
ksql

show queries;
```

### Data Type과 Alter DDL

- 기존 simple_user_stream을 삭제하고 새로운 컬럼을 추가하여 신규로 생성.

```bash
drop stream simple_user_stream delete topic;

create stream simple_user_stream 
(
  id int,
  salary decimal(10, 2),
  created_time time,
  created_date date,
  created_ts timestamp,
  name varchar,
  email varchar
) with (
  KAFKA_TOPIC = 'simple_user_stream',
  VALUE_FORMAT ='JSON',
  PARTITIONS = 1
);

insert into simple_user_stream(id, salary, created_time, created_date, created_ts, name, email) values \
(1, 987654.21, '05:23:32', '2023-03-07', '2023-03-07T05:23:32.931', 'test_name_01', 'test_email_01@domain.com');

insert into simple_user_stream(id, salary, created_time, created_date, created_ts, name) values \
(2, 987654.21, '05:23:32', '2023-03-07', '2023-03-07T05:23:32.931', 'test_name_02');

select * from simple_user_stream;

select unix_date(created_date) from simple_user_stream;

```

- Stream/Table은 Alter 명령어로 Column을 변경할 수 있으며, 현재 Add column 만 지원됨. 아래는 simple_user_stream의 phone_no 컬럼을 추가함

```sql
alter stream simple_user_stream add column phone_no varchar;

describe simple_user_stream;

insert into simple_user_stream(id, name, email, phone_no) values 
(3, 'test_name_03', 'test_email_03@test.domain', 'xxx-xxx-xxxx');

select * from simple_user_stream;
```

- KSQLDB는 컬럼의 Not Null 제약을 지원하지 않음.  기본적으로 Nullable 값이며, Not Null 키워드로 컬럼 Constraint를 지원하지 않음. 단 Table의 경우 Primary Key는 반드시 Not Null이 묵시적으로 되어야 함.

### ROWTIME 의사 컬럼

- 모든 Stream과 Table은 생성 시 지정된 컬럼외에 레코드의 생성 시점을 값으로 가지는 별도의 Timestamp 컬럼을 가질 수 있음. 만약 별도의 Timestamp 컬럼명을 생성 DDL 시 지정하지 않으면 컬럼명은 ROWTIME으로 지정됨. 이 Timestamp 컬럼은 생성 DDL시 KSQLDB에 의해서 추가적으로 생성.
- ROWTIME에 들어가는 값은 개별 레코드가 입력되는 시점을 Unix epoch 시간 가지며, BIGINT 타입의 컬럼임(Timestamp 타입의 컬럼이 아님)

```sql
describe simple_user_stream extended;

select rowtime, * from simple_user_stream;

--또는 
select rowtime, a.* from simple_user_stream a;

--rowtime 변환. timestamptostring()함수는 deprecation됨. 
--SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss') as rowtime_string, a.* from simple_user_stream a;

select from_unixtime(rowtime) as rowtime, a.*  from simple_user_stream a;

```

### KSQL 수행 시 마다 auto.offset.reset 을 earliest로 자동 설정.

- $CONFLUENT_HOME/etc/ksqldb/ksql-server.properties에 아래 추가

```bash
ksql.streams.auto.offset.reset=earliest
```

## Table

- Table은 Stream과 달리 반드시 PK를 가져야 하며, 이 PK는 고유한 값으로 데이터가 구성되어야 함.
- Table 역시 Topic을 기반으로 함.

```sql
--아래 명령어는 table 생성 시 primary key를 지정하지 않았으므로 오류를 발생 시킴. 
create table simple_user_table 
(
  id integer,
  name varchar,
  email varchar
) with (
  KAFKA_TOPIC = 'simple_user_table',
  KEY_FORMAT = 'KAFKA', 
  VALUE_FORMAT ='JSON',
  PARTITIONS = 1
);

--table 생성 시 primary key 지정 필요. 
create table simple_user_table
(
  id integer primary key,
  name varchar,
  email varchar
) with (
  KAFKA_TOPIC = 'simple_user_table',
  KEY_FORMAT = 'KAFKA', 
  VALUE_FORMAT ='JSON',
  PARTITIONS = 1
);
```

- describe 테이블명 으로 메타 정보 확인

```sql
describe simple_user_table extended;
```

### Table 데이터 입력 및 조회

- primary key가 null인 경우 insert 수행 시 오류

```sql
insert into simple_user_table(id, name, email) values (1, 'test_name_01', 'test_email_01@test.domain');
insert into simple_user_table(id, name, email) values (2, 'test_name_02', 'test_email_02@test.domain');

--primary key가 null인 경우 insert 수행 되지 않음. 
insert into simple_user_table(name, email) values ('test_name_03', 'test_email_03@test.domain');

```

- topic을 직접 Source로 하는 Table에 pull 성 Query는 수행 되지 않음.  아래와 같이 데이터를 입력 후 select 쿼리를 수행하면 오류 발생.

```sql
-- 아래 pull 쿼리는 오류 발생. 
select * from simple_user_table;
```

- topic을 직접 Source로 하는 Table에 push성 쿼리만 수행 가능

```sql
select * from simple_user_table emit changes;
```

- table에 insert 수행 시 pk 컬럼은 topic의 key값으로 매칭되어 topic 메시지로 생성됨.

```sql
print simple_user_table;
```

### Table 데이터에 중복 데이터 입력

- RDBMS의 Table에 Primary Key가 있으면 해당 key값으로 중복된 데이터 입력이 안됨.  ksqldb도 이와 유사하지만 table에 primary key가 중복된 데이터를 insert로 입력됨.  다만 ksqldb 의 table 자체에서 중복된 pk 데이터는 가장 최근에 입력된 데이터 하나만 유일하게 추출함.
- 아래 SQL은 이미 id가 1인 레코드가 있지만 다시 id가 1인 레코드를 simple_user_table에 입력하지만 오류가 발생하지 않음.

```sql
insert into simple_user_table(id, name, email) values (1, 'new_test_name_01', 'new_test_email_01@test.domain');
insert into simple_user_table(id, name, email) values (2, 'new_test_name_02', 'new_test_email_02@test.domain');
```

```sql
insert into simple_user_table(id, name, email) values (1, 'test_name_01', 'test_email_01@test.domain');
insert into simple_user_table(id, name, email) values (2, 'test_name_02', 'test_email_02@test.domain');

--primary key가 null인 경우 insert 수행 되지 않음. 
insert into simple_user_table(name, email) values ('test_name_03', 'test_email_03@test.domain');
```

- 하지만 select 로 조회 시 id가 1인 레코드는 하나만 추출됨 id가 1인 레코드가 2인 레코드 보다 나중에 나옴에 유의. id가 1인 레코드가 나중에 다시 입력 되었으므로 select 조회시 마치 upsert와 같이 기존 id가 1인 레코드중 가장 최근(나중)에 입력된 데이터로 대체됨.

```sql
select * from simple_user_table emit changes;
```

- topic 으로 조회 시 id가 1인 레코드가 추가 되어 있음을 확인.

```sql
print simple_user_table;
```

- Stream을 생성하여 결과 확인.

```sql
create stream test_user_stream
(
  id integer key,
  name varchar,
  email varchar
) with (
  KAFKA_TOPIC = 'simple_user_table',
  KEY_FORMAT = 'KAFKA', 
  VALUE_FORMAT ='JSON'
);

select * from test_user_stream;

drop stream test_user_stream;
```

### Topic을 소스로 가지는 Table에서 RocksDB 동작 메커니즘

- 기존 Table을 삭제하고 다시 데이터 입력

```sql
drop table simple_user_table delete topic;

create table simple_user_table
(
  id integer primary key,
  name varchar,
  email varchar
) with (
  KAFKA_TOPIC = 'simple_user_table',
  KEY_FORMAT = 'KAFKA', 
  VALUE_FORMAT ='JSON',
  PARTITIONS = 1
);

insert into simple_user_table(id, name, email) values (1, 'test_name_01', 'test_email_01@test.domain');
insert into simple_user_table(id, name, email) values (2, 'test_name_02', 'test_email_02@test.domain');
-- 중복 데이터 입력
insert into simple_user_table(id, name, email) values (1, 'new_test_name_01', 'new_test_email_01@test.domain');

```

- Table에 수행되는 push 성 쿼리는 topic→ rocks db를 거치는 쿼리로 수행됨.  table에 push 쿼리를 수행 할 경우 임시성 changelog 토픽이 생성됨.  더불어 rocks db도 함께 기동하여 rocks db 파일도 같이 생성됨.

```sql
cd ~/data/kafka-logs
ls -lrt *changelog*
_confluent-ksql-default_transient_transient_SIMPLE_USER_TABLE_760213148581098427_1675233045633-KsqlTopic-Reduce-changelog-0

cd /tmp/kafka-streams
ls -lrt
```

- push 쿼리를 종료하면 임시성 changelog 토픽도 삭제되고 rocksdb 의 파일도 삭제됨.
- rocksdb의 default state store 저장 디렉토리를 변경.  $CONFLUENT_HOME/etc/ksqldb/ksql-server.properties에 아래 추가

```bash
ksql.streams.state.dir=/home/min/data/kafka-streams
```

### 여러개 컬럼들을 PK로 가지는 Table

- Table은 여러개의 컬럼들을 PK로 가질 수 있음.  PK가 여러개의 컬럼을 가질 때 KEY_FORMAT은 KAFKA가 될 수 없으며, JSON 또는 AVRO와 같은 Format이 되어야 함.

```sql
--아래는 KEY_FORMAT이 'KAFKA' 이므로 오류 발생.  
create table simple_user_table_mkey
(
  cust_id integer primary key,
  cust_seq integer primary key,
  name varchar,
  email varchar
) with (
  KAFKA_TOPIC = 'simple_user_table_mkey',
  KEY_FORMAT = 'KAFKA', 
  VALUE_FORMAT ='JSON',
  PARTITIONS = 1
);

--KEY_FORMAT을 JSON으로 설정하여 생성. 
create table simple_user_table_mkey
(
  cust_id integer primary key,
  cust_seq integer primary key,
  name varchar,
  email varchar
) with (
  KAFKA_TOPIC = 'simple_user_table_mkey',
  KEY_FORMAT = 'JSON', 
  VALUE_FORMAT ='JSON',
  PARTITIONS = 1
);
```

- 아래와 같이 데이터를 입력 후 데이터 확인.

```sql
insert into simple_user_table_mkey(cust_id, cust_seq, name, email) values (1, 1, 'test_name_01', 'test_email_01@test.domain');
insert into simple_user_table_mkey(cust_id, cust_seq, name, email) values (2, 1, 'test_name_02', 'test_email_02@test.domain');

select * from simple_user_table_mkey emit changes;

print simple_user_table_mkey;

drop table simple_user_table_mkey delete topic;
```
