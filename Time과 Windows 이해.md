# Time과 Windows 이해

### Stream/Table의 Rowtime Timestamp 필드

- Stream과 Table은 레코드의 생성시 해당 레코드의 기준이 되는 메타성 Timestamp를 가짐. 이 Timestamp 컬럼은 데이터가 생성되는 시점을 가질 수도 있고, 레코드의 특정 컬럼값을 지정할 수도 있음.
- 이 메타성 Timestamp는 Create Stream/Table 생성 시 with절의 Timestamp 속성을 지정하여 설정할 수 있으며 이를 지정하지 않으면 기본적으로 레코드가 입력되는 시점의 Timestamp를 rowtime이라는 컬럼명으로 가짐.
- rowtime은 기본적으로 unix time 으로 big int 형 값을 가짐.

```sql
--rowtime은 stream/table의 select 시 rowtime 컬럼명을 별도로 지정하지 않으면 볼수 없음. 
select * from simple_user_stream;

select rowtime, * from simple_user_stream;

-- timestamp field가 지정되지 않고 rowtime을 사용하고 있음을 확인. 
describe simple_user_stream extended;

-- rowtime은 unix epoch time millis이므로 이를 timestamp type으로 변환. 
select rowtime, from_unixtime(rowtime) as rowtime_ts from simple_user_stream;

select from_unixtime(rowtime) as rowtime_str, from_unixtime(unix_timestamp()) as currenttime_str from simple_user_stream;
```

### Event/Ingestion/Processing TIME

- kafka topic은 기본적으로 개별 메시지별로 timestamp 값을 가짐. 이 timestamp값은 producer가 메시지를 전송한 시점의 timestamp값 또는 broker가 topic에 메시지를 기록한 시점의 timestamp 값이 될 수 있음.
- topic에 저장되는 timestamp 유형은 CreateTime(producer 메시지 전송 시점), LogAppendTime(broker가 topic에 메시지 기록 시점)이 있으며 이는 broker의 log.message.timestamp.type 또는 topic의 message.timestamp.type 속성값으로 가지고 있음.

```sql
kafka-configs --bootstrap-server localhost:9092 --entity-type brokers --entity-name 0 --all --describe | grep log.message.timestamp.type
```

### 커스텀 Rowtime Timestamp의 설정

- Stream/Table의 Rowtime은 Create Stream/Table 생성 시 with절의 Timestamp 속성을 지정하여 설정할 수 있으며 이를 지정하지 않으면 기본적으로 레코드가 생성되는 시점의 Timestamp를 가짐.
- 아래 device_status_stream의 create_ts는 해당 event가 소스 시스템에서 발생한 시간임. 이를 timestamp로 지정함.

```sql
drop stream if exists device_status_stream delete topic;

-- WITH절에 TIMESTAMP 속성을 VARCHAR 타입 CREATE_TS 컬럼으로 지정. 아래는 TIMESTAMP_FORMAT을 지정하지 않아서 실패함. 
CREATE STREAM device_status_stream (
  device_id BIGINT KEY,
  create_ts VARCHAR,
  temperature DOUBLE,
  power_watt INT
) WITH (
  KAFKA_TOPIC = 'device_status_stream_topic',
  PARTITIONS = 1,
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'CREATE_TS'
);

insert into device_status_stream values (1, '2023-02-10T05:23:32.931', 5.2, 13);
insert into device_status_stream values (1, '2023-02-10T05:23:42.891', 7.4, 17);
insert into device_status_stream values (1, '2023-02-10T05:23:53.288', 4.2, 1);
insert into device_status_stream values (1, '2023-02-10T05:24:22.211', 3.7, 11);
insert into device_status_stream values (1, '2023-02-10T05:24:32.911', 6.8, 9);
insert into device_status_stream values (1, '2023-02-10T05:27:15.244', 3.8, 8);
insert into device_status_stream values (2, '2023-02-10T05:21:19.131', 7.2, 3);
insert into device_status_stream values (2, '2023-02-10T05:21:25.231', 12.4, 22);
insert into device_status_stream values (2, '2023-02-10T05:21:39.531', 15.6, 31);
insert into device_status_stream values (2, '2023-02-10T05:22:00.111', 12.1, 42);
insert into device_status_stream values (2, '2023-02-10T05:22:19.121', 22.7, 19);
insert into device_status_stream values (2, '2023-02-10T05:24:32.333', 16.7, 29);

-- 아래는 CREATE_TS가 VARCHAR 컬럼일 때 TIMESTAMP FORMAT으로 적용함. ''T''와 같이 T를 Escape하기 위해 두개의 single quote 적용 필요. 
drop stream if exists device_status_stream delete topic;

CREATE STREAM device_status_stream (
  device_id BIGINT KEY,
  create_ts VARCHAR,
  temperature DOUBLE,
  power_watt INT
) WITH (
  KAFKA_TOPIC = 'device_status_stream_topic',
  PARTITIONS = 1,
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'CREATE_TS',
  TIMESTAMP_FORMAT = 'yyyy-MM-dd''T''HH:mm:ss.SSS'
);

insert into device_status_stream values (1, '2023-02-10T05:23:32.931', 5.2, 13);
insert into device_status_stream values (1, '2023-02-10T05:23:42.891', 7.4, 17);
insert into device_status_stream values (1, '2023-02-10T05:23:53.288', 4.2, 1);
insert into device_status_stream values (1, '2023-02-10T05:24:22.211', 3.7, 11);
insert into device_status_stream values (1, '2023-02-10T05:24:32.911', 6.8, 9);
insert into device_status_stream values (1, '2023-02-10T05:27:15.244', 3.8, 8);
insert into device_status_stream values (2, '2023-02-10T05:21:19.131', 7.2, 3);
insert into device_status_stream values (2, '2023-02-10T05:21:25.231', 12.4, 22);
insert into device_status_stream values (2, '2023-02-10T05:21:39.531', 15.6, 31);
insert into device_status_stream values (2, '2023-02-10T05:22:00.111', 12.1, 42);
insert into device_status_stream values (2, '2023-02-10T05:22:19.121', 22.7, 19);
insert into device_status_stream values (2, '2023-02-10T05:24:32.333', 16.7, 29);

--아래는 CREATE_TS가 VARCHAR 컬럼일 때 TIMESTAMP FORMAT으로 적용함
drop stream if exists device_status_stream delete topic;

CREATE STREAM device_status_stream (
  device_id BIGINT KEY,
  create_ts VARCHAR,
  temperature DOUBLE,
  power_watt INT
) WITH (
  KAFKA_TOPIC = 'device_status_stream_topic',
  PARTITIONS = 1,
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'CREATE_TS',
  TIMESTAMP_FORMAT = 'yyyy-MM-dd HH:mm:ss.SSS'
);

insert into device_status_stream values (1, '2023-02-10 05:23:32.931', 5.2, 13);
insert into device_status_stream values (1, '2023-02-10 05:23:42.891', 7.4, 17);
insert into device_status_stream values (1, '2023-02-10 05:23:53.288', 4.2, 1);
insert into device_status_stream values (1, '2023-02-10 05:24:22.211', 3.7, 11);
insert into device_status_stream values (1, '2023-02-10 05:24:32.911', 6.8, 9);
insert into device_status_stream values (1, '2023-02-10 05:27:15.244', 3.8, 8);
insert into device_status_stream values (2, '2023-02-10 05:21:19.131', 7.2, 3);
insert into device_status_stream values (2, '2023-02-10 05:21:25.231', 12.4, 22);
insert into device_status_stream values (2, '2023-02-10 05:21:39.531', 15.6, 31);
insert into device_status_stream values (2, '2023-02-10 05:22:00.111', 12.1, 42);
insert into device_status_stream values (2, '2023-02-10 05:22:19.121', 22.7, 19);
insert into device_status_stream values (2, '2023-02-10 05:24:32.333', 16.7, 29);

--아래는 CREATE_TS가 TIMESTATMP 컬럼일 때 WITH절의 TIMESTAMP로 적용함
drop stream if exists device_status_stream delete topic;

CREATE STREAM device_status_stream (
  device_id BIGINT KEY,
  create_ts TIMESTAMP,
  temperature DOUBLE,
  power_watt INT
) WITH (
  KAFKA_TOPIC = 'device_status_stream_topic',
  PARTITIONS = 1,
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'CREATE_TS'
);

insert into device_status_stream values (1, '2023-02-10T05:23:32.931', 5.2, 13);
insert into device_status_stream values (1, '2023-02-10T05:23:42.891', 7.4, 17);
insert into device_status_stream values (1, '2023-02-10T05:23:53.288', 4.2, 1);
insert into device_status_stream values (1, '2023-02-10T05:24:22.211', 3.7, 11);
insert into device_status_stream values (1, '2023-02-10T05:24:32.911', 6.8, 9);
insert into device_status_stream values (1, '2023-02-10T05:27:15.244', 3.8, 8);
insert into device_status_stream values (2, '2023-02-10T05:21:19.131', 7.2, 3);
insert into device_status_stream values (2, '2023-02-10T05:21:25.231', 12.4, 22);
insert into device_status_stream values (2, '2023-02-10T05:21:39.531', 15.6, 31);
insert into device_status_stream values (2, '2023-02-10T05:22:00.111', 12.1, 42);
insert into device_status_stream values (2, '2023-02-10T05:22:19.121', 22.7, 19);
insert into device_status_stream values (2, '2023-02-10T05:24:32.333', 16.7, 29);
```

- create_ts 컬럼이 timestamp로 지정되어 있음을 확인하고 rowtime 컬럼과 create_ts 컬럼 확인.

```sql
-- create_ts 컬럼이 timestamp로 지정되어 있음을 확인. 
describe device_status_stream extended;

-- rowtime이 create_ts와 동일함. 
select rowtime, from_unixtime(rowtime) as rowtime_ts, a.* from device_status_stream a;

-- topic 메시지의 timestamp는 다름. 
print device_status_stream_topic;
```

### Tumbling Window

- 아래와 같이 기존 device_status_stream을 삭제하고 재 생성.

```sql
drop stream if exists device_status_stream delete topic;

CREATE STREAM device_status_stream (
  device_id BIGINT KEY,
  create_ts TIMESTAMP,
  temperature DOUBLE,
  power_watt INT
) WITH (
  KAFKA_TOPIC = 'device_status_stream_topic',
  PARTITIONS = 1,
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'CREATE_TS'
);

insert into device_status_stream values (1, '2023-02-10T05:23:32.931', 5.2, 13);
insert into device_status_stream values (1, '2023-02-10T05:23:42.891', 7.4, 17);
insert into device_status_stream values (1, '2023-02-10T05:23:53.288', 4.2, 1);
insert into device_status_stream values (1, '2023-02-10T05:24:22.211', 3.7, 11);
insert into device_status_stream values (1, '2023-02-10T05:24:32.911', 6.8, 9);
insert into device_status_stream values (1, '2023-02-10T05:27:15.244', 3.8, 8);
insert into device_status_stream values (2, '2023-02-10T05:21:19.131', 7.2, 3);
insert into device_status_stream values (2, '2023-02-10T05:21:25.231', 12.4, 22);
insert into device_status_stream values (2, '2023-02-10T05:21:39.531', 15.6, 31);
insert into device_status_stream values (2, '2023-02-10T05:22:00.111', 12.1, 42);
insert into device_status_stream values (2, '2023-02-10T05:22:19.121', 22.7, 19);
insert into device_status_stream values (2, '2023-02-10T05:24:32.333', 16.7, 29);
```

- 아래와 같이 Tumbling window를 적용하여 출력 결과 확인

```sql
select count(*) as cnt from device_status_stream window tumbling (size 1 minutes) emit changes;

select device_id, count(*) as cnt from device_status_stream group by device_id emit changes;

--window 절을 사용하면 windowstart, windowend 컬럼을 사용할 수 있음. 
select device_id, WINDOWSTART, WINDOWEND, count(*) as cnt from device_status_stream window tumbling (size 1 minutes) 
group by device_id emit changes;

select WINDOWSTART, WINDOWEND, count(*) as cnt from device_status_stream window tumbling (size 1 minutes) group by 1 emit changes;

-- size를 1, 2 minutes, 10 minutes, 1 seconds로 변화 시키면서 결과 확인. 
select device_id, from_unixtime(WINDOWSTART) as w_start, from_unixtime(WINDOWEND) as w_end, count(*) as cnt 
from device_status_stream window tumbling (size 1 minutes) group by device_id emit changes;
```

- 아래 신규 데이터를 입력한 뒤 출력 결과 확인.

```sql
insert into device_status_stream values (1, '2023-02-10T05:27:22.244', 4.8, 9);
insert into device_status_stream values (2, '2023-02-10T05:25:03.343', 12.7, 19);
```

### Hopping Window

- 아래와 같이 Hopping window를 적용하여 출력 결과 확인

```sql
select device_id, count(*) as cnt from device_status_stream group by device_id emit changes;

--window 절을 사용하면 windowstart, windowend 컬럼을 사용할 수 있음.  
select device_id, WINDOWSTART, WINDOWEND, count(*) as cnt from device_status_stream window hopping (size 1 minutes, advance by 10 seconds) 
group by device_id emit changes;

-- size를 2 minutes, 10 minutes, 1 seconds로 변화 시키면서 결과 확인. 
select device_id, from_unixtime(WINDOWSTART) as w_start, from_unixtime(WINDOWEND) as w_end, count(*) as cnt 
from device_status_stream window hopping (size 1 minutes, advance by 10 seconds)  group by device_id emit changes;

```

- 아래 신규 데이터를 입력한 뒤 출력 결과 확인.

```sql
insert into device_status_stream values (1, '2023-02-10T05:27:42.244', 2.8, 9);
insert into device_status_stream values (1, '2023-02-10T05:28:30.244', 6.2, 11);
insert into device_status_stream values (2, '2023-02-10T05:25:23.343', 12.7, 19);
```

### Session Window

- 아래와 같이 Session window를 적용하여 출력 결과 확인

```sql
select device_id, count(*) as cnt from device_status_stream group by device_id emit changes;

--window 절을 사용하면 windowstart, windowend 컬럼을 사용할 수 있음. 
select device_id, WINDOWSTART, WINDOWEND, count(*) as cnt from device_status_stream window session(1 minutes) 
group by device_id emit changes;

-- session을 1 minutes, 3 minutes 변화 시키면서 결과 확인. 
select device_id, from_unixtime(WINDOWSTART) as w_start, from_unixtime(WINDOWEND) as w_end, count(*) as cnt 
from device_status_stream window session(1 minutes)  group by device_id emit changes;

```

- 아래 신규 데이터를 입력한 뒤 출력 결과 확인.

```sql

insert into device_status_stream values (2, '2023-02-10T05:27:12.343', 12.7, 19);
-- 기존 window 크기를 변화 시키므로 기존 window 데이터는 tombstone 메시지 발생. 
insert into device_status_stream values (1, '2023-02-10T05:28:40.189', 6.0, 12);
insert into device_status_stream values (2, '2023-02-10T05:23:15.343', 11.7, 29);
```

### Window의 Grace period

- 아래와 같이 기존 device_status_stream을 삭제하고 재 생성.

```sql
drop stream if exists device_status_stream delete topic;

CREATE STREAM device_status_stream (
  device_id BIGINT KEY,
  create_ts TIMESTAMP,
  temperature DOUBLE,
  power_watt INT
) WITH (
  KAFKA_TOPIC = 'device_status_stream_topic',
  PARTITIONS = 1,
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'CREATE_TS'
);

insert into device_status_stream values (1, '2023-02-10T05:23:32.931', 5.2, 13);
insert into device_status_stream values (1, '2023-02-10T05:23:42.891', 7.4, 17);
insert into device_status_stream values (1, '2023-02-10T05:23:53.288', 4.2, 1);
insert into device_status_stream values (1, '2023-02-10T05:24:22.211', 3.7, 11);
insert into device_status_stream values (1, '2023-02-10T05:24:32.911', 6.8, 9);
insert into device_status_stream values (1, '2023-02-10T05:27:15.244', 3.8, 8);
insert into device_status_stream values (2, '2023-02-10T05:21:19.131', 7.2, 3);
insert into device_status_stream values (2, '2023-02-10T05:21:25.231', 12.4, 22);
insert into device_status_stream values (2, '2023-02-10T05:21:39.531', 15.6, 31);
insert into device_status_stream values (2, '2023-02-10T05:22:00.111', 12.1, 42);
insert into device_status_stream values (2, '2023-02-10T05:22:19.121', 22.7, 19);
insert into device_status_stream values (2, '2023-02-10T05:24:32.333', 16.7, 29);
```

- 아래와 같이 Window에 Grace Period를 적용하여 출력 결과 확인

```sql
select device_id, from_unixtime(WINDOWSTART) as w_start, from_unixtime(WINDOWEND) as w_end, count(*) as cnt 
from device_status_stream window tumbling (size 1 minutes) group by device_id emit changes;

select device_id, from_unixtime(WINDOWSTART) as w_start, from_unixtime(WINDOWEND) as w_end, count(*) as cnt 
from device_status_stream window tumbling (size 1 minutes, grace period 1 hour) group by device_id emit changes;

-- grace period를 2 ~ 6 minutes까지 변화 시키면서 결과 출력
select device_id, from_unixtime(WINDOWSTART) as w_start, from_unixtime(WINDOWEND) as w_end, count(*) as cnt 
from device_status_stream window tumbling (size 1 minutes, grace period 2 minutes) group by device_id emit changes;
```

- 아래와 같이 신규 데이터를 입력하고 데이터 확인.

```sql
insert into device_status_stream values (1, '2023-02-10T05:22:32.931', 5.2, 13);
insert into device_status_stream values (1, '2023-02-10T05:23:42.931', 5.2, 13);
```

### Window Table의 제약 사항

- key format은 JSON으로 설정.

```sql
--KEY_FORMAT KAFKA로 생성. 
CREATE TABLE device_status_mv01
as
select device_id, from_unixtime(WINDOWSTART) as w_start, from_unixtime(WINDOWEND) as w_end, count(*) as cnt 
from device_status_stream window tumbling (size 1 minutes) group by device_id emit changes;

select * from device_status_mv01 emit changes;

drop table device_status_mv01 delete topic;

-- KEY_FORMAT을 JSON으로 재 생성. 
CREATE TABLE device_status_mv01
with 
(
KAFKA_TOPIC = 'device_status_mv01_topic',
PARTITIONS = 1,
KEY_FORMAT = 'JSON',
VALUE_FORMAT = 'JSON'
)
as
select device_id, from_unixtime(WINDOWSTART) as w_start, from_unixtime(WINDOWEND) as w_end, count(*) as cnt 
from device_status_stream window tumbling (size 1 minutes) group by device_id emit changes;

select * from device_status_mv01 emit changes;

```

- dummy 컬럼값을 이용한 window Group by

```sql
-- window 절에 group by 를 사용하지 않으므로 아래는 오류 발생. 
select from_unixtime(WINDOWSTART) as w_start, from_unixtime(WINDOWEND) as w_end, count(*) as cnt 
from device_status_stream window tumbling (size 1 minutes)  emit changes;

--dummy 값으로 group by 적용
select from_unixtime(WINDOWSTART) as w_start, from_unixtime(WINDOWEND) as w_end, count(*) as cnt 
from device_status_stream window tumbling (size 1 minutes) group by 1 emit changes;

-- CTAS로 만들 경우 group by 컬럼값을 반드시 select 절에 기술해야 함. 
CREATE TABLE device_status_mv02
with 
(
KAFKA_TOPIC = 'device_status_mv02_topic',
PARTITIONS = 1,
KEY_FORMAT = 'JSON',
VALUE_FORMAT = 'JSON'
)
as
select 1 as dummy, from_unixtime(WINDOWSTART) as w_start, from_unixtime(WINDOWEND) as w_end, count(*) as cnt 
from device_status_stream window tumbling (size 1 minutes) group by 1 emit changes;

```

- window가 적용된 table에는 permanent query(ctas) 불가

```sql
-- temporary query는 수행됨. 
select * from device_status_mv01 where device_id = 1;

-- permanent query는 수행 되지 않음. 
create table device_status_mv03 
as
select * from device_status_mv01 where device_id = 1;

```

- timestamp가 입력일시(rowtime)을 가지는 device_status_stream_temp를 생성.

```sql
drop stream if exists device_status_stream_temp delete topic;

CREATE STREAM device_status_stream_temp (
  device_id BIGINT KEY,
  create_ts TIMESTAMP,
  temperature DOUBLE,
  power_watt INT
) WITH (
  KAFKA_TOPIC = 'device_status_stream_temp_topic',
  PARTITIONS = 1,
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'JSON'
);

insert into device_status_stream_temp values (1, '2023-02-10T05:23:32.931', 5.2, 13);
insert into device_status_stream_temp values (1, '2023-02-10T05:23:42.891', 7.4, 17);
insert into device_status_stream_temp values (1, '2023-02-10T05:23:53.288', 4.2, 1);
insert into device_status_stream_temp values (1, '2023-02-10T05:24:22.211', 3.7, 11);
insert into device_status_stream_temp values (1, '2023-02-10T05:24:32.911', 6.8, 9);
-- 1~2 분 후에 입력
insert into device_status_stream_temp values (1, '2023-02-10T05:27:15.244', 3.8, 8);
insert into device_status_stream_temp values (2, '2023-02-10T05:21:19.131', 7.2, 3);
insert into device_status_stream_temp values (2, '2023-02-10T05:21:25.231', 12.4, 22);
insert into device_status_stream_temp values (2, '2023-02-10T05:21:39.531', 15.6, 31);
insert into device_status_stream_temp values (2, '2023-02-10T05:22:00.111', 12.1, 42);
insert into device_status_stream_temp values (2, '2023-02-10T05:22:19.121', 22.7, 19);
insert into device_status_stream_temp values (2, '2023-02-10T05:24:32.333', 16.7, 29);
```

- device_status_stream_temp에 Window table CTAS 수행 후 TOPIC 내용 확인.

```sql
CREATE TABLE device_status_temp_mv01
with 
(
KAFKA_TOPIC = 'device_status_temp_mv01_topic',
PARTITIONS = 1,
KEY_FORMAT = 'JSON',
VALUE_FORMAT = 'JSON'
)
as
select device_id, from_unixtime(WINDOWSTART) as w_start, from_unixtime(WINDOWEND) as w_end, count(*) as cnt 
from device_status_stream_temp window tumbling (size 1 minutes) group by device_id emit changes;

```

- 실습을 위해 생성한 MView 삭제

```sql
drop table device_status_temp_mv01 delete topic;
drop stream device_status_stream_temp delete topic;

drop table device_status_mv01 delete topic;
drop table device_status_mv02 delete topic;

```

### Window 조인 - 01

- Stream-Stream 조인은 조인 시 지정된 Window 기간내에서만 조인 가능.
- 기존 device_status_stream을 재 생성하되 데이터를 변경. 신규 device_master_stream 생성

```sql
drop stream if exists device_status_stream delete topic;

CREATE STREAM device_status_stream (
  device_id BIGINT KEY,
  create_ts TIMESTAMP,
  temperature DOUBLE,
  power_watt INT,
  equip_id VARCHAR
) WITH (
  KAFKA_TOPIC = 'device_status_stream_topic',
  PARTITIONS = 1,
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'CREATE_TS'
);

-- 새로운 stream device_master_stream 생성. 
drop stream if exists device_master_stream delete topic;

CREATE STREAM device_master_stream (
  device_id BIGINT KEY,
  create_ts TIMESTAMP,
  device_name VARCHAR,
  upgrade_type VARCHAR
) WITH (
  KAFKA_TOPIC = 'device_master_topic',
  PARTITIONS = 1,
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'CREATE_TS'
);

--device_master_stream에 데이터 입력 
insert into device_master_stream values (1, '2023-02-10T04:03:32.931', 'Engine Sensor', 'D');
insert into device_master_stream values (2, '2023-02-10T04:05:25.231', 'GPS Sensor', 'D');

--device_status_stream에 데이터 입력. 
insert into device_status_stream values (1, '2023-02-10T05:23:32.931', 5.2, 13, 'A001');
insert into device_status_stream values (2, '2023-02-10T05:23:35.121', 22.7, 19, 'A001');

insert into device_status_stream values (1, '2023-02-10T05:23:42.891', 7.4, 17, 'A001');
insert into device_status_stream values (2, '2023-02-10T05:23:54.333', 16.7, 29, 'A001');

-- within 1 hours는 조인의 선두 stream인 device_status_stream의 과거 1시간 ~ 미래 1시간 이내 기준을 조인 대상
-- Stream-Stream 조인 Mview시 Grace Period를 반드시 명시 필요. 
-- within 2 hours로 변경해서 다시 조인 수행. 
select b.device_id as device_id, b.create_ts as create_ts, b.upgrade_type, 
       a.create_ts as status_ts, a.power_watt
from device_status_stream a
  join device_master_stream b within 1 hours grace period 1 minutes on a.device_id = b.device_id emit changes;
```

- device_master_stream에 아래 데이터를 입력하면서 window join의 within 1~3 hour로 변경하면서 결과 확인.

```sql
-- device_status_stream에 아래 데이터를 입력하면서 window join의 within 1~3 hour로 변경하면서 결과 확인. 
insert into device_master_stream values (1, '2023-02-10T06:03:32.931', 'Engine Sensor', 'R');
insert into device_master_stream values (2, '2023-02-10T06:05:25.231', 'GPS Sensor', 'R');

insert into device_master_stream values (1, '2023-02-10T08:03:32.931', 'Engine Sensor', 'C');
insert into device_master_stream values (2, '2023-02-10T08:05:25.231', 'GPS Sensor', 'C');
```

- device_status_stream 에 아래 데이터를 입력하면서 window join의 within 1~3 hour로 변경하면서 결과 확인.

```sql
insert into device_status_stream values (1, '2023-02-10T05:23:53.288', 4.2, 1, 'A001');
insert into device_status_stream values (2, '2023-02-10T05:24:05.131', 7.2, 3, 'A001');

insert into device_status_stream values (1, '2023-02-10T05:24:22.211', 3.7, 11, 'A001');
insert into device_status_stream values (2, '2023-02-10T05:24:25.231', 12.4, 22, 'A001');

insert into device_status_stream values (1, '2023-02-10T05:24:32.911', 6.8, 9, 'A001');
insert into device_status_stream values (2, '2023-02-10T05:24:39.531', 15.6, 31, 'A001');

insert into device_status_stream values (1, '2023-02-10T05:25:02.244', 3.8, 8, 'A001');
insert into device_status_stream values (2, '2023-02-10T05:25:01.111', 12.1, 42, 'A001');
```

- device_master_stream에 upgrade_type을 ‘P’로 하여 데이터 입력
- device_master_stream의 upgrade_type이 ‘P’ 일때 전후 5분이내 device_status_stream의 데이터 상태 추출.

```sql
select b.device_id, b.create_ts, b.upgrade_type, a.create_ts as status_ts, a.power_watt
from device_master_stream b
  join device_status_stream a within 5 minutes grace period 2 minutes on a.device_id = b.device_id
where b.upgrade_type='P' emit changes;

-- device_master_stream에 upgrade_type이 P 인 데이터 입력. 
insert into device_master_stream values (1, '2023-02-10T05:20:32.931', 'Engine Sensor', 'P');
insert into device_master_stream values (2, '2023-02-10T05:20:25.231', 'GPS Sensor', 'P');
```

- 해당 조건으로 Mview CSAS 생성 및 신규 데이터 입력. .

```sql
-- Stream-Stream CSAS Mview 생성 시 grace period를 지정하지 않으면 제대로 된 결과가 출력되지 않을 수 있음
create stream prod_device_monitor_by5min_stream
as
select b.device_id as device_id, b.create_ts as dev_create_ts, b.upgrade_type, a.create_ts as status_ts, a.power_watt
from device_master_stream b
  join device_status_stream a within 5 minutes grace period 1 minutes on a.device_id = b.device_id
where b.upgrade_type='P' emit changes;

select * from prod_device_monitor_by5min_stream emit changes;

insert into device_status_stream values (2, '2023-02-10T05:25:12.333', 66.7, 109, 'A001');

-- device_master_stream에 upgrade_type이 P 인 데이터 입력. 
insert into device_master_stream values (1, '2023-02-10T05:23:32.931', 'Engine Sensor', 'P');
```

### Window 조인 - 02

- Action 성 Stream과 Stream 조인시 Window 조인의 활용

```sql
drop stream equipment_status_stream delete topic;

create stream equipment_status_stream 
(
  equip_id VARCHAR KEY, 
  create_ts TIMESTAMP,
  status VARCHAR
) WITH (
  KAFKA_TOPIC = 'equipment_status_stream_topic',
  PARTITIONS = 1,
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'CREATE_TS'
);

insert into equipment_status_stream values ('A001', '2023-02-10T05:21:00.000', 'GOOD');
insert into equipment_status_stream values ('A001', '2023-02-10T05:22:00.000', 'GOOD');
insert into equipment_status_stream values ('A001', '2023-02-10T05:23:00.000', 'FAULT');
insert into equipment_status_stream values ('A001', '2023-02-10T05:24:00.000', 'FAULT');
insert into equipment_status_stream values ('A001', '2023-02-10T05:25:00.000', 'FAULT');

-- within 1 minutes 로 equipment_status_stream과 device_status_stream 조인. 
select a.equip_id, a.create_ts as equip_create_ts, a.status, b.device_id, b.create_ts device_ts, b.power_watt
from equipment_status_stream a
  join device_status_stream b within 1 minutes on a.equip_id = b.equip_id
where a.status='FAULT' emit changes;

-- 아래 데이터를 입력하고 위의 조인 결과 모니터링. 
insert into equipment_status_stream values ('A001', '2023-02-10T05:26:00.000', 'FAULT');
insert into equipment_status_stream values ('A001', '2023-02-10T05:27:00.000', 'FAULT');
insert into equipment_status_stream values ('A001', '2023-02-10T05:28:00.000', 'GOOD');
```