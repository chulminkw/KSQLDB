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
  device_id BIGINT,
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
  device_id BIGINT,
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
  device_id BIGINT,
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
  device_id BIGINT,
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

### Window 설정

- event가 발생하는 기간(interval)을 window 형태로 지정하여 지정된 window별로 event에 대한 연산 수행 가능.
- stream/table에 window를 적용하면 기본적으로 해당 stream/table에 설정된 메타 timestamp 값에 따라 window가 설정됨.
- 아래와 같이 샘플 stream을 생성.

```sql
drop stream if exists device_status_stream delete topic;

CREATE STREAM device_status_stream (
  device_id BIGINT,
  create_ts TIMESTAMP,
  temperature DOUBLE,
  power_watt INT
) WITH (
  KAFKA_TOPIC = 'device_status_stream',
  PARTITIONS = 3,
  KEY_FORMAT = 'JSON',
  VALUE_FORMAT = 'JSON'
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

select rowtime, * from device_status_stream;

select from_unixtime(rowtime) as rowtime_ts, a.* from device_status_stream a;
```

- window는 여러가지 유형이 있으며, 가장 많이 사용되는 window는 tumbling window임.  window는 group by와 함께 사용되어야 하며 때문에 push 쿼리만 가능.

```sql
select device_id, count(*) from device_status_stream window tumbling(size 1 seconds) group by device_id emit changes;

select device_id, count(*), windowstart, windowend from device_status_stream window tumbling(size 1 seconds) group by device_id emit changes;

select device_id, count(*), min(rowtime), max(rowtime), windowstart, windowend from device_status_stream window tumbling(size 1 seconds) group by device_id emit changes;

select device_id, count(*), from_unixtime(min(rowtime)), from_unixtime(max(rowtime)), from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end from device_status_stream window tumbling(size 1 seconds) group by device_id emit changes;
```

- device_status_stream에 레코드 insert시 한번에 다 적용을 했기 때문에 window size가 1초라도 한번에 모든 데이터가 해당 window에 포함될 수 있음. 이번에 insert시 rowtime간의 간격을 좀 더 길게 하면서 데이터 입력

```sql
drop stream if exists device_status_stream delete topic;

CREATE STREAM device_status_stream (
  device_id BIGINT,
  create_ts TIMESTAMP,
  temperature DOUBLE,
  power_watt INT
) WITH (
  KAFKA_TOPIC = 'device_status_stream',
  PARTITIONS = 3,
  KEY_FORMAT = 'JSON',
  VALUE_FORMAT = 'JSON'
);

-- 아래를 수행시 한 건씩 시간 간격을 두면서 insert 수행. 
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

- window 기간(interval)을 5초로 설정하고 다시 조회

```sql
select device_id, count(*), from_unixtime(min(rowtime)), from_unixtime(max(rowtime)), from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end from device_status_stream window tumbling(size 1 seconds) group by device_id emit changes;
```

### Custom Timestamp의 설정

- 메타성 timestamp는 특별히 지정하지 않으면 데이터 입력 시각이 할당됨. 이 timestamp 시간을 Stream/table의 컬럼을 지정하여 설정할 수 있음. device_status_stream의 create_ts는 해당 event가 소스 시스템에서 발생한 시간임. 이를 timestamp로 지정함.

```sql
drop stream if exists device_status_stream delete topic;

-- WITH절에 TIMESTAMP 속성을 CREATE_TS 컬럼으로 지정. 
CREATE STREAM device_status_stream (
  device_id BIGINT,
  create_ts TIMESTAMP,
  temperature DOUBLE,
  power_watt INT
) WITH (
  KAFKA_TOPIC = 'device_status_stream',
  PARTITIONS = 3,
  KEY_FORMAT = 'JSON',
  VALUE_FORMAT = 'JSON',
  TIMESTAMP = 'CREATE_TS'
);

-- 아래는 CREATE_TS가 VARCHAR 컬럼일 때 TIMESTAMP FORMAT으로 적용함. ''T''와 같이 T를 Escape하기 위해 두개의 single quote 적용 필요. 
drop stream if exists device_status_stream delete topic;

CREATE STREAM device_status_stream (
  device_id BIGINT,
  create_ts VARCHAR,
  temperature DOUBLE,
  power_watt INT
) WITH (
  KAFKA_TOPIC = 'device_status_stream',
  PARTITIONS = 3,
  KEY_FORMAT = 'JSON',
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
```

- create_ts 컬럼이 timestamp로 지정되어 있음을 확인하고 rowtime 컬럼과 create_ts 컬럼 확인.

```sql
-- create_ts 컬럼이 timestamp로 지정되어 있음을 확인. 
describe device_status_stream extended;

-- rowtime이 create_ts와 동일함. 
select from_unixtime(rowtime) as event_ts, a.* from device_status_stream a;
```

- window size를 변경하면서 쿼리 수행.

```sql
-- 10초 간격 window size
select device_id, count(*) as cnt, from_unixtime(min(rowtime)), from_unixtime(max(rowtime)), 
	from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end 
from device_status_stream window tumbling(size 10 seconds) group by device_id emit changes;

-- 30초 간격 window size
select device_id, count(*) as cnt, from_unixtime(min(rowtime)), from_unixtime(max(rowtime)), 
	from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end 
from device_status_stream window tumbling(size 30 seconds) group by device_id emit changes;

-- 1분 간격 window size 
select device_id, count(*) as cnt, from_unixtime(min(rowtime)), from_unixtime(max(rowtime)), 
	from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end 
from device_status_stream window tumbling(size 1 minutes) group by device_id emit changes;

-- max(create_ts)는 max()함수가 varchar/timestamp 를 지원하지 않으므로 수행 불가. 
select device_id, count(*) as cnt, min(unix_timestamp(create_ts)) as create_ts_start, max(unix_timestamp(create_ts)) as create_ts_end, 
	from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end, latest_by_offset(create_ts) as latest_ts
from device_status_stream window tumbling(size 1 minutes) group by device_id emit changes;
```

- 만약 device_status_stream의 데이터가 주기적으로 발생하지 않고 delay가 발생하는 지 확인하고자 할 경우

```sql
-- 개별 device_id별 건수와 window size내에서 delay 정보를 table로 생성.
create table device_status_tab_by_id
as
select device_id, count(*) as cnt, from_unixtime(min(rowtime)) as create_ts_start, from_unixtime(max(rowtime)) as create_ts_end,
	from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end, 
	latest_by_offset(create_ts) as latest_create_ts,
	windowend - max(unix_timestamp(create_ts)) as max_delay_ms
from device_status_stream window tumbling(size 1 minutes) group by device_id emit changes;

-- 최대 delay가 30초 이상인 데이터 추출. 
select device_id, cnt, w_start, w_end, latest_create_ts, max_delay_ms
from device_status_tab_by_id where max_delay_ms > 30000;
```
