# KSQLDB Group by와 MView 이해

- 새로운 stream인 customer_activity_stream 을 생성하고 데이터 입력. partitions=3 으로 설정.

```sql
CREATE STREAM customer_activity_stream (
    CUSTOMER_ID INTEGER KEY,
    ACTIVITY_SEQ INTEGER,
    ACTIVITY_TYPE VARCHAR,
    ACTIVITY_POINT DOUBLE
   ) WITH (
    KAFKA_TOPIC = 'customer_activity_topic',
    KEY_FORMAT = 'KAFKA',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 3
);

INSERT INTO customer_activity_stream (customer_id, activity_seq, activity_type, activity_point) VALUES (1, 1,'branch_visit',0.4);
INSERT INTO customer_activity_stream (customer_id, activity_seq, activity_type, activity_point) VALUES (2, 1,'web_open',0.43);
INSERT INTO customer_activity_stream (customer_id, activity_seq, activity_type, activity_point) VALUES (2, 2, 'deposit',0.56);
INSERT INTO customer_activity_stream (customer_id, activity_seq, activity_type, activity_point) VALUES (3, 3,'web_open',0.33);
INSERT INTO customer_activity_stream (customer_id, activity_seq, activity_type, activity_point) VALUES (1, 4,'deposit',0.41);
INSERT INTO customer_activity_stream (customer_id, activity_seq, activity_type, activity_point) VALUES (2, 5,'deposit',0.44);
INSERT INTO customer_activity_stream (customer_id, activity_seq, activity_type, activity_point) VALUES (1, 7, 'mobile_open', 0.97);
INSERT INTO customer_activity_stream (customer_id, activity_seq, activity_type, activity_point) VALUES (2, 8,'deposit',0.83);
INSERT INTO customer_activity_stream (customer_id, activity_seq, activity_type, activity_point) VALUES (4, 1,'mobile_open',0.33);
```

- stream 조회 및 topic 내용 조회

```sql
select * from customer_activity_stream;

set 'auto.offset.reset'='earliest';

print customer_activity_stream;

```

### Aggregation과 Group by의 적용

- Stream/Table의 aggregation/group by 연산은 rocksdb에서 수행이 됨.
- aggregate 함수를 사용시 반드시 group by와 함께 적용되어야 함. 전체 테이블에 group by 미 적용 후 aggregate 함수 적용은 허용하지 않음.

```sql
-- 아래는 count(*) 시에 group by 를 적용하지 않았으므로 수행 오류
select count(*) as cnt from customer_activity_stream emit changes;

select count(*) as cnt from customer_activity_stream group by 1 emit changes;

```

- 아래와 같이 group by 와 aggregation 함수 수행.

```sql
SELECT ACTIVITY_TYPE, COUNT(*) AS CNT, SUM(ACTIVITY_POINT) AS SUM_P,
, AVG(ACTIVITY_POINT) AS AVG_P, MAX(ACTIVITY_POINT) AS MAX_P, MIN(ACTIVITY_POINT) AS MIN_P FROM CUSTOMER_ACTIVITY_STREAM GROUP BY ACTIVITY_TYPE EMIT CHANGES;

```

### Stream또는 Table에 따라 제약되는 Aggregation 함수

- 아래와 같이 기존 simple_products_topic을 기반으로 새로운 Table인 simple_product_table 생성.

```bash
CREATE table simple_products_table (
  product_id VARCHAR PRIMARY KEY,
  product_name VARCHAR,
  category VARCHAR,
  price DECIMAL(10,2),
  reg_date DATE,
  reg_time TIME,
  reg_timestamp TIMESTAMP
) WITH (
  KAFKA_TOPIC = 'simple_products_topic',
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'JSON'
);

```

- 특정 aggregation 함수는 테이블에서 수행되지 않음.

```bash
select count(*) as cnt, count_distinct(category) as cnt_cat from simple_products_table group by 1 emit changes;
```

### Group by시 Rocksdb의 사용.

- Stream/Table의 aggregation/group by 연산은 rocksdb에서 수행이 됨.
- Stream에 ksql로 group by 를 사용하면 임시 내부 토픽으로 -xxx-GroupBy-repartion과-xxx-Aggregate-Materialize-changelog이 생성됨. 개별 내부 토픽은 stream이 가지는 파티션 수와 동일한 파티션을 가지게 됨.  rocksdb역시 aggregation/group by 를 위해 동작.

```sql
cd data/kafka-logs
ls -lrt
cd /data/kafka-streams
ls -lrt
```

- ksql에 aggregation 함수를 적용 시 group by 를 적용하는 이유는  aggregation의 결과로 단순한 scalar 값을 반환하는 것을 제한하기 위함으로 추정(뇌피셜 ^^).  
또한 stream/table에 aggregation을 적용하기 위해서는 rocksdb 를 이용해야 함(stateful 연산).  하지만 rocksdb는 create stream/table as 로 생성된 stream/table이 아닌 topic 기반의 stream/table에서는 pull query로 바로 동작하지 않음.  pull query의 특성상 쿼리를 수행하고 바로 자원을 반환하게 되면 rocksdb를 너무 빈번하게 사용하면서 자원을 많이 사용하게 될까봐 막아놓은 것으로 추정(뇌피셜 ^^).

### Group by의 적용

- group by 적용

```sql
select activity_type, count(*) as cnt from customer_activity_stream group by activity_type emit changes;

SELECT ACTIVITY_TYPE, COUNT(*) AS CNT, SUM(ACTIVITY_POINT) AS SUM_P,
       AVG(ACTIVITY_POINT) AS AVG_P, MAX(ACTIVITY_POINT) AS MAX_P, MIN(ACTIVITY_POINT) AS MIN_P FROM CUSTOMER_ACTIVITY_STREAM GROUP BY ACTIVITY_TYPE EMIT CHANGES;
```

- 다른 ksql cli 창을 열고 customer_activity_stream 에 새로운 데이터를 입력 한 뒤 기존의 push 쿼리에 새로운 결과가 반영되었는지 확인.

```sql
INSERT INTO customer_activity_stream (customer_id, activity_id, activity_type, activity_point) VALUES (3, 9, 'deposit',0.86);
```

- 임시 repartition내부 토픽과 changelog 내부 토픽 확인.  key가 아닌 컬럼으로 group by 시에는 repartition 내부 토픽이 생성됨.

```sql
cd data/kafka-log
ls -lrt
```

- Group by 를 key로 할 경우 repartition내부 토픽은 생성되지 않음.

```sql
select customer_id, count(*) as cnt from customer_activity_stream group by customer_id emit changes;
```

- 내부 토픽 생성 확인.

```sql
cd data/kafka-log
ls -lrt
```

### Group by 절에 여러개의 컬럼들이 있을 경우

- Group by 절에 기술된 컬럼은 Group by 로 수행 되는 SQL 결과 집합의 PK가 됨.  이때 여러개의 컬럼들로 PK가 될 수 있음.

### 가공된 Group by 절의 사용

- group by 절을 가공된 컬럼값으로 적용할 경우 KSQL 출력상으로는 잘못된 값이 출력 되는 것으로 보일 수 있지만 CTAS로 MVIEW를 생성하여 확인하면 정상 출력 됨.

```sql
select case when category is null then 'etc' else category end as category,
       count(*) as cnt from simple_products 
group by case when category is null then 'etc' else category end emit changes;
```

- CTAS로 MVIEW생성하여 출력 확인

```sql
create table category_count_mv01 
as
select case when category is null then 'etc' else category end as category,
       count(*) as cnt from simple_products 
group by case when category is null then 'etc' else category end emit changes;
```

### Materialized View의 적용 - Stream.

- Materialized View는 소스 Stream/Table을 가공/변환/필터링을 통한 데이터 중간 처리, 그리고 Group by/Join등을 통한 중간(또는 최종) 레벨의 집계 테이블을 생성하고 이를 Topic으로 저장하는 용도로 사용됨. MView는 KSQLDB에서 매우 활용도가 높음.
- Create table as 명령어로 Materialzed View 생성. Materialized View는 Stream 또는 Table 형태로 생성 될 수 있으며, Create Stream as Select , 또는 Create Table as Select 와 같은 명령어 형식으로 생성될 수 있음.
- Materialized view 생성시 select 절에 emit changes를 명시적으로 붙여 주는게 Stream/Table Pipeline을 기반으로 하고 있는 Materialized View의 의미를 더 강조 할 수 있음.  emit changes 절을 생략해도 Mview는 Source Stream/Table의 데이터 변경 사항을 자동으로 반영
- 아래와 같이 Materialized view를 CSAS로 생성함.

```sql
create stream customer_activity_stream_mv_01
as
select * from customer_activity_stream where activity_type in ('web_open', 'mobile_open') emit changes;
```

- 새로운 데이터를 customer_activity_stream으로 입력하고 MView가 해당 데이터를 반영하는 지 확인.  Mview 생성 시 CSAS절에 emit changes을 적용하더라도 이 Mview를 Select 시에 Push Query로 호출하고자 한다면 반드시 Select Mview emit changes; 와 같이 호출해 줘야함.

```sql
INSERT INTO customer_activity_stream (customer_id, activity_id, activity_type, activity_point) VALUES (2, 10,'mobile_open',0.65);

select * from customer_activity_stream_mv_01;

select * from customer_activity_stream_mv_01 emit changes;
```

- emit changes를 제외하고 Materialized View를 생성하고 새로운 데이터를 customer_activity_stream에 입력후 Mview 데이터 확인.

```sql
create stream customer_activity_stream_mv_02
as
select * from customer_activity_stream where activity_type in ('web_open', 'mobile_open');

INSERT INTO customer_activity_stream (customer_id, activity_id, activity_type, activity_point) VALUES (2, 11,'web_open',0.45);

select * from customer_activity_stream_mv_02 emit changes;
```

- CSAS로 Mview가 생성되면 새로운 Stream과 Topic이 함께 생성됨. CSAS에 with 절로 MView의 topic 생성 속성을 별도로 지정하지 않으면 Topic명은 Mview의 이름을 그대로 적용하며(대문자로 Topic명을 저장) Topic의 key/value format과 Partition 갯수는 Source Stream Topic의 속성을 그대로 가져감.
- 아래는 MView생성시 with절로 별도의 Topic 속성을 지정.

```sql
drop stream customer_activity_stream_mv_02 delete topic;

create stream customer_activity_stream_mv_02
with (
	KAFKA_TOPIC='CUSTOMER_ACTIVITY_TOPIC',
	VALUE_FORMAT='JSON',
	PARTITIONS=3
)
as
select * from customer_activity_stream where activity_type in ('web_open', 'mobile_open');

SHOW TOPICS;
```

- MView의 토픽으로 CUSTOMER_ACTIVITY_TOPIC이 생성되었는지 확인

```sql
cd data/kafka-logs
ls -lrt
```

- CSAS로 Mview가 생성되면 새로운 Stream과 Topic외에 Query Object가 별도로 생성됨. 개별 CSAS/CTAS Query들은 별도의 Query ID로 관리됨.

```sql
show streams;
show topics;

show queries;
```

- CSAS/CTAS로 하나의 MView를 생성하면 반드시 Sink가 되는 Topic이 생성됨.   Topic으로 저장하지 않고 1차 transformation의 대상으로만 CSAS를 만들고 2차 Transformation에서 다시 이 CSAS를 활용하기 위한 용도로 활용하더라도 개별 MView는 무조건 Sink Topic으로 메시지가 저장됨.  MView는 영구적으로(Persistent)하게 데이터를 저장하게 됨.
- 아래와 같이 customer_activity_stream_03을 customer_activity_stream_04를 위한 추출 대상 용도로만 사용하더라도 customer_activity_stream_03은 sink용 Topic을 가져야 함.

```sql
create stream customer_activity_stream_mv_03
as
select * from customer_activity_stream where activity_type in ('web_open', 'mobile_open');

create stream customer_activity_stream_mv_04
as
select customer_id, activity_point from customer_activity_stream_mv_03 where activity_type in ('web_open', 'mobile_open');

select * from customer_activity_stream_mv_04;

show topics;
```

### MView 생성 시 유의 사항.

- CTAS/CSAS 적용 시 set ‘auto.offset.reset’ = ‘earliest’를 적용하지 않은 상태에서 MVIEW를 만들 때는 참조하는 Stream/Table의 기존 데이터를 이용하지 못하고 신규로 입력되는 데이터 만을 가지고 만들므로 데이터가 아예 없을 수 있음.

### Materialized View의 적용 - Table

- Table도 Stream과 동일하게 CTAS로 Mview로 생성하며, 생성된 MView는 pull 쿼리, Push 쿼리 모두 수행 가능.  Table을 기반으로 만들어진 MView를 생성 시에는 무조건 rocksdb가 가동되므로 pull 쿼리 수행이 가능.

```sql
show tables;

-- table기반으로 MView 생성. 
create table simple_products_mv01
as
select * from simple_products where category='pets';

show tables;

-- 토픽을 direct 소스로 하는 Table에 pull query를 수행 할 수 없음. 
select * from simple_products;

-- CTAS로 만들어진 Mview에는 pull query도 수행 가능
select * from simple_products_mv01;

select * from simple_products_mv01 emit changes;
```

### Materialized View의 적용 - CSAS/CTAS에 group by 적용.

- group by를 수행하면 group by 절에 기술된 컬럼값으로 유일한값(unique) 레벨로 데이터가 만들어짐. Stream 기반으로 Group by가 적용된 Mview를 생성할 시 group by 컬럼값으로 unique한 레벨의 집합이 만들어 지게 되므로 최종 생성되는 MView는 Table로 만들어 져야함.  따라서 이 경우 명확하게 CSAS가 아니라 CTAS, 즉 생성된 MView가 Stream이 아니라 Table로 만들어 지도록 명령어 작성.
- 이렇게 Table로 만들어 질 수 있는 집합을 명확하게 Table로 명시하여 생성하는 것은 전체 data application 차원에서 더 나은 선택임(추후 join시 명확한 집합 레벨이 적용되어야 함)

```sql
-- 아래는 CSAS로 Stream을 Group by 로 적용하여 MView를 생성하려 했기에 오류 발생. 
create stream activity_stream_grp_mv_00
as
select customer_id, count(*) as cnt, avg(activity_point) avg_point from customer_activity_stream
group by customer_id emit changes;

-- Group를 적용하여 MView 생성 시 반드시 CTAS로 생성 해야함. 
create table activity_stream_grp_mv_01
as
select customer_id, count(*) as cnt, avg(activity_point) avg_point from customer_activity_stream
group by customer_id emit changes;
```

- CTAS로 생성된 MView에는 Push 쿼리, Pull 쿼리 모두 수행 가능

```sql
select * from activty_stream_grp_mv_01;

select * from activity_stream_grp_mv_01 emit changes;
```

- Group by 기반의 MView 생성 시 rocksdb가 가동됨. rocksdb는 groupby를 수행하면 무조건 실행됨. 내부 토픽으로 XXX-SystemID+Query_id+Aggregate-Materialize-changelog이 생성됨. group by 를 key컬럼(table일 경우 primary key)로 수행하지 않으면 repartition 내부 토픽도 함께 생성됨.

```sql
cd data/kafka-logs
ls -lrt
```

### MView의 Source Stream/Table 삭제 시 유의 사항

- MView CSAS/CTAS의 기반인 Stream/Table을 삭제할 경우는 이를 참조하고 있는 MVIEW도 먼저 삭제가 되어야 함.
- describe stream/table extended로 해당 stream/table을 기반으로 하고 있는 MView를 확인 할 수 있음.

```sql
-- customer_activity_stream을 source로 하는 여러 mview가 있기 때문에 아래 SQL을 수행 불가. 
drop stream customer_activity_stream delete topic;

describe customer_activity_stream extended;
```

- customer_activity_stream을 소스로 하는 mview들을 모두 drop 후 customer_activity_stream drop

```sql
drop stream CUSTOMER_ACTIVITY_STREAM_MV_01 delete topic;
drop stream CUSTOMER_ACTIVITY_STREAM_MV_02 delete topic;

-- CUSTOMER_ACTIVITY_STREAM_MV_03은 CUSTOMER_ACTIVITY_STREAM_MV_04가 참조하고 있기때문에 삭제 안됨. 
drop stream CUSTOMER_ACTIVITY_STREAM_MV_03 delete topic;

drop stream CUSTOMER_ACTIVITY_STREAM_MV_04 delete topic;

drop stream CUSTOMER_ACTIVITY_STREAM_MV_03 delete topic;

drop table  ACTIVITY_STREAM_GRP_MV_01 delete topic;

drop stream customer_activity_stream delete topic;
```

- Stream/Table이 소스/타겟으로 하고 있는 topic을 삭제하면 Stream/Table이 정상 동작하지 않음.

### Limit 절의 사용

- limit 절은 출력 레코드의 건수를 제한함. limit절을 이용하면 특정 건수만큼의 레코드만 출력하므로 push 쿼리라도 해당 건수의 레코드를 출력했으면 바로 종료됨.

```sql
select * from customer_activity_stream emit changes limit 1;
```

### Group by절에 여러개의 컬럼들이 있을 경우

- Group by CTAS를 이용하여 Table기반의 MView를 만들때 Group by 절에 기술된 컬럼들은 자동으로 MView의 PK가 됨.  이때 Group by 절에 여러개의 컬럼들이 있을 경우 KEY_FORMAT을 KAFKA가 아닌 JSON또는 AVRO로 지정해 줘야함. 여러개의 PK 컬럼들은 KAFKA와 같은 Primitive 타입이 될 수 없기 때문임. 따라서 CTAS의 WITH절에 KEY_FORMAT=’JSON’과 같이 명시적으로 지정을 해줘야 함.

```sql
--아래는 여러개의 컬럼들로 group by를 CTAS 적용하지만, 
--KEY_FORMAT을 명시적으로 설정하지 않아 기본값인 KAFKA가 여러개의 컬럼들을 PK로 만들수 없어서 오류 발생. 
create table activity_stream_grp_mv_05
as
select customer_id, activity_type, count(*) as cnt from customer_activity_stream group by customer_id, activity_type;

-- 아래는 KEY_FORMAT을 명시적으로 JSON으로 지정. 
create table activity_stream_grp_mv_05 WITH ( KEY_FORMAT='JSON' )
as
select customer_id, activity_type, count(*) as cnt from customer_activity_stream group by customer_id, activity_type;

print ACTIVITY_STREAM_GRP_MV_05 from beginning;

```
