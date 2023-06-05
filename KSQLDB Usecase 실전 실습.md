# KSQLDB Usecase 실전 실습

### 실습 환경 초기화

- 이미 생성되어 있는 Connector 제거

```sql
delete_connector dgen_shoe_customers_avro;
delete_connector dgen_shoes_avro;
delete_connector dgen_shoe_orders_avro;
delete_connector dgen_shoe_clickstream_avro;
delete_connector es_sink_order_metric;
delete_connector es_sink_order_metric_per_mins;

```

- 이미 생성되어 있는 실습용 Table과 Stream을 KSQLDB에서 제거

```sql
-- 분석용 table/stream 삭제
drop table order_metric_per_30mins delete topic;
drop table order_metric_by_state_per_30mins delete topic;
drop table order_metric_by_brand delete topic;
drop table order_cnt_by_state delete topic;

drop table grade_cnt delete topic;
drop table grade_by_customer delete topic;

drop stream shoe_clickstream_enriched delete topic;
drop stream shoe_orders_enriched delete topic;

-- base table/stream 삭제
drop stream shoe_orders delete topic;
drop stream shoe_clickstream delete topic;
drop table shoes delete topic;
drop table shoe_customers delete topic;
```

- ksqldb와 connect를 종료하고, connect 관련 메타 topic을 삭제

```sql
kafka-topics --bootstrap-server localhost:9092 --delete --topic connect-offsets
kafka-topics --bootstrap-server localhost:9092 --delete --topic connect-status
kafka-topics --bootstrap-server localhost:9092 --delete --topic connect-configs

```

- connect와 ksqldb 재 기동

```sql
connect_start.sh
ksql_start.sh
```

### Shoes Clickstream Datagen Connector 생성 및 구동

- Shoes 상품 데이터를 랜덤 생성하는 Connector 생성. dgen_shoe_avro.json에 저장.

```sql
{
  "name": "dgen_shoes_avro",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "shoes_avro",
    "quickstart": "shoes",
    "max.interval": 500,
    "iterations": 1000,
    "tasks.max": "1",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://localhost:8081",
    "value.converter.schema.registry.url": "http://localhost:8081"
  }
}
```

- Shoes  Customer 데이터를 랜덤 생성하는 Connector 생성. dgen_shoe_customers_avro.json에 저장

```sql
{
  "name": "dgen_shoe_customers_avro",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "shoe_customers_avro",
    "quickstart": "shoe_customers",
    "max.interval": 500,
    "iterations": 1000,
    "tasks.max": "1",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://localhost:8081",
    "value.converter.schema.registry.url": "http://localhost:8081"
  }
}
```

- Shoes  Orders 데이터를 랜덤 생성하는 Connector 생성. dgen_shoe_orders_avro.json에 저장.

```sql
{
  "name": "dgen_shoe_orders_avro",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "shoe_orders_avro",
    "quickstart": "shoe_orders",
    "max.interval": 1000,
    "tasks.max": "1",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://localhost:8081",
    "value.converter.schema.registry.url": "http://localhost:8081"
  }
}
```

- Shoes Clickstream 데이터를 랜덤 생성하는 Connector 생성.  dgen_shoe_clickstream_avro.json에 저장.

```sql
{
  "name": "dgen_shoe_clickstream_avro",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "shoe_clickstream_avro",
    "quickstart": "shoe_clickstream",
    "max.interval": 500,
    "tasks.max": "1",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://localhost:8081",
    "value.converter.schema.registry.url": "http://localhost:8081"
  }
}
```

### Shoes Clickstream 관련 Table 및 Stream 생성.

- Shoes Table 생성

```sql
CREATE TABLE shoes (
  product_id varchar primary key,
  brand varchar,
  name varchar,
  sale_price integer,
  rating double
)
WITH (
KAFKA_TOPIC = 'shoes_avro',
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO'
);

select * from shoes emit changes limit 5;
```

- shoe_customers Table 생성

```sql
CREATE TABLE shoe_customers (
  customer_id varchar primary key,
  first_name varchar,
  last_name varchar,
  email varchar,
  phone varchar,
  street_address varchar,
  state varchar,
  zip_code varchar,
  country_code varchar
)
WITH (
KAFKA_TOPIC = 'shoe_customers_avro',
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO'
);

select * from shoe_customers emit changes limit 5;
```

- shoe_orders Stream 생성

```sql
CREATE STREAM shoe_orders (
  order_id int key,
  product_id varchar,
  customer_id varchar,
  ts bigint
)
WITH (
KAFKA_TOPIC = 'shoe_orders_avro',
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO', 
TIMESTAMP = 'ts'
);

select * from shoe_orders emit changes limit 5;
```

- shoe_clickstream Stream 생성

```sql
CREATE STREAM shoe_clickstream (
  stream_id varchar key,
  product_id varchar,
  user_id varchar,
  view_time int,
  page_url varchar,
  ip varchar,
  ts bigint 
)
WITH (
KAFKA_TOPIC = 'shoe_clickstream_avro',
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO', 
TIMESTAMP = 'ts'
);

select * from shoe_clickstream emit changes limit 5;
```

### 실전 실습 - 01

- 고객 별 주문 건수. 고객명별로 주문 건수 구하기. 먼저 shoe_orders와 shoes, shoe_customers 를 조인하여 shoe_orders의 고객과 상품에 대한 추가 속성을 가지는 Join Stream을 생성.

```sql
CREATE STREAM shoe_orders_enriched 
WITH (
KAFKA_TOPIC = 'shoe_orders_enriched_avro',
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO',
PARTITIONS = 1,
TIMESTAMP = 'ts'
)
AS
select a.order_id, a.customer_id as customer_id , c.first_name + ' ' + c.last_name as customer_name,
       a.product_id as product_id, b.brand, b.name as product_name, b.sale_price, b.rating,
       c.email, c.phone, c.street_address, c.state, c.zip_code, c.country_code,
       1 as dummy, a.ts as ts, from_unixtime(a.ts) as ts_format
from shoe_orders a
  join shoes b on a.product_id = b.product_id
  join shoe_customers c on a.customer_id = c.customer_id;

select * from shoe_orders_enriched emit changes limit 3;

describe shoe_orders_enriched extended;
```

- 고객별/고객 거주 state별 주문 건수

```sql
-- 고객별 주문 건수
select customer_id, latest_by_offset(customer_name) as customer_name,
      count(*) as order_cnt
from shoe_orders_enriched group by customer_id emit changes;

--고객 거주 state 별 주문 건수
create table order_cnt_by_state
WITH (
KAFKA_TOPIC = 'order_cnt_by_state',
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO',
PARTITIONS = 1
)
as
select state, 
      count(*) as order_cnt,
      as_value(state) as state_val
from shoe_orders_enriched group by state emit changes;
```

- 제품 brand 별 주문 건수 및 총 판매액/할인 감안 총판매액(소수점 3자리)

```sql
-- brand별 주문 건수
select brand, count(*) as brand_count
from shoe_orders_enriched 
group by brand emit changes;

create table order_metric_by_brand
WITH (
KAFKA_TOPIC = 'order_metric_by_brand',
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO',
PARTITIONS = 1
)
as
select brand, count(*) as brand_count, sum(sale_price) as total_amount,
   round(sum((100- rating)/100 * sale_price), 3) as total_amount_with_rating,
   as_value(brand) as brand_val
from shoe_orders_enriched 
group by brand emit changes;
```

- 매출별 고객 등급 및 전체 등급별 건수

```sql
-- 매출별 고객 등급
create table grade_by_customer
as
select customer_id, latest_by_offset(customer_name) as customer_name,
    sum(sale_price) as total_amount,
    case when sum(sale_price) > 20000 then 'GOLD'
         when sum(sale_price) > 10000 then 'SILVER'
         when sum(sale_price) > 5000 then 'BRONZE'
         else 'STANDARD' end as grade 
from shoe_orders_enriched 
group by customer_id
emit changes;

--전체 등급별 건수
create table grade_cnt
WITH (
KAFKA_TOPIC = 'grade_cnt',
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO',
PARTITIONS = 1
)
as
select grade, count(*) as customer_grade_cnt,
       as_value(grade) as grade_val
from grade_by_customer
group by grade
emit changes;

create table grade_cnt_with_ts
as
select rowtime as ts, * from grade_cnt emit changes;

```

- (특정 Brand를)  5번째마다 제품 구매고객에 대해 free 쿠폰 대상 추출

```sql
SELECT customer_id, latest_by_offset(customer_name) as customer_name,
    COUNT(*) AS total,
    (COUNT(*) % 5) AS sequence,
    (COUNT(*) % 5) = 0 AS next_one_free
FROM shoe_orders_enriched
--WHERE brand = 'Torphy Group'
GROUP BY customer_id 
emit changes;
```

- 고객별 30분 단위 주문 건수, 고객거주 state별 30분 단위 주문 건수,평균 매출액, 총 매출액/ 30분 단위 주문 건수,평균 매출액, 총 매출액

```sql
-- 고객별 30분 단위 주문 건수
select customer_id, latest_by_offset(customer_name) as customer_name,
      count(*) as order_cnt
from shoe_orders_enriched
window tumbling (size 30 minutes)  
group by customer_id emit changes;

-- 30분 단위 state별 주문 건수,평균 매출액, 총 매출액
create table order_metric_by_state_per_30mins
WITH (
KAFKA_TOPIC = 'order_metric_by_state_per_30mins',
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO',
PARTITIONS = 1
)
as
select state, as_value(state) as state_val, from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end, 
       count(*) as order_cnt, avg(sale_price) as avg_amount, sum(sale_price) as total_amount,
       state + cast(windowstart as varchar) as doc_id
from shoe_orders_enriched
window tumbling (size 30 minutes) 
group by state emit changes;

-- 30분 단위 전체 주문 건수,평균 매출액, 총 매출액
create table order_metric_per_30mins
WITH (
KAFKA_TOPIC = 'order_metric_per_30mins',
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO',
PARTITIONS = 1
)
as
select dummy, from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end, 
       count(*) as order_cnt, round(avg(sale_price),2) as avg_amount, sum(sale_price) as total_amount,
       cast(dummy as varchar) + cast(windowstart as varchar) as doc_id
from shoe_orders_enriched
window tumbling (size 30 minutes) 
group by dummy emit changes;
```

### 실전 실습 - 02

- shoe_clickstream과 shoes, shoe_customers 조인 Stream 생성.

```sql
CREATE STREAM shoe_clickstream_enriched 
WITH (
KAFKA_TOPIC = 'shoe_clickstream_enriched_avro',
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO',
PARTITIONS = 1,
TIMESTAMP = 'ts'
)
AS
select a.stream_id, a.product_id as product_id, a.user_id as user_id, a.view_time,
       a.page_url, a.ip, 
       b.brand, b.name as product_name, b.sale_price, b.rating,
       c.first_name + ' ' + c.last_name as customer_name,
       c.state, c.country_code,
       1 as dummy, a.ts as ts, from_unixtime(a.ts) as ts_format
from shoe_clickstream a
   join shoes b on a.product_id = b.product_id
   join shoe_customers c on a.user_id = c.customer_id
emit changes;
```

- 30분 마다 page 별 클릭수

```sql
select page_url,
       from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end, 
       count(*) as page_cnt
from shoe_clickstream a
window tumbling (size 30 minutes)
group by page_url
emit changes;
```

```sql
-- page_url은 datagen에서 거의 고유하게 생성됨. 
select page_url, count(*) as page_cnt
from shoe_clickstream a
group by page_url
having count(*) > 1
emit changes;
```

- 30분 주기 product별 클릭수. 30분간 brand 별 클릭수가 10회 이하인 brand 추출

```sql
select product_id, latest_by_offset(product_name) as product_name,
       from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end, 
       count(*) as page_cnt
from shoe_clickstream_enriched a
window tumbling (size 30 minutes)
group by product_id
emit changes;

select brand, 
       from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end, 
       count(*) as page_cnt
from shoe_clickstream_enriched a
window tumbling (size 30 minutes)
group by brand
having count(*) <= 10
emit changes;
```

- 30분 주기 클릭수(tumbling window와 hopping window)

```sql
select dummy,
       from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end, 
       count(*) as page_cnt
from shoe_clickstream_enriched a
window tumbling (size 30 minutes)
group by dummy
emit changes;

select dummy,
       from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end, 
       count(*) as page_cnt
from shoe_clickstream_enriched a
window hopping (size 30 minutes, advance by 1 minutes )
group by dummy
emit changes;

```

- 30분 마다 brand별 클릭 수 및 top 5 가격

```sql

select brand, 
       from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end, 
       count(*) as page_cnt, topk(sale_price, 5) as top5_prices
from shoe_clickstream_enriched a
window tumbling (size 30 minutes)
group by brand
emit changes;
```

- 30분 마다 product별 평균 view_time이 30 이상인 데이터 추출

```sql

select product_id, latest_by_offset(product_name) as product_name, 
       from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end, 
       avg(view_time) as avg_view_time
from shoe_clickstream_enriched a
window tumbling (size 30 minutes)
group by product_id
having avg(view_time) > 30
emit changes;
```

- 사용자별 세션당 접속 건수. 세션의 기준은 40분간 click이 없는 경우 하나의 세션으로 설정.

```sql
SELECT
    user_id,
    latest_by_offset(customer_name) as customer_name,
    from_unixtime(windowstart) as w_start, from_unixtime(windowend) as w_end,
    COUNT(*) AS page_count
FROM shoe_clickstream_enriched 
WINDOW SESSION (40 minutes)
GROUP BY user_id
emit changes;
```

### KSQLDB와 Elasticsearch 연동 및 Kibana 시각화 실전 실습

- order 주문과 매출액 관련 분석 Table을 연동하는 Elasticsearch sink connector 생성. es_sink_order_metric.json으로 저장.

```sql
{
    "name": "es_sink_order_metric",
    "config": {
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "tasks.max": "1",
        "topics": "order_cnt_by_state, order_metric_by_brand, grade_cnt",
        "connection.url": "http://192.168.56.101:9200",
        "connection.username": "es_connect_dev",
        "connection.password": "es_connect_dev",

        "key.ignore": "false",
        "schema.ignore": "false",

        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": "http://localhost:8081",
        "value.converter.schema.registry.url": "http://localhost:8081"

    }
}
```

- 30분 주기 order 주문과 매출액 관련 분석 Table을 연동하는 Elasticsearch sink connector 생성. es_sink_order_metric_per_mins.json으로 저장.

```sql
{
    "name": "es_sink_order_metric_per_mins",
    "config": {
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "tasks.max": "1",
        "topics": "order_metric_by_state_per_30mins, order_metric_per_30mins",

        "connection.url": "http://192.168.56.101:9200",
        "connection.username": "es_connect_dev",
        "connection.password": "es_connect_dev",

        "key.ignore": "false",
        "schema.ignore": "false",

        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": "http://localhost:8081",
        "value.converter.schema.registry.url": "http://localhost:8081",

        "transforms": "create_key, extract_key",
        "transforms.create_key.type": "org.apache.kafka.connect.transforms.ValueToKey",
        "transforms.create_key.fields": "DOC_ID",
        "transforms.extract_key.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
        "transforms.extract_key.field": "DOC_ID"
    }
}
```
