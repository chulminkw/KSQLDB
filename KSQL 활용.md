# KSQL 활용

## 주요 SQL 쿼리 수행해보기

- simple_products 스트림을 생성.

```sql
drop stream simple_products delete topic;

CREATE STREAM simple_products (
  product_id VARCHAR KEY,
  product_name VARCHAR,
  category VARCHAR,
  price DECIMAL(10,2),
  reg_date DATE,
  reg_time TIME,
  reg_timestamp TIMESTAMP
) WITH (
  KAFKA_TOPIC = 'simple_products_topic',
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'JSON',
  PARTITIONS = 3
);

INSERT INTO simple_products ( product_id, product_name, category, price, reg_date, reg_time, reg_timestamp ) 
VALUES ( 'p001', 'tea', 'beverages', 2.55, '2023-02-07', '11:36:37', '2023-02-07T11:36:37' );

INSERT INTO simple_products ( product_id, product_name, category, price, reg_date, reg_time, reg_timestamp ) 
VALUES ( 'p002', 'coffee', 'beverages', 2.99, '2023-02-08', '13:26:27', '2023-02-08T13:26:27' );

INSERT INTO simple_products ( product_id, product_name, category, price, reg_date, reg_time, reg_timestamp ) 
VALUES ( 'p003', 'dog', 'pets', 249.99, '2023-02-09', '14:26:27', '2023-02-09T14:26:27'  );

INSERT INTO simple_products ( product_id, product_name, category, price, reg_date, reg_time, reg_timestamp ) 
VALUES ( 'p004', 'cat', 'pets', 195.00, '2023-02-10', '13:36:27', '2023-02-10T13:36:27'  );

INSERT INTO simple_products ( product_id, product_name, category, price, reg_date, reg_time, reg_timestamp ) 
VALUES ( 'p005', 'beret', 'fashion', 34.99, '2023-02-07', '12:36:37', '2023-02-07T12:36:37' );

INSERT INTO simple_products ( product_id, product_name, category, price, reg_date, reg_time, reg_timestamp ) 
VALUES ( 'p006', 'handbag', 'fashion', 126.00, '2023-02-08', '17:36:37', '2023-02-08T17:36:37' );

INSERT INTO simple_products ( product_id, product_name, price) values ('p007', 'pocketmon', 30.00);

SELECT * FROM simple_products;

```

### Projection, Filtering

- projection, table/column alias, where 조건 =, in, like 및 >, between 실습

```sql
--projection
select product_name, category from simple_products;

--table alias
select a.product_name, a.category from simple_products a;

--column alias
select product_name, category as cat_name from simple_products;

-- where 조건 =, in, like
select * from simple_products where category = 'beverages';
select * from simple_products where category in ('beverages', 'pets');
select * from simple_products where product_name like 'c%';

select * from simple_products where price > 10 ;
select * from simple_products where price between 100 and 200;

```

### Limit 절의 사용

- limit 절을 사용하여 SQL 수행 결과를 일부만 출력. limit 절 조건을 만족하면 push 쿼리도 자동으로 종료

```bash
select * from simple_products where category = 'beverages' limit 1;
select * from simple_products where category = 'beverages' emit changes limit 1;
```

### 문자열 concat

- 문자열 concat은 + 를 사용.  || 가 (더이상)제대로 동작하지 않음.  ||은 concat 함수로 대체 가능. 하지만 concat() 함수는 3개 이상의 값을 연결할 때 중첩 형태로 호출이 되어야 해서 불편.

```sql
select product_name+','+category as concat_str from simple_products;

-- 아래는 오류 발생. 
select product_name||category as concat_str from simple_products ;

-- || 는 concat() 함수로 대체됨. 
select concat(product_name, category) as concat_str from simple_products ;

select concat(product_name, concat(',', category)) as concat_str from simple_products;
```

- concat_ws()는 여러개의 문자열을 동일한 seperator로 결합하여 사용.

```sql
select concat_ws(',', product_name, category) as concat_str from simple_products;
```

- 숫자형과 문자형 컬럼값을 문자값으로 concat 할 수 없음.  concat은 문자열만 가능하므로 숫자형은 문자열로 형변환을 해야함.

```sql
select product_name + price from simple_products;
```

### cast() - 컬럼형 변환

- cast를 이용하여 컬럼형을 변환 할 수 있음.  숫자값을 문자열, date/time/timestamp등을 문자열로 변환하는 데 많이 사용. (나중에 조인에서 언급할 예정이지만 join 연결 키 컬럼값은 서로 같은 데이터 타입이여야만 함. 이 경우 다른 한쪽 키 컬럼값의 타입을 변경할 때 cast()를 적용.
- 아래는 cast()를 이용하여 price를 문자열로 형변환 후 문자열 concat 수행.

```sql
select product_name + ':' + cast(price as varchar) as concat_str from simple_products;
```

- 아래는 bigint 타입인 rowtime을 문자열로 변경하여 문자열 concat 수행.

```sql
select '데이터 입력시간:' + cast(rowtime as varchar) as concat_str from simple_products;
```

### ifnull(), coalesce() - Null값 치환

- nullif()와 colalesce() 모두 특정 컬럼이 null일 경우 이를 특정 값으로 치환할 수 있음.  아래와 같이 특정 컬럼이 null이 되도록 데이터 한건을 입력

```sql
insert into simple_products(product_id, product_name, price) values ('p007', 'testprd', 100);

select * from simple_products;
```

- ifnull() 또는 coalesce()를 이용하여 category 컬럼값이 null이면 ‘etc’로 변환하여 출력

```sql
select product_name, ifnull(category, 'etc') as category from simple_products;

select product_name, coalesce(category, 'etc') as category from simple_products ;
```

### Case when 구문

- case when 구문 실습

```sql
select product_name,
       case when category is null then 'etc' 
            else category end as category from simple_products; 

select product_name, price,
       case when price <= 100 then 'LOW'
            when price <= 200 then 'MEDIUM'
            else 'HIGH' end as grade
from simple_products;
```

### Temporal 타입(Date/Time/Timestamp) 변환

- format_date()와 format_timestamp()를 Date와 Timestamp 타입을 문자열로 변환.

```sql
select product_name, format_date(reg_date, 'yyyy-MM-dd') as reg_date_str,
       format_timestamp(reg_timestamp, 'yyyy-MM-dd HH:mm:ss') as reg_timestamp_str
from simple_products;

select product_name, format_date(reg_date, 'MM/dd, yyyy') as reg_date_str,
       format_timestamp(reg_timestamp, 'MM/dd, yyyy HH:mm') as reg_timestamp_str
from simple_products;
```

- parse_date()로 문자열을 Date로 변환

```sql
select product_name, format_date(reg_date, 'MM/dd, yyyy') as reg_date_str,
       parse_date(format_date(reg_date, 'MM/dd, yyyy'), 'MM/dd, yyyy') as reg_date
from simple_products;
```

### Date/Timestamp 타입 값을 int/big int형 Unix Time값으로 변환

- unix_date(Date)는 인자로 들어온 Date 타입의 값을 기반으로 1970-01-01 00:00:00 시각과의 일자를 int 형으로 반환. unix_timestamp(Timestamp)는 인자로 들어온 Timestamp값을 기반으로 1970-01-01 00:00:00.000 시각과의 간격을 millisecond 단위로 반환함.

```sql
SELECT UNIX_DATE(reg_date), unix_timestamp(reg_timestamp) from simple_products;
```

### int/big int형 일자/millisecond초단위 값을 Date/Timestamp 타입 값으로 변환

- from_days(days)는 인자로 들어온 int형의 days를 기준으로(주로 Unix Time값)으로 date로 변환. from_unixtime(milliseconds)는 인자로 들어온 big int형의 milliseconds 단위 값(주로 Unix Time)을 Timestamp로 변환.

```sql
SELECT from_days(UNIX_DATE(reg_date)), reg_date, from_unixtime(unix_timestamp(reg_timestamp)), reg_timestamp from simple_products;
```

### Date/Time/Timestamp 값의 기간 Add/Subtract

- Data/Time/Timestamp값을 Days/Hours/Minutes/Seconds 등의 기간을 더하거나 빼기 위해 DATEADD(), DATESUB() / TIMEADD(), TIMESUB()/TIMESTAMPADD(), TIMESTAMPSUB() 함수를 사용
- add_months()같은 함수가 없는게 아쉽다.

```sql
select product_name, reg_date, dateadd(days, 2, reg_date) as added_date,
       datesub(days, 2, reg_date) as subtracted_date from simple_products;

select product_name, reg_time, timeadd(hours, 2, reg_time) as added_time,
       timesub(hours, 2, reg_time) as substracted_time from simple_products;

select product_name, reg_timestamp, timestampadd(days, 2, reg_timestamp) as added_ts01,
       timestampsub(days, 2, reg_timestamp) as substracted_t2,
       timestampadd(hours, 3, reg_timestamp) as added_ts02 from simple_products;

```

### 현재 일자/시간 표현과 두개의 Date 또는 Timestamp 값 사이의 기간 구하기

- 현재 일자 및 시간 가져 오기. KSQL은 현재일자/시간을 가지는 별도의 속성을 지원하지 않으면 DATE/TIMESTAMP를 UTC 기반의 int/long int로 변환하는 UNIX_DATE(), UNIX_TIMESTAMP() 함수에 아무 인자를 넣지 않았을 때 현재 일자/시간을 int/long int로 가져오고 이를 다시 from_days()/from_timestamp()로 변환해야함. .

```sql
select from_days(unix_date()), from_unixtime(unix_timestamp()) from simple_products;
```

- 두개의 Date 값 사이의 일자 기간 구하기.  KSQLDB는 Interval을 지원하지 않음.  현재일자에서 특정 일자 사이의 기간을 구하기위해서는 unix_date()으로 UTC로 변환된 일자값의 차이를 구하면 됨.

```sql
select product_name, reg_date, unix_date() - unix_date(reg_date) from simple_products ;
```

- 두개의 Timestamp 값 사이의 시간 구하기.  unix_timestamp는 millisecond 단위의 UTC를 반환함.

두 UTC 사이의 차이를 1000으로 나누면 초단위, 다시 이를 3600으로 나누면 시간 단위 기간이 됨.  여기서 다시 24를 추가해서 나누면 일자 단위 기간이 됨. 

```sql
select (unix_timestamp() - unix_timestamp(reg_timestamp))/1000/3600 as interval_hour, unix_timestamp() current_bigint , unix_timestamp(reg_timestamp) as reg_bigint, reg_timestamp from simple_products;

select (unix_timestamp() - unix_timestamp(reg_timestamp))/1000/3600 as interval_hour,(unix_timestamp() - unix_timestamp(reg_timestamp))/1000/3600/24 as interval_days,  unix_timestamp() current_bigint , unix_timestamp(reg_timestamp) as reg_bigint, reg_timestamp from simple_products;
```

### Timestamp의 Timezone 변경 및 Kafka 메시지의 전송/저장 시각 Timezone고찰

- unix_timestamp()의 반환은 기본적으로 UTC를 기준으로 함. 이걸 한국 시간으로 변환하려면 CONVERT_TZ(Timestamp, ‘from timezone’, ‘to timezone’)을 이용해서 변환

```bash
INSERT INTO simple_products ( product_id, product_name, category, price, reg_date, reg_time, reg_timestamp ) 
VALUES ( 'p008', 'shoes', 'fashion', 226.00, '2023-03-13', '17:36:37', '2023-03-13T17:36:37' );

select from_days(unix_date()) as current_date, convert_tz(from_unixtime(unix_timestamp()), 'UTC', 'Asia/Seoul') as current_ts, product_name, reg_date, reg_time from simple_products;

```

- Kafka의 메시지 전송 시 Header에 메시지 전송시간/또는 Kafka Topic 에 기록된 시간에 대한 정보를 Header에 가지고 있음.
- 기본적으로 이 Header에 가지는 메시지의 전송 또는 Topic 기록 시간은 UTC Timezone임. 때문에 한국 시간으로 이를 변환하려면 다시 +9:00를 더해 줘야 함
- 아래 print 토픽명 명령은 Topic에 기록된 메시지 전송시간을 rowtime으로 나타냄

```bash
print simple_products_topic;

select rowtime, a.* from simple_products a;

select rowtime, from_unixtime(rowtime) as rowts, a.* from simple_products a;
select rowtime, convert_tz(from_unixtime(rowtime), 'UTC', 'Asia/Seoul') as rowts, a.* from simple_products a;
```

- 

### Aggregation 함수

### Count()와 Count_distinct( )

- count(*), count(컬럼명) 지원. SQL과 달리 count(distinct 컬럼명)이 아니라 별도의 count_distinct(컬럼명) 함수로 지원.

```sql
select category, count(*) from simple_products group by category emit changes;
```

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