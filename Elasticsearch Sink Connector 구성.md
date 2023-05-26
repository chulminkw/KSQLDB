# Elasticsearch Sink Connector 구성

### ElasticSearch 설치

- ElasticSearch의 Public GPG Key를 Linux APT로 이전

```sql
curl -fsSL https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elastic.gpg
```

- ElasticSearch 7.x Source List를 apt에서 참조 할 수 있도록 등록

```sql
echo "deb [signed-by=/usr/share/keyrings/elastic.gpg] https://artifacts.elastic.co/packages/7.x/apt stable main" | sudo tee -a /etc/apt/sources.list.d/elastic-7.x.list
```

- ElasticSearch 7.x 설치

```sql
sudo apt update
sudo apt install elasticsearch
```

- sudo su - 로 root 권한으로 /etc/elasticsearch/elasticsearch.yml 파일 설정 변경

```sql
node.name: node-1
network.host: 192.168.56.101
discovery.seed_hosts: ["192.168.56.101"]
cluster.initial_master_nodes: ["node-1"]
xpack.security.enabled: true
xpack.security.transport.ssl.enabled: true
```

- ElasticSearch 기동

```sql
sudo systemctl start elasticsearch
sudo systemctl status elasticsearch
# VM 기동 시 마다 자동 기동
sudo systemctl enable elasticsearch
```

- ElasticSearch가 설치되면 /usr/share/elasticsearch 디렉토리에 bin, lib, jdk 등의 서브 디렉토리등으로 설치됨.

```sql
cd /usr/share/elasticsearch
ls
```

### Kibana 설치 및 환경 구성.

- ElasticSearch 7.x Source List를 apt에서 참조 할 수 있도록 등록되어 있으면 kibana도 7.x로 설치됨.

```sql
sudo apt update
sudo apt install kibana
```

- /etc/elasticsearch/elasticsearch.yml 파일에서 xpack.security.enabled: true 로 설정 재확인

```sql
xpack.security.enabled: true
```

- elasticsearch-setup-passwords를 이용하여 built in 사용자의 password 설정.

```sql
sudo /usr/share/elasticsearch/bin/elasticsearch-setup-passwords interactive
```

- elasticsearch-setup-passwords 명령어는 단 한번밖에 수행되지 않음. 만약에 passoword를 재 설정하고자 할 경우에는 다시 원래 bootstrap password를 적용한 후 elasticsearch-setup-passwords를 수행해야함. bootstrap password를 적용하는 방법은 먼저 elasticsearch와 kibana를 down시킨 후 아래 명령어를 적용. 적용 후 elasticsearch-setup-passwords를 재 적용함.

```yaml
bin/elasticsearch-keystore add "bootstrap.password"
```

- /etc/kibana/kibana.yml config 설정.

```yaml
server.host: "192.168.56.101"
elasticsearch.hosts: ["http://192.168.56.101:9200"]
elasticsearch.username: "kibana_system"
elasticsearch.password: "elastic"
elasticsearch.ssl.verificationMode: none
```

- elasticsearch와 kibana를 재 기동후 kibana 접속

```sql
sudo systemctl restart elasticsearch kibana
```

- 로컬 브라우저에서 [http://192.168.56.101:5601](http://192.168.56.101:5601)로 kibana login 페이지에 접속하여 username을 elastic, password를 elastic built-in 사용자의 password를 입력한 뒤 로그인 함.

### Kibana에서 Elasticsearch sink connector용 role과 user 생성하기

- kibana의 dev tools을 이용하여 role을 생성. 왼쪽 메뉴의 Management→ Dev Tools를 선택하고 왼쪽 clip 보드에 아래를 입력하고 실행 수행. 
es_sink_connector_role 이라는 이름으로 신규 role을 생성하고 create_index, read, write, view_index_metadata를 모든 index에서 수행할 수 있는 privileges 할당.

```sql
POST /_security/role/es_sink_connector_role
{
  "indices": [
    {
      "names": [ "*" ],
      "privileges": ["create_index", "read", "write", "view_index_metadata"]
    }
  ]
}'
```

- Role 관리 화면에서도 수행 가능. Management → Stack Management → Security → Roles 화면에서 create role 버튼을 클릭하여 신규 role 생성 할 수 있음.

![kibana_create_role.png](Elasticsearch%20Sink%20Connector%20%E1%84%80%E1%85%AE%E1%84%89%E1%85%A5%E1%86%BC%20541c79f298c844c49b5fb5a1e3ac924d/kibana_create_role.png)

- connector용 신규 유저 es_connect_dev 생성.

```sql
POST /_security/user/es_connect_dev
{
  "password" : "es_connect_dev",
  "roles" : [ "es_sink_connector_role" ]
}'
```

### Elasticsearch Sink Connector 다운로드 및 설치

- Elasticsearch sink connector plug-in 다운로드 받기

[ElasticSearch Sink Connector](https://www.confluent.io/hub/confluentinc/kafka-connect-elasticsearch)

- 다운로드 받은 압축파일을 실습용 vm에 올리고 압축 해제
- /home/min/connector_plugins 디렉토리에 es_sink_connector 서브디렉토리를 만들고 압축파일의 lib 디렉토리에 있는 모든 jar 파일을 es_sink_connector 디렉토리로 이동.
- connect를 재 기동하고 connector-plugins 확인

```sql
http http://localhost:8083/connector-plugins
```

### Elasticsearch Sink Connector 환경 설정 - 01

- ksqldb에서 신규 stream인 simple_stream_test_01 생성하되 Key 컬럼을 설정하지 않음.

```sql
create stream simple_stream_test_01
(
  id int,
  name varchar,
  email varchar
) with (
  KAFKA_TOPIC = 'simple_stream_test_01',
  VALUE_FORMAT ='JSON',
  PARTITIONS = 1
);
```

- 새롭게 생성된 Stream에 데이터를 insert 문으로 입력 후 Stream과 Topic 메시지 확인.

```sql
insert into simple_stream_test_01(id, name, email) values (1, 'test_name_01', 'test_email_01@test.domain');
insert into simple_stream_test_01(id, name, email) values (2, 'test_name_02', 'test_email_02@test.domain');

select * from simple_stream_test_01;

describe simple_stream_test_01 extended;
```

- es_sink_simple_stream_test_01.json에 아래와 같은 설정으로 config 파일 생성. key.ignore와 schema.ignore를 false로 설정. 해당 설정은 테스트를 위한 것이며, 수행 시 schema 정보가 없어서 schema.ignore=false 설정 오류로 에러 발생.

```sql
{
    "name": "es_sink_simple_stream_test_01",
    "config": {
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "tasks.max": "1",
        "topics": "simple_stream_test_01",
        "connection.url": "http://192.168.56.101:9200",
        "connection.username": "es_connect_dev",
        "connection.password": "es_connect_dev",

        "key.ignore": "false",
        "schema.ignore": "false",

        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
  
    }
}
```

- 아래와 같이 key.ignore, schema.ignore를 true로 설정하고 다시 connector 생성/등록. 하지만 key.ignore와 schema.ignore를 true로 설정하는 것은 권장사항이 아님.

```sql
{
    "name": "es_sink_simple_stream_test_01",
    "config": {
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "tasks.max": "1",
        "topics": "simple_stream_test_01",
        "connection.url": "http://192.168.56.101:9200",
        "connection.username": "es_connect_dev",
        "connection.password": "es_connect_dev",

        "key.ignore": "true",
        "schema.ignore": "true",
        
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
  
    }
}
```

- kibana에서 simple_stream_test_01명으로 index가 생성됨을 확인
- dev tool에서 아래 명령어를 입력하여 신규 생성 index와 document 확인

```sql
# 아래는 index 확인
GET simple_stream_test_01

# 아래는 simple_user_stream 인덱스의 모든 document 확인. 
GET simple_stream_test_01/_search
```

- 아래와 같이 curl 또는 httpie로 REST API 호출할 수도 있음

```sql
curl -u es_search_sink:es_search_sink  -XGET '192.168.56.101:9200/simple_stream_test_01/_search?pretty'

http -a es_search_sink:es_search_sink http://192.168.56.101:9200/simple_stream_test_01/_search
```

### Elasticsearch Sink Connector 환경 설정(Avro 기반) - 02

- Key값을 가지는 avro 기반 Topic을 ES Sink Connector와 연동

```sql
create stream simple_stream_test_02
(
  id int KEY,
  name varchar,
  email varchar
) with (
  KAFKA_TOPIC = 'simple_stream_test_02',
  KEY_FORMAT = 'AVRO',
  VALUE_FORMAT ='AVRO',
  PARTITIONS = 1
);

insert into simple_stream_test_02(id, name, email) values (1, 'test_name_01', 'test_email_01@test.domain');
insert into simple_stream_test_02(id, name, email) values (2, 'test_name_02', 'test_email_02@test.domain');

select * from simple_stream_test_02;

describe simple_stream_test_02 extended;
```

- schema registry에 해당 topic으로 schema subject 확인 및 global 호환성 설정 확인

```sql
http http://localhost:8081/schemas

http http://localhost:8081/subjects

http GET http://localhost:8081/schemas/ids/1

http GET http://localhost:8081/config
```

- es_sink_simple_stream_test_02.json으로 ES Sink Connector 환경 설정 및 Connector 생성.

```sql
{
    "name": "es_sink_simple_stream_test_02",
    "config": {
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "tasks.max": "1",
        "topics": "simple_stream_test_02",
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

register_connector es_sink_simple_test_02.json
```

- ES에서 simple_stream_test_02 index가 생성되고 document가 입력되었는지 확인

### 데이터 추가/변경/삭제

- topic에 추가 메시지 입력하고 ES에 적용 확인

```sql
insert into simple_stream_test_02(id, name, email) values (3, 'test_name_03', 'test_email_03@test.domain');
insert into simple_stream_test_02(id, name, email) values (4, 'test_name_04', 'test_email_04@test.domain');

select * from simple_stream_test_02;

print simple_stream_test_02 from beginning;
```

- topic에 key값이 기존 key값과 동일한 메시지 생성 후 ES 적용 확인

```sql
insert into simple_stream_test_02(id, name, email) values (3, 'new_test_name_03', 'new_test_email_03@test.domain');
insert into simple_stream_test_02(id, name, email) values (4, 'new_test_name_04', 'new_test_email_04@test.domain');

select * from simple_stream_test_02;

print simple_stream_test_02 from beginning;
```

### 복합 Key(멀티 컬럼 PK)기반의 Topic을 ES Sink Connector로 연동

```sql
CREATE STREAM customer_activity_stream_avro (
    CUSTOMER_ID INTEGER KEY,
    ACTIVITY_SEQ INTEGER,
    ACTIVITY_TYPE VARCHAR,
    ACTIVITY_POINT DOUBLE
   ) WITH (
    KAFKA_TOPIC = 'customer_activity_stream_avro',
    KEY_FORMAT = 'AVRO',
    VALUE_FORMAT = 'AVRO',
    PARTITIONS = 1
);

INSERT INTO customer_activity_stream_avro (customer_id, activity_seq, activity_type, activity_point) VALUES (1, 1,'branch_visit',0.4);
INSERT INTO customer_activity_stream_avro (customer_id, activity_seq, activity_type, activity_point) VALUES (2, 1,'web_open',0.43);
INSERT INTO customer_activity_stream_avro (customer_id, activity_seq, activity_type, activity_point) VALUES (2, 2, 'deposit',0.56);
INSERT INTO customer_activity_stream_avro (customer_id, activity_seq, activity_type, activity_point) VALUES (3, 3,'web_open',0.33);
INSERT INTO customer_activity_stream_avro (customer_id, activity_seq, activity_type, activity_point) VALUES (1, 4,'deposit',0.41);
INSERT INTO customer_activity_stream_avro (customer_id, activity_seq, activity_type, activity_point) VALUES (2, 5,'deposit',0.44);
INSERT INTO customer_activity_stream_avro (customer_id, activity_seq, activity_type, activity_point) VALUES (1, 7, 'mobile_open', 0.97);
INSERT INTO customer_activity_stream_avro (customer_id, activity_seq, activity_type, activity_point) VALUES (2, 8,'deposit',0.83);
INSERT INTO customer_activity_stream_avro (customer_id, activity_seq, activity_type, activity_point) VALUES (4, 1,'mobile_open',0.33);
```

```sql
create table customer_activity_avro_mv01
with (
KAFKA_TOPIC='customer_activity_avro_mv01', 
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO',
PARTITIONS = 1
)
as
select customer_id, activity_type, count(*) as cnt 
from customer_activity_stream_avro group by customer_id, activity_type;

select * from customer_activity_avro_mv01;

print customer_activity_avro_mv01;
```

- es_sink_customer_activity_avro_mv01.json 파일로 ES Sink Connector 생성 시도. 하지만 여러개의 key값을 가지는 struct 형태의 key는 ES의 document _id로 변환하지 못하므로 오류 발생.

```sql
{
    "name": "es_sink_customer_activity_avro_mv01",
    "config": {
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "tasks.max": "1",
        "topics": "customer_activity_avro_mv01",
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

- document id로 사용하기 위해 customer_id + activity_type으로 추가적인 단일 컬럼 생성.

```sql
create table customer_activity_avro_mv02
with 
(
KAFKA_TOPIC='customer_activity_avro_mv02', 
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO',
PARTITIONS = 1
)
as
select  customer_id, activity_type, max(rowtime) as rtime,
        as_value(customer_id) as cust_id, as_value(activity_type) as act_type,
        count(*) as cnt,
        cast(customer_id as varchar) + activity_type as doc_id
from customer_activity_stream_avro 
group by customer_id, activity_type;
```

- 메시지 Value값을 Key로 변환하는 SMT를 적용하여 ES Sink Connector 구성

```sql
{
    "name": "es_sink_customer_activity_avro_mv02",
    "config": {
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "tasks.max": "1",
        "topics": "customer_activity_avro_mv02",
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

### Elasticsearch의 index명을 SMT로 변경

- Elasticsearch sink connector는 topic명을 기본적으로 index명으로 생성함. 이를 변경하려면 SMT RegexRouter를 이용해야함.
- 다만 RegexRouter를 이용할 때 flush.synchronously를 true로 설정해야함. 설정하지 않을 시 Connect에서 Found a topic name 'es_customers' that doesn't match assigned partitions. Connector doesn't support topic mutating SMTs 오류를 발생 시킴. flush.synchronously를 true로 설정하면 elasticsearch로 입력 성능이 떨어짐.
- 아래 설정으로 es_oc_sink_customers_01.json 파일을 생성하고 Connector 등록 생성.  아래 설정은 mysqlavro.oc.customers가 아닌 es_customers로 index를 생성함.

```sql
{
    "name": "es_oc_sink_customers_01",
    "config": {
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "tasks.max": "1",
        "topics": "mysqlavro.oc.customers",
        "connection.url": "http://192.168.56.101:9200",
        "connection.username": "elastic",
        "connection.password": "elastic",
        "type.name": "_doc",

        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": "http://localhost:8081",
        "value.converter.schema.registry.url": "http://localhost:8081",

        "transforms": "change_index, create_key, extract_key",
        "transforms.change_index.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.change_index.regex": "mysqlavro\\.oc\\.customers",
        "transforms.change_index.replacement": "es_customers",
        "transforms.create_key.type": "org.apache.kafka.connect.transforms.ValueToKey",
        "transforms.create_key.fields": "customer_id",
        "transforms.extract_key.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
        "transforms.extract_key.field": "customer_id",
        "flush.synchronously": "true"

    }
}
```
