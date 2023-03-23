# KSQLDB와 Connect 연동 - 01

### Embedded Connect 설정

- embeded Connect Worker를 기동하기 위해서 아래 설정을 ksql-server.properties에 connect 환경 파일을 설정.

```sql
ksql.connect.worker.config=config/connect.properties
```

- ksqldb 재기동 후 JVM Process 확인 및 ksqldb 프로세스의 thread list 확인

```sql
jps
ps -ef|grep java

jstack -l jvm프로세스 id | grep connect
```

- Embedded Connect가 기동되었는지 확인

```sql
http GET http://localhost:8083
curl -X GET http://localhohst:8083

netstat -an |grep 8083
```

### Embedded Connect에서 Datagen Connector 설치

- 아래 URL에서 Datagen Connector 다운로드하여 로컬 PC에 저장.

[https://www.confluent.io/hub/confluentinc/kafka-connect-datagen](https://www.confluent.io/hub/confluentinc/kafka-connect-datagen) 또는 [https://github.com/chulminkw/KSQLDB/blob/main/confluentinc-kafka-connect-datagen-0.6.0.zip](https://github.com/chulminkw/KSQLDB/blob/main/confluentinc-kafka-connect-datagen-0.6.0.zip)

- 압축 파일을 실습 VM에 업로드
- home directory에 connector_plugins 디렉토리 생성.
- home directory에서 압축 파일을 풀고 lib 디렉토리를 connector_plugins 디렉토리로 이동한 뒤 lib 디렉토리를 datagen_source로 변경
- [ksql-server.properties](http://ksql-server.properties) 파일의 plugin.path를 /home/min/connector_plugins 로 설정 후 ksqldb 재 기동.
- 아래 command도

```sql
curl –X GET –H “Content-Type: application/json” http://localhost:8083/connector-plugins
http http://localhost:8083/connector-plugins
```

### Embedded Connect에서 Datagen Connector 구동 - 01

- DGEN_CLICKSTREAM_USERS Connector 생성.

```sql
CREATE SOURCE CONNECTOR IF NOT EXISTS DGEN_CLICKSTREAM_USERS WITH (
  'connector.class'          = 'io.confluent.kafka.connect.datagen.DatagenConnector',
  'kafka.topic'              = 'dgen_clickstream_users',
  'quickstart'               = 'CLICKSTREAM_USERS',
  'max.interval'              = '500',
  'tasks.max'                = '1',
  'key.converter'            = 'org.apache.kafka.connect.converters.IntegerConverter',
  'value.converter'          = 'org.apache.kafka.connect.json.JsonConverter',
  'value.converter.schemas.enable' = 'true'
);
```

- datagen_clickstream_user Connector의 상태 및 topic 메시지 확인.

```sql
--ksql cli에서 아래 수행
show connectors;

print dgen_clickstream_users;
```

- 또는 아래 명령어를 수행하여 topic 메시지 확인.

```sql
kafka-console-consumer --bootstrap-server localhost:9092 --topic dgen_clickstream_users --from-beginning --property print.key=true
```

- connector 삭제하기

```sql
drop connector DGEN_CLICKSTREAM_USERS;
```

- topic 삭제하기

```sql
kafka-topics --bootstrap-server localhost:9092 --delete --topic dgen_clickstream_users
```

- partition이 3개인 topic 생성.

```sql
kafka-topics --bootstrap-server localhost:9092 --create --topic dgen_clickstream_users --partitions 3
```

- connector 재생성.

```sql
CREATE SOURCE CONNECTOR IF NOT EXISTS DGEN_CLICKSTREAM_USERS WITH (
  'connector.class'          = 'io.confluent.kafka.connect.datagen.DatagenConnector',
  'kafka.topic'              = 'dgen_clickstream_users',
  'quickstart'               = 'CLICKSTREAM_USERS',
  'max.interval'              = '500',
  'tasks.max'                = '1',
  'key.converter'            = 'org.apache.kafka.connect.converters.IntegerConverter',
  'value.converter'          = 'org.apache.kafka.connect.json.JsonConverter',
  'value.converter.schemas.enable' = 'true'
);
```

### Connector Utility Shell 생성.

- connect에 등록된 connector들 조회 - show_connectors

```sql
vi show_connectors
http GET http://localhost:8083/connectors

chmod +x show_connector
```

- topic message 조회 - show_topic_messages

```sql
vi show_topic_messages

if [ $1 == 'avro' ]
then
        kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic $2 --from-beginning --property print.key=true |jq '.'

else
        kafka-console-consumer --bootstrap-server localhost:9092 --topic $2 --from-beginning --property print.key=true |jq '.'

fi

chmod +x show_topic_messages
```

- user_id를 PK로 하는 clickstream_users 테이블 생성.

```sql
CREATE TABLE clickstream_users (
  user_id VARCHAR PRIMARY KEY,
  registered_At BIGINT,
  first_name VARCHAR,
  last_name VARCHAR,
  city VARCHAR,
  level VARCHAR
) WITH (
  KAFKA_TOPIC = 'dgen_clickstream_users_avro',
  VALUE_FORMAT = 'AVRO',
  PARTITIONS = 1
);
```

- clickstream connector 생성

```sql
CREATE SOURCE CONNECTOR IF NOT EXISTS DATAGEN_CLICKSTREAM WITH (
  'connector.class'          = 'io.confluent.kafka.connect.datagen.DatagenConnector',
  'kafka.topic'              = 'dgen_clickstream_avro',
  'quickstart'               = 'CLICKSTREAM',
  'maxInterval'              = '5000',
  'tasks.max'                = '1',
  'value.converter'       = 'io.confluent.connect.avro.AvroConverter',
  'value.converter.schema.registry.url' = 'http://localhost:8081'
);
```

- clickstream stream 생성.

```sql
CREATE STREAM clickstream (
  ip VARCHAR,
  userid INT,
  remote_user VARCHAR,
  time VARCHAR,  
  _time BIGINT,
  request VARCHAR,
  status VARCHAR,
  bytes VARCHAR,
  referer VARCHAR,
  agent VARCHAR
) WITH (
  KAFKA_TOPIC = 'dgen_clickstream_avro',
  VALUE_FORMAT = 'AVRO',
  PARTITIONS = 1
);
```

- 아래와 같이 Embeded Connector 생성 스크립트를 ksqldb에서 수행.

```sql

```
