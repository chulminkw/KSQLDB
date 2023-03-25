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
drop connector DGEN_CLICKSTREAM_USER
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

### External 모드로 KSQLDB와 연동

- 기존에 Embedded 모드에서 등록한 Connector들은 모두 삭제하고 ksqldb 프로세스 종료
- External Connect의 환경 설정 파라미터 수정.

```bash
cd $CONFLUENT_HOME/etc/kafka
vi connect-distributed.properties
plugin.path = /home/min/connector_plugins 
```

- External Connect를 기동하는 스크립트 connect_start.sh과 connect_start_log.sh 생성.

```bash
vi connect_start.sh
$CONFLUENT_HOME/bin/connect-distributed $CONFLUENT_HOME/etc/kafka/connect-distributed.properties

vi connect_start_log.sh
log_suffix=`date +"%Y%m%d%H%M%S"`
$CONFLUENT_HOME/bin/connect-distributed $CONFLUENT_HOME/etc/kafka/connect-distributed.properties 2>&1 | tee -a ~/connect_console_log/connect_console_$log_suffix.log

mkdir connect_console_log
chmod +x *.sh
```

- External Connect를 기동하고 정상 기동 확인

```bash
connect_start.sh

http http://localhost:8083/connectors
http http://localhost:8083/connector-plugins
```

- ksqldb-server.properties에 기존 embeded connect 설정은 주석처리 하고 ksql.connect.url=http://localhost:8083 설정 저장.

```bash
vi ksqldb-server.properties
#ksql.connect.worker.config=/home/min/confluent/etc/ksqldb/connect.properties
ksql.connect.url=http://localhost:8083
```

### External 모드 Connect에서 신규 Connector 생성 후 KSQLDB와 연동

### Connector Utility Shell 생성.

- connect에 등록된 connector들 조회 - show_connectors

```sql
vi show_connectors
http GET http://localhost:8083/connectors

chmod +x show_connector
```

- 신규 connector 생성 - register_connector

```sql
vi register_connector
/usr/bin/http POST http://localhost:8083/connectors @/home/min/connector_configs/$1

chmod +x register_connector
```

- 기존 connector 삭제 - delete_connector

```sql
vi delete_connectors
http DELETE http://localhost:8083/connectors/$1

chmod +x delete_connector
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

- topic 삭제하고 재생성.

```sql
kafka-topics --bootstrap-server localhost:9092 --delete --topic dgen_clickstream_users
kafka-topics --bootstrap-server localhost:9092 --create --topic dgen_clickstream_users --partitions 3
```

- 아래와 같은 설정으로 dgen_clickstream_users.json 파일을 생성.

```json

{
  "name": "dgen_clickstream_users",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "dgen_clickstream_users",
    "quickstart": "CLICKSTREAM_USERS",
    "max.interval": 500,
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.converters.IntegerConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true"
  }
}
```

- dgen_clickstream_users connector 생성 등록 및 topic 메시지 확인

```bash
register_connector dgen_clickstream_users.json
show_connectors

show_topic_messages json dgen_clickstream_users
```

- ksqldb에서 Connector 확인/삭제/생성 관리

```sql
SHOW CONNECTORS;

drop connector "dgen_clickstream_users";

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

### ClickStream 데이터 생성을 위한 Datagen Connector 생성.

- Click Stream 데이터를 위한 Streams 생성. Key는 String, Value는 Json형태로 생성.  converter schema는 disable 설정(즉 value의 schema정보는 메시지로 생성하지 않음)

```sql
-- 기존에 dgen_clickstreams topic이 있으면 삭제. 
kafka-topics --bootstrap-server localhost:9092 --delete --topic dgen_clickstreams_topic

-- partition의 갯수가 3인 topic dgen_clickstreams 생성.
kafka-topics --bootstrap-server localhost:9092 --create --topic dgen_clickstreams_topic --partitions 3 
```

```json
{
  "name": "dgen_clickstreams",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "dgen_clickstreams_topic",
    "quickstart": "clickstream",
    "max.interval": 1000,
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
```

### ClickStream 생성 및 Stream과 Table 생성.

- dgen_clickstreams_topic에 기반한 clickstreams Stream 생성

```sql
CREATE STREAM clickstreams (
  ip VARCHAR KEY,
  userid INTEGER,
  remote_user VARCHAR,
  time VARCHAR, 
  _time INTEGER,
  request VARCHAR,
  status VARCHAR,
  bytes VARCHAR,
  referer VARCHAR,
  agent VARCHAR
)
WITH (
KAFKA_TOPIC = 'dgen_clickstreams_topic',
KEY_FORMAT = 'JSON',
VALUE_FORMAT = 'JSON'
);

select * from clickstreams limit 5;
```

- clickstream_users Table 생성.

```sql
CREATE TABLE clickstream_users (
  user_id INTEGER PRIMARY KEY,
  username VARCHAR,
  registered_At BIGINT,
  first_name VARCHAR,
  last_name VARCHAR,
  city VARCHAR,
  level VARCHAR
) WITH (
  KAFKA_TOPIC = 'dgen_clickstream_users_topic',
  VALUE_FORMAT = 'JSON'
);

print dgen_clickstream_users limit 3;

select * from clickstream_users emit changes limit 3;
```

- value.converter.schemas.enable을 false로 설정하여 dgen_clickstream_users.json 변경. 먼저 기존 Table과 topic을 삭제하고 topic 재 생성. connector 도 drop

```sql
drop table clickstream_users delete topic;
drop connector "dgen_clickstream_users";
```

```sql
kafka-topics --bootstrap-server localhost:9092 --create --topic dgen_clickstream_users_topic --partitions 3
```

- 아래 설정으로 dgen_clickstream_users.json 변경.

```sql
{
  "name": "dgen_clickstream_users",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "dgen_clickstream_users_topic",
    "quickstart": "CLICKSTREAM_USERS",
    "max.interval": 1000,
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.converters.IntegerConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

- clickstream_users Table 재 생성 후 데이터 확인. .

```sql
print dgen_clickstream_users limit 3;

CREATE TABLE clickstream_users (
  user_id INTEGER PRIMARY KEY,
  username VARCHAR,
  registered_At BIGINT,
  first_name VARCHAR,
  last_name VARCHAR,
  city VARCHAR,
  level VARCHAR
) WITH (
  KAFKA_TOPIC = 'dgen_clickstream_users_topic',
  VALUE_FORMAT = 'JSON'
);

select * from clickstream_users emit changes limit 3;
```

### Avro 포맷으로 KSQLDB Stream/Table 생성.

- 아래와 같은 설정으로 dgen_clickstreams_avro.json 을 생성하고 connector 생성.

```json
{
  "name": "dgen_clickstreams_avro",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "kafka.topic": "dgen_clickstreams_topic_avro",
    "quickstart": "clickstream",
    "max.interval": 1000,
    "tasks.max": "1",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://localhost:8081",
    "value.converter.schema.registry.url": "http://localhost:8081"
  }
}
```

- dgen_clickstreams_topic_avro에 기반한 clickstreams Stream 생성

```sql
--_time은 int64-> int32 Serialization 이슈로 BIGINT로 변경.
CREATE STREAM clickstreams_avro (
  ip VARCHAR KEY,
  userid INTEGER,
  remote_user VARCHAR,
  time VARCHAR, 
  _time BIGINT,
  request VARCHAR,
  status VARCHAR,
  bytes VARCHAR,
  referer VARCHAR,
  agent VARCHAR
)
WITH (
KAFKA_TOPIC = 'dgen_clickstreams_topic_avro',
KEY_FORMAT = 'AVRO',
VALUE_FORMAT = 'AVRO'
);

select * from clickstreams_avro limit 5;
```

- 방금 생성한 Connector, Stream, Topic 삭제

```json
drop connector "dgen_clickstreams_topic_avro";
drop streams clickstreams_avro delete topic;
```
