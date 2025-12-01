# Kafka Streams Template Project

이 프로젝트는 Kafka Streams 애플리케이션을 빠르고 표준화된 방식으로 개발하기 위한 템플릿입니다. 
복잡한 Topology 코드를 직접 작성하는 대신, **설정 파일(`pipelines.yml`)**과 **데이터 매핑 로직(Mapper)**만 정의하면 스트림즈 애플리케이션을 구동할 수 있도록 설계되었습니다.

## 시작하기 (Getting Started)

이 프로젝트를 처음 다운로드 받으셨다면, 다음 순서대로 실행 환경을 구성해 보세요.

### 1. 필수 요구 사항 (Prerequisites)
*   **Java 17** 이상
*   **Maven 3.8** 이상
*   접근 가능한 **Kafka Cluster** 및 **Schema Registry**

### 2. 환경 변수 설정 (.env)
프로젝트 루트 디렉토리에 `.env` 파일을 생성하거나 수정하여 Kafka 연결 정보를 설정합니다.
제공된 `.env` 예시 파일을 참고하세요.

```properties
# 필수 설정
APPLICATION_ID=my-kstreams-app-v1
KAFKA_URL=kafka+ssl://my-kafka-broker:9092
SCHEMA_REGISTRY_URL=http://my-schema-registry:8081
STATE_DIR=/tmp/kstreams-state

# SSL 설정 (필요한 경우)
KAFKA_TRUSTSTORE_PATH=/path/to/truststore.jks
KAFKA_TRUSTSTORE_PASS=changeit
KAFKA_KEYSTORE_PATH=/path/to/keystore.p12
KAFKA_KEYSTORE_PASS=password
```

### 3. 빌드 및 실행
터미널에서 다음 명령어를 사용하여 프로젝트를 빌드하고 실행합니다.

```bash
# 코드 포맷팅 적용 (빌드 전 권장)
mvn spotless:apply

# 프로젝트 빌드 (Avro 소스 생성 및 테스트 포함)
mvn clean install

# 애플리케이션 실행 (fat-jar)
java -jar target/kstreams.jar
```

---

## 프로젝트 구조 (Project Structure)

핵심 파일과 디렉토리의 역할은 다음과 같습니다.

*   `src/main/resources/pipelines.yml`: **가장 중요한 파일**입니다. 스트림즈의 전체 흐름(Source -> Process -> Sink)을 정의합니다.
*   `src/main/resources/avro/`: Kafka Topic에서 사용할 Avro 스키마 파일(.avsc)들을 위치시킵니다.
*   `src/main/java/com/ci/streams/mapper/`: 데이터 변환 로직을 담당하는 Java 클래스들을 작성하는 곳입니다.
*   `src/main/java/com/ci/streams/App.java`: 애플리케이션의 진입점이며, `pipelines.yml`을 읽어 Topology를 생성합니다.

---

## 개발 가이드 (Development Guide)

새로운 파이프라인을 추가하려면 다음 3단계를 따르세요.

### Step 1: Avro 스키마 정의
`src/main/resources/avro/` 디렉토리에 입력/출력 데이터의 구조를 정의한 `.avsc` 파일을 추가합니다.
빌드(`mvn clean install`)를 실행하면 자동으로 Java 클래스가 생성됩니다.

### Step 2: Mapper 구현
데이터를 어떻게 변환할지 정의합니다. `src/main/java/com/ci/streams/mapper/` 패키지 아래에 클래스를 생성합니다.
보통 `RecordMapper` 인터페이스를 구현하거나, 기존의 Abstract 클래스를 상속받아 구현합니다.

예시:
```java
public class MyCustomMapper implements RecordMapper<InputClass, OutputClass> {
    @Override
    public OutputClass map(InputClass input) {
        // 변환 로직 작성
        OutputClass output = new OutputClass();
        output.setField(input.getField());
        return output;
    }
}
```

특히 `AbstractJsonDebeziumMapper`를 상속받는 경우, 다음과 같이 `FieldMappingRule`을 사용하여 입력 JSON 데이터의 필드를 Avro 스키마의 필드에 매핑할 수 있습니다.

```java
import com.ci.streams.mapper.AbstractJsonDebeziumMapper;
import com.ci.streams.mapper.FieldMappingRule;
import com.ci.streams.mapper.lv1.Lv1Mapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;

import java.util.List;
import java.util.Map;

import static com.ci.streams.mapper.FieldMappingRule.ofJsonString;
import static com.ci.streams.mapper.FieldMappingRule.ofJsonTimestamp;

public class SampleUserMapper extends AbstractJsonDebeziumMapper implements Lv1Mapper {

  private final List<FieldMappingRule<JsonNode>> MAPPING_RULES;

  public SampleUserMapper(Schema schema, Map<String, Object> params) {
    super(schema, params);

    this.MAPPING_RULES =
        List.of(
            ofJsonString("user_id", "id"),
            ofJsonString("user_name", "name"),
            ofJsonString("email", "email"),
            ofJsonTimestamp("created_at", "created_at"));
  }

  @Override
  protected List<FieldMappingRule<JsonNode>> getMappingRules() {
    return MAPPING_RULES;
  }
}
```
`FieldMappingRule`을 사용하여 `ofJsonString`, `ofJsonTimestamp`와 같은 헬퍼 메서드를 통해 소스 JSON 필드를 대상 Avro 필드로 매핑하는 규칙을 정의합니다. 이는 Debezium과 같은 CDC(Change Data Capture) 솔루션에서 생성된 JSON 형태의 데이터를 처리할 때 특히 유용합니다.

반면, 이미 Kafka Streams 내부에서 Avro 형태로 처리된 데이터를 다시 매핑해야 할 경우 (예: Lv1 프로세스 이후의 Lv2 프로세스), `ofAvroString`, `ofAvroLong`과 같은 헬퍼 메서드를 사용하거나 커스텀 매핑 로직을 구현할 수 있습니다. 이는 입력 데이터가 JSON이 아닌 Avro 레코드일 때 유용합니다.

```java
import com.ci.streams.mapper.AbstractAvroKStreamsMapper;
import com.ci.streams.mapper.FieldMappingRule;
import com.ci.streams.mapper.lv2.Lv2Mapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;

import static com.ci.streams.mapper.FieldMappingRule.ofAvroLong;
import static com.ci.streams.mapper.FieldMappingRule.ofAvroString;

public class SampleUnionMapper extends AbstractAvroKStreamsMapper implements Lv2Mapper {

  private final List<FieldMappingRule<GenericRecord>> MAPPING_RULES;
  private final String channel;

  public SampleUnionMapper(Schema schema, Map<String, Object> params) {
    super(schema, params);
    this.channel = (String) params.get("channel"); // pipelines.yml에서 channel 파라미터 사용

    this.MAPPING_RULES =
        List.of(
            ofAvroString("user_id", "cust_id"), // Lv1 User의 cust_id를 user_id로
            new FieldMappingRule<>(
                "full_name", // 대상 필드 이름
                input -> { // 커스텀 매핑 로직
                  Object firstNameObj = safeGet(input, "cust_first_name");
                  Object lastNameObj = safeGet(input, "cust_last_name");
                  String fullName = "";
                  if (firstNameObj != null) fullName += firstNameObj.toString();
                  if (lastNameObj != null) fullName += " " + lastNameObj.toString();
                  return fullName.trim().isEmpty() ? null : fullName.trim();
                }),
            ofAvroString("contact_email", "cust_email"), // Lv1 User의 cust_email을 contact_email로
            ofAvroString("contact_phone", "cust_phone"), // Lv1 User의 cust_phone을 contact_phone로
            ofAvroString("__op", "__op"), // _op는 그대로 전달
            ofAvroLong("__processed_at", "__processed_at") // _processed_at 그대로 전달
            );
  }

  @Override
  protected List<FieldMappingRule<GenericRecord>> getMappingRules() {
    return MAPPING_RULES;
  }
}
```
이 예시는 `AbstractAvroKStreamsMapper`를 상속받아 `GenericRecord` 형태의 Avro 데이터를 처리하는 방법을 보여줍니다. `ofAvroString`, `ofAvroLong`과 같은 메서드로 Avro 필드 간의 직접 매핑을 정의하거나, `FieldMappingRule`에 람다 표현식을 사용하여 `full_name`처럼 여러 필드를 조합하거나 복잡한 변환 로직을 적용할 수 있습니다. `params`를 통해 `pipelines.yml`에 정의된 추가 파라미터(`channel` 등)를 Mapper 내에서 활용하는 예시도 포함되어 있습니다.

### Step 3: pipelines.yml 등록
`src/main/resources/pipelines.yml` 파일에 새 파이프라인을 정의합니다.

```yaml
lv1_processors: # 또는 api_processors, lv2_processors
  pipelines:
    - name: "MyNewPipeline"           # 파이프라인 이름 (유니크해야 함)
      sourceTopic: "source.topic.name" # 읽어올 토픽
      destinationTopic: "dest.topic.name" # 저장할 토픽
      schemaName: "OutputSchemaName"   # Step 1에서 만든 스키마 이름 (확장자 제외)
      mapperName: "MyCustomMapper"     # Step 2에서 만든 Mapper 클래스 이름
```

---

## 고급 설정 (pipelines.yml)

`pipelines.yml`은 반복되는 설정을 줄이기 위해 **Common(공통 설정)**과 **Template(템플릿)** 기능을 제공합니다.

### Processor 그룹
*   `lv1_processors`: 1차 가공 (Raw -> Bronze/Silver)
*   `api_processors`: 외부 API 연동 등이 필요한 처리
*   `lv2_processors`: 집계나 조인 등 심화 가공 (Silver -> Gold/Platinum)

### Template 기능 (Lv2 예시)
여러 소스 토픽을 동일한 로직으로 처리해야 할 때 유용합니다.

```yaml
lv2_processors:
  templates:
    - name: "UnionTemplate"
      destinationTopic: "gold.union"
      # ... 공통 설정 ...
  
  sources:
    - sourceTopic: "topic.A"
      targetTemplates: ["UnionTemplate"] # topic.A를 UnionTemplate 규칙으로 처리
    - sourceTopic: "topic.B"
      targetTemplates: ["UnionTemplate"] # topic.B도 UnionTemplate 규칙으로 처리
```

이렇게 설정하면 `topic.A`와 `topic.B` 모두 `gold.union` 토픽으로 데이터가 모이게 됩니다.

---

## 테스트 (Testing)
JUnit 5와 `kafka-streams-test-utils`를 사용하여 Topology를 테스트할 수 있습니다.
`src/test/java` 경로에 테스트 코드를 작성하고 `mvn test`로 실행하세요.