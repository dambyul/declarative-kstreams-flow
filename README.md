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

# SSL 설정 (PEM 방식 지원 - 권장)
# 별도의 Truststore/Keystore(JKS) 생성 없이 PEM 파일을 직접 지정할 수 있습니다.
KAFKA_TRUSTED_CERT_FILE=/path/to/ca.crt
KAFKA_CLIENT_CERT_FILE=/path/to/client.crt
KAFKA_CLIENT_CERT_KEY_FILE=/path/to/client.key

# 또는 컨테이너 환경 등에서 파일 생성이 어려운 경우 내용을 직접 입력:
# KAFKA_TRUSTED_CERT=-----BEGIN CERTIFICATE-----...
# KAFKA_CLIENT_CERT=-----BEGIN CERTIFICATE-----...
# KAFKA_CLIENT_CERT_KEY=-----BEGIN PRIVATE KEY-----...
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

# 운영 환경 실행 (외부 설정 파일 사용)
java -Dconfig.file=/path/to/prod_pipelines.yml -jar target/kstreams.jar
```

---

## 프로젝트 구조 (Project Structure)

핵심 파일과 디렉토리의 역할은 다음과 같습니다.

*   `src/main/resources/pipelines.yml`: **가장 중요한 파일**입니다. 스트림즈의 전체 흐름(Source -> Process -> Sink)을 정의합니다.
*   `src/main/resources/avro/`: Kafka Topic에서 사용할 Avro 스키마 파일(.avsc)들을 위치시킵니다.
*   `src/main/java/com/ci/streams/mapper/`: 데이터 변환 로직을 담당하는 Java 클래스들을 작성하는 곳입니다.
*   `src/main/java/com/ci/streams/App.java`: 애플리케이션의 진입점이며, `pipelines.yml`을 읽어 Topology를 생성합니다.

---

## 주요 기능 (Key Features)

### 1. 강력한 SSL 지원 (PEM Support)
Bouncy Castle 라이브러리를 내장하여 `openssl`을 이용한 복잡한 JKS 변환 과정 없이 **PEM 형식의 인증서를 바로 사용**할 수 있습니다.
PKCS#1 및 PKCS#8 키 형식을 모두 지원하며, 환경 변수(`_FILE` 접미사 활용)를 통해 유연하게 경로를 지정할 수 있습니다.

### 2. 외부 설정 파일 지원 (External Configuration)
애플리케이션 재빌드 없이 설정을 변경할 수 있도록 외부 설정 파일을 지원합니다.
*   **방법 1**: Java System Property `-Dconfig.file=/path/to/pipelines.yml`
*   **방법 2**: Environment Variable `PIPELINES_CONFIG_FILE=/path/to/pipelines.yml`

### 3. 템플릿 기반 설정 (Template Config)
`pipelines.yml`에서 반복되는 파이프라인 설정을 **Template**으로 정의하고, 여러 **Source**에서 이를 재사용할 수 있습니다.
이를 통해 설정 파일의 길이를 줄이고 유지보수성을 높일 수 있습니다.

```yaml
lv2_processors:
  templates:
    - name: "UnionTemplate"
      destinationTopic: "gold.union"
      # ... 공통 설정 ...
  
  sources:
    - sourceTopic: "topic.A"
      targetTemplates: ["UnionTemplate"]
```

---

## 개발 가이드 (Development Guide)

새로운 파이프라인을 추가하려면 다음 3단계를 따르세요.

### Step 1: Avro 스키마 정의
`src/main/resources/avro/` 디렉토리에 입력/출력 데이터의 구조를 정의한 `.avsc` 파일을 추가합니다.
빌드(`mvn clean install`)를 실행하면 자동으로 Java 클래스가 생성됩니다.

### Step 2: Mapper 구현
데이터를 어떻게 변환할지 정의합니다. `src/main/java/com/ci/streams/mapper/` 패키지 아래에 클래스를 생성합니다.
보통 `RecordMapper` 인터페이스를 구현하거나, 기존의 Abstract 클래스를 상속받아 구현합니다.

### Step 3: pipelines.yml 등록
`src/main/resources/pipelines.yml` 파일에 새 파이프라인을 정의합니다.

---

## 테스트 (Testing)
JUnit 5와 `kafka-streams-test-utils`를 사용하여 Topology를 테스트할 수 있습니다.
`src/test/java` 경로에 테스트 코드를 작성하고 `mvn test`로 실행하세요.
