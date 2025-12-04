# Declarative Kafka Streams Flow

본 프로젝트는 Kafka Streams 애플리케이션을 빠르고 표준화된 방식으로 개발하기 위한 템플릿입니다.
복잡한 Topology 코드를 직접 작성하는 대신, **설정 파일**(`pipelines.yml`)과 **데이터 매핑 로직**(Mapper)만 정의하면 스트림즈 애플리케이션을 구동할 수 있도록 설계되었습니다.

Debezium을 통해 수집된 CDC 데이터를 Kafka Streams로 실시간 전처리 및 변환하여, 다양한 타겟 시스템으로 안정적으로 전달하는 역할을 수행할 수 있습니다.

## 1. 주요 기능 (Key Features)

*   **실시간 데이터 파이프라인**: Source Topic의 데이터를 실시간으로 소비하여 변환 후 Destination Topic으로 전달합니다.
*   **유연한 아키텍처**: `pipelines.yml` 설정 파일을 통해 파이프라인 토폴로지(Source, Mapper, Sink)를 동적으로 구성할 수 있습니다.
*   **템플릿 기반 설정**: 반복되는 파이프라인 설정을 Template으로 정의하고 재사용하여 설정 관리를 효율화합니다.
*   **보안 강화 (SSL/PEM)**: JKS 변환 없이 PEM 형식의 인증서를 직접 사용하여 Kafka에 안전하게 연결할 수 있습니다.
*   **다양한 데이터 포맷**: JSON, Avro 등 다양한 데이터 포맷을 지원하며, Schema Registry와 연동됩니다.

## 2. 프로젝트 구조 (Project Structure)

```
src/main/java/com/ci/streams/
├── config/         # 파이프라인 설정 모델 (PipelineDefinition 등)
├── pipeline/       # Kafka Streams 토폴로지 및 태스크 정의
├── processor/      # 데이터 처리 프로세서
├── mapper/         # 데이터 매핑 및 변환 로직 (사용자 구현 영역)
├── smt/            # Kafka Connect SMT (예: TombstoneOnDelete)
└── util/           # 공통 유틸리티 (보안, 환경변수, SerDe 등)
```

*   `src/main/resources/pipelines.yml`: **핵심 설정 파일**. 스트림즈의 전체 흐름을 정의합니다.
*   `src/main/resources/avro/`: Avro 스키마 파일(.avsc) 위치.

## 3. 시작하기 (Getting Started)

### 3.1. 필수 요구 사항
*   Java 17 이상
*   Maven 3.8 이상
*   Kafka Cluster 및 Schema Registry

### 3.2. 빌드
Maven을 사용하여 프로젝트를 빌드합니다. Avro 스키마로부터 Java 소스가 자동 생성됩니다.
```bash
mvn spotless:apply  # 코드 포맷팅
mvn clean install   # 빌드 및 테스트
```

### 3.3. 실행
생성된 JAR 파일을 실행합니다.
```bash
java -jar target/kstreams.jar
```

운영 환경에서는 외부 설정 파일을 지정할 수 있습니다.
```bash
java -Dconfig.file=/path/to/prod_pipelines.yml -jar target/kstreams.jar
```

## 4. 환경 설정 (Configuration)

애플리케이션 실행을 위해 `.env` 파일 또는 환경 변수에 다음 값들을 설정해야 합니다.

| 변수명 | 설명 | 예시 |
|---|---|---|
| `KAFKA_URL` | Kafka 브로커 주소 목록 | `kafka+ssl://broker:9092` |
| `APPLICATION_ID` | Kafka Streams 애플리케이션 ID | `my-kstreams-app` |
| `SCHEMA_REGISTRY_URL` | Schema Registry 주소 | `http://schema-registry:8081` |
| `STATE_DIR` | 상태 저장소 경로 | `/tmp/kstreams-state` |
| `NUM_STREAM_THREADS` | 스트림 처리 스레드 수 | `32` |

### 보안 설정 (SSL/PEM)
PEM 인증서 파일을 사용하는 경우 (권장):
*   `KAFKA_TRUSTED_CERT_FILE`: CA 인증서 경로
*   `KAFKA_CLIENT_CERT_FILE`: 클라이언트 인증서 경로
*   `KAFKA_CLIENT_CERT_KEY_FILE`: 클라이언트 키 경로

환경 변수에 직접 PEM 내용을 입력하는 경우:
*   `KAFKA_TRUSTED_CERT`
*   `KAFKA_CLIENT_CERT`
*   `KAFKA_CLIENT_CERT_KEY`

## 5. 개발 가이드 (Development Guide)

새로운 파이프라인을 추가하는 방법:

1.  **Avro 스키마 정의**: `src/main/resources/avro/`에 `.avsc` 파일 추가.
2.  **Mapper 구현**: `src/main/java/com/ci/streams/mapper/`에 매퍼 클래스 구현 (`RecordMapper` 구현 또는 `Abstract...Mapper` 상속).
3.  **pipelines.yml 등록**: `src/main/resources/pipelines.yml`에 파이프라인 설정 추가.


## 6. 파이프라인 설정 (Pipeline Configuration)

`pipelines.yml` 파일에서 사용할 수 있는 주요 파라미터는 다음과 같습니다.

| 파라미터명 | 설명 | 필수 여부 | 사용 범위 | 예시 |
|---|---|---|---|---|
| `name` | 파이프라인의 고유 이름 | **Yes** | Common | `Lv1SamplePipeline` |
| `sourceTopic` | 데이터를 읽어올 Kafka 토픽 | **Yes** | Common | `bronze.sample_user` |
| `destinationTopic` | 데이터를 저장할 Kafka 토픽 | **Yes** | Common | `silver.sample_user` |
| `mapperName` | 데이터 변환을 수행할 Mapper 클래스 이름 (Bean Name) | **Yes** | Common | `SampleUserMapper` |
| `schemaName` | 출력 데이터의 Avro 스키마 이름 | **Yes** | Common | `SampleUser` |
| `failTopic` | 처리 실패 시 데이터를 보낼 DLQ 토픽 | No | Common | `error.lv1` |
| `primaryKeyFields` | 레코드의 Key로 사용할 필드 목록 | No | Common | `["cust_id"]` |
| `inputFormat` | 입력 데이터 포맷 (JSON, AVRO) | No | LV1 | `JSON` |
| `inputFields` | 입력 데이터에서 필요한 필드 목록 | No | API | `["cust_address"]` |
| `targetTemplates` | 적용할 템플릿 이름 목록 | No | LV2 | `["UnionTemplate"]` |
| `pipelineNamePrefix` | 파이프라인 이름 접두사 (템플릿 사용 시) | No | LV2 | `Union` |
| `pipelineNameSuffix` | 파이프라인 이름 접미사 (템플릿 사용 시) | No | LV2 | `CH1` |
| `channel` | 채널 식별자 (필요 시) | No | LV2 | `SampleCH-1` |
| `keyType` | Key의 데이터 타입 (String, Avro 등) | No | LV2 | `String` |
| `keyFormat` | Key 포맷팅 문자열 | No | LV2 | `{phone}` |

## 7. 코드 품질

*   **Spotless**: Google Java Style 가이드를 준수합니다. (`mvn spotless:apply`)
*   **PMD/CPD**: 정적 분석을 통해 코드 품질을 유지합니다.
