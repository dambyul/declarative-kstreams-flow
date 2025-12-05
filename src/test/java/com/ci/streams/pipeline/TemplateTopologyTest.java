package com.ci.streams.pipeline;

import static org.junit.jupiter.api.Assertions.*;

import com.ci.streams.config.PipelineConfig;
import com.ci.streams.config.PipelineDefinition;
import com.ci.streams.util.SerdeFactory;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TemplateTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<GenericRecord, GenericRecord> outputTopic;

    private Serde<GenericRecord> keyGenericAvroSerde;
    private Serde<GenericRecord> valueGenericAvroSerde;
    private SchemaRegistryClient schemaRegistryClient;

    private static final String SCHEMA_REGISTRY_SCOPE = "TemplateTopologyTest";
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String FAIL_TOPIC = "fail-topic";

    @BeforeEach
    void setUp() throws Exception {
        schemaRegistryClient = new MockSchemaRegistryClient();

        registerSchema("avro/SourceEvent.avsc", "input-topic-value");
        Schema lv1Schema = registerSchema("avro/lv1/SampleUser.avsc", OUTPUT_TOPIC + "-value");

        Schema.Field custIdField = lv1Schema.getField("user_id");
        Schema keySchema = Schema.createRecord("SampleUserKey", null, "com.ci.streams.avro.key", false);
        keySchema.setFields(
                List.of(new Schema.Field("user_id", custIdField.schema(), custIdField.doc(), null)));
        schemaRegistryClient.register(OUTPUT_TOPIC + "-key", keySchema);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-template-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put("schema.registry.url", MOCK_SCHEMA_REGISTRY_URL);
        props.put("id.compatibility.strict", "false");
        props.put("avro.reflection.allow.null", "true");

        keyGenericAvroSerde = SerdeFactory.createGenericAvroSerde(props, true);
        valueGenericAvroSerde = SerdeFactory.createGenericAvroSerde(props, false);

        PipelineDefinition params = new PipelineDefinition();
        params.setSourceTopic(INPUT_TOPIC);
        params.setDestinationTopic(OUTPUT_TOPIC);
        params.setFailTopic(FAIL_TOPIC);
        params.setSchemaName("SampleUser");
        params.setMapperName("SampleUserMapper");
        params.setPrimaryKeyFields(List.of("user_id"));
        // Channel logic test config
        params.setChannel("ConfigChannel");

        PipelineConfig config = new PipelineConfig();
        config.setName("TestTask");
        config.setType("LV1_PROCESSOR");
        config.setParams(params);

        StreamTask task = new JsonDebeziumProcessorTask("LV1_PROCESSOR");
        StreamsBuilder builder = new StreamsBuilder();
        task.buildPipeline(builder, props, config);
        Topology topology = builder.build();

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(
                INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic(
                OUTPUT_TOPIC, keyGenericAvroSerde.deserializer(), valueGenericAvroSerde.deserializer());
    }

    private Schema registerSchema(String resourcePath, String subject) throws Exception {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null)
                throw new RuntimeException("Schema not found: " + resourcePath);
            Schema schema = new Schema.Parser().parse(is);
            schemaRegistryClient.register(subject, schema);
            return schema;
        }
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null)
            testDriver.close();
        if (keyGenericAvroSerde != null)
            keyGenericAvroSerde.close();
        if (valueGenericAvroSerde != null)
            valueGenericAvroSerde.close();
    }

    @Test
    void testCreateOperation() {
        String json = "{"
                + "\"schema\": {},"
                + "\"payload\": {"
                + "  \"op\": \"c\","
                + "  \"after\": {"
                + "    \"id\": \"USER_001\","
                + "    \"name\": \"Test User\","
                + "    \"email\": \"test@gmail.com\","
                + "    \"phone_number\": \"01012345678\","
                + "    \"active_yn\": \"Y\","
                + "    \"created_at\": 1704067200000"
                + "  }"
                + "}"
                + "}";

        inputTopic.pipeInput("key1", json);

        assertFalse(outputTopic.isEmpty());
        KeyValue<GenericRecord, GenericRecord> record = outputTopic.readKeyValue();

        GenericRecord key = record.key;
        assertEquals("USER_001", key.get("user_id").toString());

        GenericRecord value = record.value;
        assertEquals("USER_001", value.get("user_id").toString());
        assertEquals("Test User", value.get("user_name").toString());
        assertEquals("test@gmail.com", value.get("email").toString());
        assertEquals("010-1234-5678", value.get("phone").toString());
        assertEquals(true, value.get("is_active"));
        assertEquals("c", value.get("__op").toString());
        assertNotNull(value.get("__processed_at"));
    }

    @Test
    void testDeleteOperation() {
        String json = "{"
                + "\"schema\": {},"
                + "\"payload\": {"
                + "  \"op\": \"d\","
                + "  \"before\": {"
                + "    \"id\": \"USER_002\","
                + "    \"name\": \"Deleted User\""
                + "  },"
                + "  \"after\": null"
                + "}"
                + "}";

        inputTopic.pipeInput("key2", json);

        assertFalse(outputTopic.isEmpty());
        KeyValue<GenericRecord, GenericRecord> record = outputTopic.readKeyValue();

        assertEquals("USER_002", record.key.get("user_id").toString());

        GenericRecord value = record.value;
        assertEquals("d", value.get("__op").toString());
        assertEquals("USER_002", value.get("user_id").toString());
    }
}
