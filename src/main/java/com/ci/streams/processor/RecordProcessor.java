package com.ci.streams.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Record;

/** 레코드 처리 함수형 인터페이스. 입력 레코드를 처리하여 GenericRecord로 반환합니다. */
@FunctionalInterface
public interface RecordProcessor<K, V> {
  GenericRecord process(Record<K, V> record) throws Exception;
}
