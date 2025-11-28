package com.ci.streams.processor;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Record;

@FunctionalInterface
public interface RecordProcessor<K, V> {
  GenericRecord process(Record<K, V> record) throws Exception;
}
