package com.ci.streams.mapper;

import org.apache.kafka.streams.processor.api.Record;

@FunctionalInterface
public interface RecordMapper<K, V, KR, VR> {
  Record<KR, VR> process(Record<K, V> record);
}
