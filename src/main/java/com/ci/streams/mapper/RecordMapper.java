package com.ci.streams.mapper;

import org.apache.kafka.streams.processor.api.Record;

/** 레코드 매퍼 인터페이스. 입력 레코드를 다른 형태의 레코드로 변환합니다. */
@FunctionalInterface
public interface RecordMapper<K, V, KR, VR> {
  Record<KR, VR> process(Record<K, V> record);
}
