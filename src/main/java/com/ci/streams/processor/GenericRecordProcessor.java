package com.ci.streams.processor;

import com.ci.streams.mapper.RecordMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Record;

public class GenericRecordProcessor<K, V, KR, VR> implements RecordProcessor<K, V> {
  private final RecordMapper<K, V, KR, VR> mapper;

  public GenericRecordProcessor(RecordMapper<K, V, KR, VR> mapper) {
    this.mapper = mapper;
  }

  @Override
  public GenericRecord process(Record<K, V> record) throws Exception {
    Record<KR, VR> outputRecord = mapper.process(record);
    return (outputRecord != null) ? (GenericRecord) outputRecord.value() : null;
  }
}
