package com.ci.streams.mapper.lv1;

import com.ci.streams.avro.SourceEvent;
import com.ci.streams.mapper.RecordMapper;
import org.apache.avro.generic.GenericRecord;

public interface Lv1Mapper extends RecordMapper<String, SourceEvent, String, GenericRecord> {}
