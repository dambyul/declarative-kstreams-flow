package com.ci.streams.mapper.lv2;

import com.ci.streams.mapper.RecordMapper;
import org.apache.avro.generic.GenericRecord;

public interface Lv2Mapper
    extends RecordMapper<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {}
