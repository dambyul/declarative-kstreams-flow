package com.ci.streams.mapper.api;

import com.ci.streams.mapper.RecordMapper;
import org.apache.avro.generic.GenericRecord;

public interface ApiMapper extends RecordMapper<String, GenericRecord, String, GenericRecord> {}
