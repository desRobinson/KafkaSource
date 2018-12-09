package io.quell.sourceflow;

import io.quell.dummy.avro.SourceSinkDummyEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.support.GenericMessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;

@EnableBinding(Source.class)
@EnableAutoConfiguration
@Slf4j
public class Emit {

    @Bean
    @InboundChannelAdapter(value = Source.OUTPUT, poller = @Poller(fixedDelay = "5000", maxMessagesPerPoll = "1"))
    public MessageSource<byte[]> timerMessageSource() throws IOException {

        GenericRecord record = buildGenericRecord("HelloWorld", new Date().toString(), SourceSinkDummyEvent.getClassSchema());
        byte[] serializedAvro = buildAvro(record);

        return () -> new GenericMessage<>(serializedAvro);
    }

    private GenericRecord buildGenericRecord(String key, Object value, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        record.put(key, value);

        return record;
    }

    private byte[] buildAvro(GenericRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder e = EncoderFactory.get().binaryEncoder(out, null);
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(record.getSchema());

        datumWriter.write(record, e);
        e.flush();

        return out.toByteArray();
    }
}
