package com.cloudera.oryx.kafka.util;

import java.util.Collections;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.common.collection.CloseableIterator;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class ConsumeData implements Iterable<KeyMessage<String, String>> {


    private final String topic;
    private final int maxMessageSize;
    private final int kafkaPort;

    public ConsumeData(String topic, int kafkaPort) {
        this(topic, 1 << 16, kafkaPort);
    }

    public ConsumeData(String topic, int maxMessageSize, int kafkaPort) {
        this.topic = topic;
        this.maxMessageSize = maxMessageSize;
        this.kafkaPort = kafkaPort;

    }

    @Override
    public CloseableIterator<KeyMessage<String, String>> iterator() {
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(
                ConfigUtils.keyValueToProperties(
                "group.id", "OryxGroup-ConsumeData",
                "bootstrap.servers", "localhost:" + kafkaPort,
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "max.partition.fetch.bytes", maxMessageSize,
                "auto.offset.reset", "earliest"
        ));

        consumer.subscribe(Collections.singletonList(topic));
        return new ConsumeDataIterator<>(consumer);
    }
}
