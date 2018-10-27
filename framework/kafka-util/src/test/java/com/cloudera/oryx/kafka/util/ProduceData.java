package com.cloudera.oryx.kafka.util;


import java.util.Objects;
import java.util.Properties;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.settings.ConfigUtils;


public final class ProduceData {

    private static final Logger log = LoggerFactory.getLogger(ProduceData.class);

    private final DatumGenerator<String,String> datumGenerator;
    private final int kafkaPort;
    private final String topic;
    private final int howMany;
    private final int intervalMsec;

    public ProduceData(DatumGenerator<String, String> datumGenerator,
                       int kafkaPort,
                       String topic,
                       int howMany,
                       int intervalMsec) {
        Objects.requireNonNull(datumGenerator);
        Objects.requireNonNull(topic);
        Preconditions.checkArgument(kafkaPort > 0);
        Preconditions.checkArgument(howMany > 0);
        Preconditions.checkArgument(intervalMsec >= 0);

        this.datumGenerator = datumGenerator;
        this.kafkaPort = kafkaPort;
        this.topic = topic;
        this.howMany = howMany;
        this.intervalMsec = intervalMsec;
    }

    public void start() throws InterruptedException {
        RandomGenerator random = RandomManager.getRandom();

        Properties props = ConfigUtils.keyValueToProperties(
                "bootstrap.servers", "localhost:" + kafkaPort,
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "compression.type", "gzip",
                "linger.ms", 0,
                "batch.size", 0,
                "acks", 1,
                "max.request.size", 1 << 26 // TODO
        );

        try (Producer<String, String> producer = new KafkaProducer<String, String>(props)) {
            for (int i = 0; i < howMany; i++) {
                Pair<String, String> datum = datumGenerator.generate(i, random);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, datum.getFirst(),datum.getSecond());
                producer.send(record);
                if (intervalMsec > 0) {
                    Thread.sleep(intervalMsec);
                }
            }
        }

    }




}
