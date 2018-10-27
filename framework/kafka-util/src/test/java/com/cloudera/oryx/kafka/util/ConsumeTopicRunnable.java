package com.cloudera.oryx.kafka.util;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.cloudera.oryx.api.KeyMessage;


public final class ConsumeTopicRunnable implements Callable<Void> {

    private final Iterator<KeyMessage<String,String>> data;
    private final List<KeyMessage<String,String>> keyMessages;
    private final CountDownLatch runLatch;
    private final CountDownLatch messagesLatch;

    public ConsumeTopicRunnable(Iterator<KeyMessage<String,String>> data) {
        this(data, 0);
    }

    public ConsumeTopicRunnable(Iterator<KeyMessage<String,String>> data, int expectedMessages) {
        this.data = data;
        this.keyMessages = new ArrayList<>();
        this.runLatch = new CountDownLatch(1);
        this.messagesLatch = new CountDownLatch(expectedMessages);
    }

    @Override
    public Void call() {
        runLatch.countDown();
        data.forEachRemaining(datum -> {
            keyMessages.add(datum);
            messagesLatch.countDown();
        });

        return null;
    }

    public void awaitRun() throws InterruptedException {
        runLatch.await();
    }

    public void awaitMessages() throws InterruptedException {

        if (!messagesLatch.await(1, TimeUnit.MINUTES)) {
            throw new IllegalStateException(

            );
        }
    }

    public List<KeyMessage<String, String>> getKeyMessages() {
        return keyMessages;
    }

    List<String> getKeys() {
        return keyMessages.stream().map(KeyMessage::getKey).collect(Collectors.toList());
    }
}
