package com.cloudera.oryx.common.lang;

import java.io.Closeable;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.common.io.IOUtils;

public final class OryxShutdownHook implements Runnable {

    private final Deque<Closeable> closeAtShutdown = new LinkedList<>();
    private volatile boolean triggered;

    @Override
    public void run() {
        triggered = true;
        synchronized (closeAtShutdown) {
            closeAtShutdown.forEach(IOUtils::closeQuietly);
        }
    }

    public boolean addCloseable(Closeable closeable) {
        Objects.requireNonNull(closeable);
        Preconditions.checkState(!triggered, "Can't add closeable %s", closeable);
        synchronized (closeAtShutdown) {
            boolean wasFirst = closeAtShutdown.isEmpty();
            closeAtShutdown.push(closeable);
            return wasFirst;
        }
    }
}
