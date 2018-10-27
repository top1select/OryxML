package com.cloudera.oryx.api;

import java.util.Objects;


public final class KeyMessageImpl<K, M> implements KeyMessage<K,M> {
    private final K key;
    private final M message;

    public KeyMessageImpl(K key, M message) {
        this.key = key;
        this.message = message;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public M getMessage() {
        return message;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key) ^ Objects.hashCode(message);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KeyMessageImpl)) {
            return false;
        }

        KeyMessageImpl<K, M> other = (KeyMessageImpl<K, M>) o;
        return Objects.equals(key, other.key) && Objects.equals(message, other.message);
    }

    @Override
    public String toString() {
        return key + "'" + message;
    }

}
