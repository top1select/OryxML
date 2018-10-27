package com.cloudera.oryx.common.collection;

import java.io.Serializable;
import java.util.Objects;

/**
 * Encapsulates a pair of objects.
 *
 * @param <A> first object type
 * @param <B> second object type
 */
public final class Pair<A,B> implements Serializable {

    private final A first;
    private final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public A getFirst() {
        return first;
    }

    public B getSecond() {
        return second;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(first) ^ Objects.hashCode(second);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Pair)) {
            return false;
        }
        @SuppressWarnings("unchecked")
        Pair<A,B> other = (Pair<A,B>) o;
        return Objects.equals(first, other.first) && Objects.equals(second, other.second);
    }

    @Override
    public String toString() {
        return first + "," + second;
    }

}
