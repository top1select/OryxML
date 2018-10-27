package com.cloudera.oryx.kafka.util;

import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.common.collection.Pair;

@FunctionalInterface
public interface DatumGenerator<K, M> {
    Pair<K, M> generate(int id, RandomGenerator random);
}
