package com.cloudera.oryx.kafka.util;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import kafka.utils.ZkUtils$;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.Pair;

public final class KafkaUtils {

    private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class);

    private static final int ZK_TIMEOUT_MSEC =
            (int) TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);

    private KafkaUtils() {

    }

    public static void maybeCreateTopic(String zkServers, String topic, int partitions) {
        maybeCreateTopic(zkServers,topic,partitions,new Properties());
    }

    public static void maybeCreateTopic(String zkServers, String topic, int partitions,
                                        Properties topicProperties) {
        ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false);
        try {
            if (AdminUtils.topicExists(zkUtils, topic)) {
                log.info("no need to create topic {} ,", topic);
            } else {
                log.info("Creating topic {} with {} partitions", topic, partitions);
                try {
                    AdminUtils.createTopic(
                            zkUtils, topic, partitions, 1, topicProperties, RackAwareMode.Enforced$.MODULE$);
                    log.info("Created topic {}", topic);

                } catch (TopicExistsException re) {

                }
            }
        } finally {
            zkUtils.close();
        }
    }

    public static boolean topicExists(String zkServers, String topic) {
        ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false);
        try {
            return AdminUtils.topicExists(zkUtils, topic);
        }
        finally {
            zkUtils.close();
        }
    }

    public static void deleteTopic(String zkServers, String topic) {
        ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false);

        try {
            if (AdminUtils.topicExists(zkUtils, topic)) {
                log.info("Deleting topic {}", topic);
                AdminUtils.deleteTopic(zkUtils, topic);
                log.info("Deleted Zookeeper topic {}", topic);
            } else {
                log.info("No need to delete topic {} as it does not exist", topic);
            }
        } finally {
            zkUtils.close();
        }
    }

    public static void setOffsets(String zkServers, String groupID, Map<Pair<String, Integer>, Long> offsets) {
        ZkUtils zkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false);
    try {
        offsets.forEach((topicAndPartition, offset) -> {
            String topic = topicAndPartition.getFirst();
            int partition = topicAndPartition.getSecond();
            String partitionOffsetPath = "/consumers/" + groupID + "/offset/" + topic + "/" + partition;
            zkUtils.updatePersistentPath(partitionOffsetPath, Long.toString(offset), ZkUtils$.MODULE$.defaultAcls(false, ""));

        });

    }finally {
        zkUtils.close();
    }
    }

}
