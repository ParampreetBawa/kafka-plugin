package org.grails.plugins.kafka

import grails.util.Holders
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.consumer.KafkaStream
import kafka.javaapi.consumer.ConsumerConnector

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import static org.grails.plugins.kafka.Util.*

/**
 * Created by parampreet on 24/11/14.
 */
@Singleton
class ConsumerManager {
    private static int size
    private static HashMap<String, ConsumerGroup> groups

    private static Integer MAX_TRIES = 15

    private static Integer globalMaxTries

    private static Map<String, ConsumerStatus> _consumerStatusMap = [:]

    private static Map<String,ConsumerConnector> _connectors = [:]

    /**
     * takes Consumer Group as a parameter
     * @param group
     * @return
     * @throws Exception
     */
    public static ConsumerGroup addConsumerGroup(ConsumerGroup group) throws Exception {
        assert group.consumerTaskClass, "taskClass missing"

        if (!groups) {
            groups = new HashMap<String, ConsumerGroup>()
        }

        if (!group.groupId) {
            generateAndAssignGroupId(group)
        }

        if(groups.containsKey(group.groupId)){
            ConsumerConnector connector = _connectors.get(group.groupId)
            connector.commitOffsets()
            connector.shutdown()
            _connectors.remove(group.groupId)
            _consumerStatusMap.remove(group.groupId)
        }

        groups.put(group.groupId, group)
        _consumerStatusMap.put(group.groupId,ConsumerStatus.INIT)
        return group
    }

    /**
     * To start an existing consumer group
     * @param groupId
     * @param maxTries
     * @return
     * @throws Exception
     */
    public Boolean startConsumerGroup(String groupId, Integer maxTries = null) throws Exception {
        if (null == groupId)
            throw new IllegalArgumentException("group id cannot be null.")

        ConsumerGroup group = groups[groupId]
        if (!group)
            throw IllegalStateException("no consumer group with group id :${groupId} found.")

        if(_consumerStatusMap[group].equals(ConsumerStatus.STARTED))
            throw new IllegalStateException("consumer with group id : ${groupId} is already in started state.")

        ExecutorService executor = Executors.newFixedThreadPool(group.threadCount)
        openConsumerStreamsWithRetries(group, executor, maxTries)
        executor.shutdown()
        return true
    }

    /**
     * Stop the consumer group in running mode
     * @param groupId
     * @return
     */
    public Boolean stopConsumerGroup(String groupId){
        if (null == groupId)
            throw new IllegalArgumentException("group id cannot be null.")

        ConsumerGroup group = groups[groupId]
        if (!group)
            throw IllegalStateException("no consumer group with group id :${groupId} found.")

        if(_consumerStatusMap[group].equals(ConsumerStatus.STARTED))
            throw new IllegalStateException("consumer with group id : ${groupId} is not running")

        ConsumerConnector connector = _connectors.remove(groupId)
        connector.commitOffsets()
        connector.shutdown()
        _consumerStatusMap.remove(groupId)
        return true
    }

    /**
     * To Set Global MAX TRIES threshold value
     * @param maxTries
     * @return
     */
    public Integer setMaxTries(Integer maxTries) {
        globalMaxTries = maxTries
    }

    private static void openConsumerStreamsWithRetries(ConsumerGroup group, ExecutorService executor, Integer maxTries = null) {
        Integer attempts = 0
        maxTries = maxTries ?: globalMaxTries ?: MAX_TRIES
        Boolean opened = false
        while (!opened) {
            try {
                openConsumerStreams(group, executor)
                opened = true
                _consumerStatusMap.put(group.groupId, ConsumerStatus.STARTED)
            } catch (Exception e) {
                if (attempts++ == maxTries) {
                    _consumerStatusMap.put(group.groupId, ConsumerStatus.ERROR)
                    _connectors.remove(group.groupId)
                    throw new RuntimeException("Attempts=${attempts}\tException while creating thread group: ${e.toString()}: ${e.message}")
                }
            }
        }
    }

    private static void openConsumerStreams(ConsumerGroup group, ExecutorService executor) {
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(
                createConsumerConfig(zookeeper, group.groupId));
        _connectors.put(group.groupId,consumer)
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(group.topicCountMap)
        messageStreams.each { k, v ->
            if (k in group.topicCountMap.keySet()) {
                v.each { stream ->
                    ConsumerTask task = (ConsumerTask) group.consumerTaskClass.newInstance()
                    task.setStream(stream)
                    task.consumerConnector = consumer
                    executor.submit(task)
                }
            }
        }
    }

    private static synchronized void generateAndAssignGroupId(ConsumerGroup group) {
        group.groupId = "gpid-${group.consumerTaskClass.simpleName}${size ? ('-' + size++) : ''}"
    }

    private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.putAll(config.kafka.consumer.config as Map)
        return new ConsumerConfig(props);
    }

    private static String getZookeeper() {
        config.kafka.zookeeper
    }

}
