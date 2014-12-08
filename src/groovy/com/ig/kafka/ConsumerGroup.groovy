package com.ig.kafka

/**
 * Created by parampreet on 24/11/14.
 */
class ConsumerGroup {
    String groupId
    Map<String, Integer> topicCountMap
    Class<? extends ConsumerTask> consumerTaskClass

    public Boolean startConsumerGroup() throws Exception {
        ConsumerManager.instance.startConsumerGroup(this.groupId)
    }

    public Boolean stopConsumerGroup() throws Exception {
        ConsumerManager.instance.stopConsumerGroup(this.groupId)
    }

    public Integer getThreadCount() {
        topicCountMap?.collect {it.value}?.sum()?:0
    }
}
