package com.ig.kafka

/**
 * Created by parampreet on 24/11/14.
 */
class ConsumerGroup {
    String groupId
    Map<String, Integer> topicCountMap
    Class<? extends ConsumerTask> consumerTaskClass

    Boolean startConsumerGroup(){
        ConsumerManager.instance.startConsumerGroup(this.groupId)
    }
    Integer getThreadCount(){
        topicCountMap?.collect {it.value}?.sum()?:0
    }
}
