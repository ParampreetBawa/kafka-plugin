package com.test

import grails.test.mixin.TestMixin
import grails.test.mixin.support.GrailsUnitTestMixin
import grails.util.Holders
import org.grails.plugins.kafka.ConsumerGroup
import org.grails.plugins.kafka.ConsumerManager
import org.grails.plugins.kafka.ConsumerStatus
import org.grails.plugins.kafka.test.ConsumerTest
import spock.lang.Specification

/**
 * Created by parampreet on 21/12/14.
 */
@TestMixin(GrailsUnitTestMixin)
class KafkaSpec extends Specification{

    def setup(){
        grailsApplication.config.kafka.zookeeper='localhost:2181'
        config.kafka.consumer.config = [
                        "zookeeper.session.timeout.ms": "400",
                        "zookeeper.sync.time.ms": "200",
                        "auto.commit.interval.ms": "1000"]

        Holders.grailsApplication = grailsApplication
        Holders.servletContext = applicationContext.servletContext
    }
    def "test something"() {
        expect:
        null != Holders.config
        'localhost:2181' == Holders.config.kafka.zookeeper
        group == ConsumerManager.addConsumerGroup(group)
        group.groupId == 'gpid-1'

        Map status = ConsumerManager.getStatus()
        ConsumerStatus.INIT.equals(status.get('gpid-1'))

        true == group.startConsumerGroup()

        Map status2 = ConsumerManager.getStatus()
        1 == status2.size()

        ConsumerStatus.STARTED.equals(status2.get('gpid-1'))

        Exception error
        try{
            ConsumerManager.startConsumerGroup('gpid-1')
        }catch (Exception e){
            error = e
        }

        null != error

        true == ConsumerManager.stopConsumerGroup('gpid-1')

        null == ConsumerManager.status.get('gpid-1')



        where:
        group = new ConsumerGroup(groupId: 'gpid-1',topicCountMap:[test:1],consumerTaskClass:ConsumerTest.class)
    }
}
