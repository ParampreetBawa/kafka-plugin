package org.grails.plugins.kafka.test

import org.grails.plugins.kafka.ConsumerTask

/**
 * Created by parampreet on 21/12/14.
 */
class ConsumerTest extends ConsumerTask {
    void onReceive(String message){
        println(message)
    }
}
