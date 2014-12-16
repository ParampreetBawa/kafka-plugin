package org.grails.plugins.kafka

import groovy.util.logging.Log4j
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.SerializationConfig
import static org.grails.plugins.kafka.Util.*

/**
 * Created by parampreet on 15/12/14.
 */
@Log4j
class Producer {
    private static kafka.javaapi.producer.Producer<String, String> _producer = null
    private static ObjectMapper _om = null;

    static {
        _om = new ObjectMapper();
        _om.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false)
    }

    //TODO: we should move out properties to some config file
    private static ProducerConfig getConfig() {
        String brokers = config.kafka.broker.list
        assert brokers, "Kafka broker list not found"

        Properties props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        return config
    }


    private static kafka.javaapi.producer.Producer<String, String> getProducer() {
        if (!_producer) {
            _producer = new kafka.javaapi.producer.Producer<String, String>(config);
        }
        return _producer
    }

    public static void send(String topic, Map<String, Object> eventData) {
        if(!isEnabled()){
            log.warn("Kafka Event Producer is disabled.")
            return
        }

        topic = topic.toLowerCase()
        String data = _om.writeValueAsString(eventData)
        String key = null
        KeyedMessage<String, String> km= new KeyedMessage(topic, key, data)
        if(log.isDebugEnabled()) {
            log.debug("producing to topic:${topic} with data: ${data}")
        }
        producer.send(km)
    }

    private static Boolean isEnabled(){
        return config.kafka?.producer?.enabled?:false
    }
}
