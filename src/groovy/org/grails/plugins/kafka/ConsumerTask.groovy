package org.grails.plugins.kafka

import kafka.consumer.ConsumerIterator
import kafka.consumer.KafkaStream
import kafka.javaapi.consumer.ConsumerConnector
import org.codehaus.jackson.map.ObjectMapper

/**
 * Created by parampreet on 7/12/14.
 */
abstract class ConsumerTask implements Runnable {

    private ConsumerIterator<byte[], byte[]> _streamIter = null
    protected ConsumerConnector _connector
    protected ObjectMapper _objectMapper = null

    void setStream(KafkaStream stream)
    {
        if (!_streamIter){
            this._streamIter = stream.iterator()
        }
    }

    public void setConsumerConnector(ConsumerConnector connector){
        this._connector = connector
    }

    public ConsumerConnector getConsumerConnector(){
        return _connector
    }

    private boolean hasNextMsg()
    {
        return _streamIter.hasNext()
    }

    private String getNextMsg()
    {
        return new String(_streamIter.next().message())
    }

    protected final ObjectMapper getObjectMapper(){
        if (!_objectMapper){
            _objectMapper = new ObjectMapper()
        }
        return _objectMapper
    }

    //TODO: pass T for the variable serialized input/output
    protected Map<String,Object> parseJsonMsgAsMap(String msg)
    {
        return objectMapper.readValue(msg, HashMap.class)
    }

    abstract void onReceive(String message)

    @Override
    public void run() throws Exception {
        if(log.isDebugEnabled())
            log.debug("**${name} started and waiting for messages")
        while (hasNextMsg()) {
            String msg = getNextMsg()
            if(log.isDebugEnabled())
                log.debug(msg)
            try {
                onReceive(msg)
            } catch (Exception e) {
                log.error(e)
            }
        }
    }

    protected String getName() {
        this.class.name
    }

}