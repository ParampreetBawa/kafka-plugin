package org.grails.plugins.kafka

import grails.util.Holders

/**
 * Created by parampreet on 15/12/14.
 */
class Util {
    public static ConfigObject getConfig() {
        return Holders.config
    }
}
