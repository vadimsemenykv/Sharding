package com.cyber.sharding_hw.request_response;

import java.io.Serializable;

/**
 * Created by Vadim on 05.01.2015.
 */
public class MetaResponse implements Serializable{
    String hostname;
    int port;

    public MetaResponse(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }
}
