package com.cyber.sharding_hw.request_response;

import java.io.Serializable;

/**
 * Created by Vadim on 05.01.2015.
 */
public class MetaRequest implements Serializable{
    private int key;

    public MetaRequest(int key) {
        this.key = key;
    }

    public int getKey() {
        return key;
    }
}
