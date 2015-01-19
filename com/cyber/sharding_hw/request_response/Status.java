package com.cyber.sharding_hw.request_response;

import java.io.Serializable;

/**
 * Created by Vadim on 05.01.2015.
 */
public enum Status implements Serializable {
    CREATED, READ, UPDATED, DELETED, ABORTED
}
