package com.cyber.sharding_hw.request_response;

import java.io.Serializable;

/**
 * Created by Vadim on 05.01.2015.
 */
public class Request implements Serializable{
    private int key;
    private Object item;
    private Command command;

    /**
     * use this constructor for create and update operations
     * @param key
     * @param item
     * @param command
     */
    public Request(int key, Object item, Command command) {
        this.key = key;
        this.item = item;
        this.command = command;
    }

    /**
     * use this constructor for read and delete operations
     * @param key
     * @param command
     */
    public Request(int key, Command command) {
        this.key = key;
        this.command = command;
    }

    public int getKey() {
        return key;
    }

    public Object getItem() {
        return item;
    }

    public Command getCommand() {
        return command;
    }
}
