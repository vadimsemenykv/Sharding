package com.cyber.sharding_hw.request_response;

import java.io.Serializable;

/**
 * Created by Vadim on 05.01.2015.
 */
public class Response implements Serializable{
    private int key;
    private Command command;
    private Status status;
    private Object item;

    /**
     * use this constructor for create, update and delete operations
     * @param key
     * @param command
     * @param status
     */
    public Response(int key, Command command, Status status) {
        this.key = key;
        this.command = command;
        this.status = status;
    }

    /**
     * use this constructor for read operation
     * @param key
     * @param item
     * @param status
     */
    public Response(int key, Object item, Status status) {
        this.key = key;
        this.command = Command.READ;
        this.status = status;
        this.item = item;
    }

    public int getKey() {return key; }

    public Command getCommand() {
        return command;
    }

    public Status getStatus() {
        return status;
    }

    public Object getItem() {
        return item;
    }
}
