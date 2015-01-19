package com.cyber.test_client;

import java.io.Serializable;

/**
 * Created by Vadim on 15.01.2015.
 */
public class TestObject implements Serializable{
    public String name;
    public int age;

    public TestObject(String name, int age) {
        this.name = name;
        this.age = age;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TestObject)) return false;

        TestObject that = (TestObject) o;

        if (!name.equals(that.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
