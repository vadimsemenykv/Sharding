package com.cyber.test_client;

import com.cyber.sharding_hw.client.Client;

import java.util.Scanner;

/**
 * Created by Vadim on 15.01.2015.
 */
public class TestClient {
    public static void main(String[] args) {
        System.out.println("Start client");
        Scanner scanner = new Scanner(System.in);
        System.out.println("Set server ip address:");
        String ip = scanner.nextLine();
        scanner.close();

//        Client client = new Client("192.168.33.10", 3366);
        Client client = new Client(ip, 3366);
        System.out.println("write objects");
        client.createObj(new TestObject("Alex", 43));
        client.createObj(new TestObject("Sara", 47));
        client.createObj(new TestObject("Greg", 21));

        System.out.println("read objects\n");
        TestObject alex = (TestObject) client.readObj("Alex".hashCode());
        TestObject sara = (TestObject) client.readObj("Sara".hashCode());
        TestObject greg = (TestObject) client.readObj("Greg".hashCode());

        System.out.println(alex.age);
        System.out.println(sara.age);
        System.out.println(greg.age);
    }
}
