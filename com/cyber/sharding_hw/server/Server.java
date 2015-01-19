package com.cyber.sharding_hw.server;

import com.cyber.sharding_hw.request_response.MetaRequest;
import com.cyber.sharding_hw.request_response.MetaResponse;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

/**
 * Created by Vadim on 05.01.2015.
 */
public class Server {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
//        System.out.println("To start Server put \"Y\"");
//        String str = scanner.nextLine();
        String str = "y";

        System.out.println("Set server ip address:");
        String ip = scanner.nextLine();

        if (str.toLowerCase().equals("y")) {
            Master master = new Master(ip, 3366, 5, 10);
            System.out.println("To get Server status put \"status\"\n" +
                    "To soft stop server put \"soft stop\"\n" +
                    "To stop server put \"stop\"");

            while (!str.toLowerCase().equals("stop") && !str.toLowerCase().equals("soft stop")) {
                str = scanner.nextLine();
                if (str.toLowerCase().equals("stop")) master.stop();
                if (str.toLowerCase().equals("soft stop")) master.softStop();
                if (str.toLowerCase().equals("status")) System.out.println(master.status());
            }
            scanner.close();
        }
    }
}
