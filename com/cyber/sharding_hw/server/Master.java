package com.cyber.sharding_hw.server;

import com.cyber.sharding_hw.request_response.MetaRequest;
import com.cyber.sharding_hw.request_response.MetaResponse;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Vadim on 14.01.2015.
 */
public class Master{
    private String ip;
    private ServerSocket serverSocket;
    private List<Slave> slaveList;
    private Thread masterThread;
    private List<MasterConnection> masterConnections;

    final private int port;

    private int maxShardSize;
    private int maxConnections;


    public Master(String ip, int port, int maxShardSize, int maxConnections) {
        this.ip = ip;
        this.port = port;
        this.maxShardSize = maxShardSize;
        this.maxConnections = maxConnections;

        slaveList = Collections.synchronizedList(new ArrayList<Slave>());
        slaveList.add(new Slave(ip, port + 2, 10));
        slaveList.add(new Slave(ip, port + 4, 15));
//        slaveList.add(new Slave("192.168.33.10", port + 2, 10));
//        slaveList.add(new Slave("192.168.33.10", port + 4, 15));

        masterConnections = Collections.synchronizedList(new ArrayList<MasterConnection>());

        startMaster();
    }

    private void startMaster() {
        masterThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    serverSocket = new ServerSocket(port);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                while (!Thread.currentThread().isInterrupted() && !serverSocket.isClosed()) {
                    try {
                        Socket socket = serverSocket.accept();
                        while (true) {
                            if (masterConnections.size() < maxConnections) {
                                MasterConnection masterConnection = new MasterConnection(socket);
                                masterConnections.add(masterConnection);
                                Thread connThread = new Thread(masterConnection);
                                connThread.start();
                                break;
                            }
                        }
                    } catch (SocketException e) {

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        masterThread.start();
    }

    public boolean stop() {
        try {
            serverSocket.close();
            for (MasterConnection mc : masterConnections) mc.stop();
            for (Slave sl : slaveList) sl.stop();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("server stopped");
        return true;
    }

    public boolean softStop() {
        try {
            serverSocket.close();
            for (MasterConnection mc : masterConnections) mc.stop();
            for (Slave sl : slaveList) sl.softStop();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("server stopped");
        return true;
    }

    public String status() {
        String status = "";
        for (Slave sl : slaveList) status += sl.getStatus() + "\n";
        return status;
    }

    private class MasterConnection implements Runnable {
        private Socket socket;

        private MasterConnection(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream())) {

                MetaRequest metaRequest = (MetaRequest) objectInputStream.readObject();
                int key = metaRequest.getKey();

                int slaveNum = Math.abs(key % slaveList.size());
                Slave sl = slaveList.get(slaveNum);
                MetaResponse metaResponse = new MetaResponse(sl.getHostName(), sl.getPort());

                System.out.println("get request with key = " + key);
                System.out.println("send response with slave host = " + sl.getHostName() + " slave port = " + sl.getPort());

                objectOutputStream.writeObject(metaResponse);
                objectOutputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }  catch (ClassCastException e) {
                e.printStackTrace();
            }

            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            masterConnections.remove(this);
        }

        public void stop() {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MasterConnection)) return false;

            MasterConnection that = (MasterConnection) o;

            if (!socket.equals(that.socket)) return false;

            return true;
        }
    }
}
