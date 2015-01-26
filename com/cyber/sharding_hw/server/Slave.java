package com.cyber.sharding_hw.server;

import com.cyber.sharding_hw.request_response.*;
import com.cyber.sharding_hw.request_response.Status;
import com.cyber.test_client.TestObject;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Created by Vadim on 13.01.2015.
 */
public class Slave {
//    private boolean master;
    private ServerSocket serverSocket;
    private Thread slaveThread;
    private String hostName;
    private int port;
    private int maxConnections;
    private Set<Integer> keyList;
    private List<SlaveConnection> slaveConnections;

    public Slave(String hostName, int port, int maxConnections) {
        this.hostName = hostName;
        this.port = port;
        this.maxConnections = maxConnections;

        keyList = Collections.synchronizedSet(new HashSet<Integer>());
        slaveConnections = Collections.synchronizedList(new ArrayList<SlaveConnection>());

        startSlave();
    }

    private void startSlave() {
        slaveThread = new Thread(new Runnable() {
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
                            if (slaveConnections.size() < maxConnections) {
                                SlaveConnection slaveConnection = new SlaveConnection(socket);
                                slaveConnections.add(slaveConnection);
                                Thread connThread = new Thread(slaveConnection);
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
        slaveThread.start();
    }

    public boolean stop() {
        try {
            serverSocket.close();
            for (SlaveConnection sc : slaveConnections) sc.stop();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("slave " + hostName + ":" + port + " stopped");
        return true;
    }

    public boolean softStop() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        synchronized (this) {
            while (!slaveConnections.isEmpty()) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("slave " + hostName + ":" + port + " stopped");
        }
        return true;
    }

    public String getHostName() {
        return hostName;
    }

    public int getPort() {
        return port;
    }

    public String getStatus() {
        String keys = keyList.toString();
        return "Shard host = " + hostName + " port = " + port + "\n" +
                " max available connections to shard = " + maxConnections + "\n" +
                " now connected to shard = " + slaveConnections.size() + "\n" +
                " num of objects saved at shard = " + keyList.size() + "\n" +
                " key of objects saved at shard:\n" + " " + keys;
    }

    private class SlaveConnection implements Runnable {
        private Socket socket;

        private SlaveConnection(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream())) {

                Request request = (Request) objectInputStream.readObject();

                Status status = Status.ABORTED;
                Response response = null;
                switch (request.getCommand()) {
                    case CREATE:
                        status = create(request.getKey(), request.getItem());
                        response = new Response(request.getKey(), request.getCommand(), status);
                        break;
                    case READ:
                        Object obj = read(request.getKey());
                        if (obj != null) status = Status.READ;
                        response = new Response(request.getKey(), obj, status);
                        break;
                    case UPDATE:
                        status = update(request.getKey(), request.getItem());
                        response = new Response(request.getKey(), request.getCommand(), status);
                        break;
                    case DELETE:
                        status = delete(request.getKey());
                        response = new Response(request.getKey(), request.getCommand(), status);
                        break;
                    default:
                        break;
                }

                objectOutputStream.writeObject(response);
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
            slaveConnections.remove(this);
            synchronized (Slave.this) {
                Slave.this.notifyAll();
            }
        }

        private Status create(final int key, Object item) {
            File dir = new File(String.valueOf(port));
            if (!dir.exists()) dir.mkdir();

            try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(new File(dir + "/" +String.valueOf(key))))) {
                objectOutputStream.writeObject(item);
                objectOutputStream.flush();

                keyList.add(new Integer(key));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return Status.ABORTED;
            } catch (IOException e) {
                e.printStackTrace();
                return Status.ABORTED;
            }
            return Status.CREATED;
        }

        private Object read(int key) {
            File dir = new File(String.valueOf(port));
            if (!dir.exists()) dir.mkdir();

            File f = new File(dir + "/" + String.valueOf(key));
            if (f.exists()) {
                try (ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(new File(dir + "/" +String.valueOf(key))))) {
                    return objectInputStream.readObject();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }

        private Status update(int key, Object item) {
            return create(key, item) == Status.ABORTED ? Status.ABORTED : Status.UPDATED;
        }

        private Status delete(int key) {
            File dir = new File(String.valueOf(port));
            if (!dir.exists()) dir.mkdir();

            File f = new File(dir + "/" + String.valueOf(key));
            if (f.exists()) {
                f.delete();
                keyList.remove(new Integer(key));
                return Status.DELETED;
            }
            return Status.ABORTED;
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
            if (!(o instanceof SlaveConnection)) return false;

            SlaveConnection that = (SlaveConnection) o;

            if (!socket.equals(that.socket)) return false;

            return true;
        }
    }
}
