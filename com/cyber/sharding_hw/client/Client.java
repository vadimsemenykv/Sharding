package com.cyber.sharding_hw.client;

import com.cyber.sharding_hw.request_response.*;
import com.cyber.test_client.TestObject;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by Vadim on 05.01.2015.
 */
public class Client {
    private String masterHost;
    private int masterPort;

    public Client(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    public boolean createObj(Object o) {
        MetaResponse metaResponse = getMetaResponse(o.hashCode());

        if (metaResponse != null && metaResponse.getHostname() != null && metaResponse.getPort() != 0) {
            try (Socket socketToShard = new Socket(metaResponse.getHostname(), metaResponse.getPort());
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketToShard.getOutputStream());
                 ObjectInputStream objectInputStream = new ObjectInputStream(socketToShard.getInputStream())) {

                Request request = new Request(o.hashCode(), o, Command.CREATE);
                objectOutputStream.writeObject(request);
                objectOutputStream.flush();

                Response response = (Response) objectInputStream.readObject();

                System.out.println("Object with key = " + response.getKey() +
                        " Command = " + response.getCommand() + " was " + response.getStatus());

                if (response.getStatus() == Status.ABORTED) return false;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                return false;
            } catch (ClassCastException e) {
                return false;
            }
        } else {
            return false;
        }

        return true;
    }

    public Object readObj(int key) {
        MetaResponse metaResponse = getMetaResponse(key);

        if (metaResponse != null && metaResponse.getHostname() != null && metaResponse.getPort() != 0) {
            try (Socket socketToShard = new Socket(metaResponse.getHostname(), metaResponse.getPort());
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketToShard.getOutputStream());
                 ObjectInputStream objectInputStream = new ObjectInputStream(socketToShard.getInputStream())) {

                Request request = new Request(key, Command.READ);
                objectOutputStream.writeObject(request);
                objectOutputStream.flush();

                Response response = (Response) objectInputStream.readObject();

                System.out.println("Object with key = " + response.getKey() +
                        " Command = " + response.getCommand() + " was " + response.getStatus());

                if (response.getStatus() == Status.ABORTED) return false;

                return response.getItem();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                return null;
            } catch (ClassCastException e) {
                return null;
            }
        } else {
            return null;
        }
    }

    public boolean updateObj(Object o) {
        MetaResponse metaResponse = getMetaResponse(o.hashCode());

        if (metaResponse != null && metaResponse.getHostname() != null && metaResponse.getPort() != 0) {
            try (Socket socketToShard = new Socket(metaResponse.getHostname(), metaResponse.getPort());
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketToShard.getOutputStream());
                 ObjectInputStream objectInputStream = new ObjectInputStream(socketToShard.getInputStream())) {

                Request request = new Request(o.hashCode(), o, Command.UPDATE);
                objectOutputStream.writeObject(request);
                objectOutputStream.flush();

                Response response = (Response) objectInputStream.readObject();

                System.out.println("Object with key = " + response.getKey() +
                        " Command = " + response.getCommand() + " was " + response.getStatus());

                if (response.getStatus() == Status.ABORTED) return false;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                return false;
            } catch (ClassCastException e) {
                return false;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean deleteObj(int key) {
        MetaResponse metaResponse = getMetaResponse(key);

        if (metaResponse != null && metaResponse.getHostname() != null && metaResponse.getPort() != 0) {
            try (Socket socketToShard = new Socket(metaResponse.getHostname(), metaResponse.getPort());
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketToShard.getOutputStream());
                 ObjectInputStream objectInputStream = new ObjectInputStream(socketToShard.getInputStream())) {

                Request request = new Request(key, Command.DELETE);
                objectOutputStream.writeObject(request);
                objectOutputStream.flush();

                Response response = (Response) objectInputStream.readObject();

                System.out.println("Object with key = " + response.getKey() +
                        " Command = " + response.getCommand() + " was " + response.getStatus());

                if (response.getStatus() == Status.ABORTED) return false;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                return false;
            } catch (ClassCastException e) {
                return false;
            }
        } else {
            return false;
        }

        return true;
    }

    private MetaResponse getMetaResponse(int key) {
        try (Socket socketToMaster = new Socket(masterHost, masterPort);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketToMaster.getOutputStream());
             ObjectInputStream objectInputStream = new ObjectInputStream(socketToMaster.getInputStream())) {

            objectOutputStream.writeObject(new MetaRequest(key));
            objectOutputStream.flush();

            MetaResponse metaResponse = (MetaResponse) objectInputStream.readObject();

            return metaResponse;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } catch (ClassNotFoundException e) {
            return null;
        } catch (ClassCastException e) {
            return null;
        }
    }
}
