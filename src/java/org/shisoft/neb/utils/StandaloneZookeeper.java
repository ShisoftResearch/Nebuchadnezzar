package org.shisoft.neb.utils;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;

/**
 * Created by shisoft on 16/2/2016.
 */
public class StandaloneZookeeper {

    ServerCnxnFactory standaloneServerFactory;

    public void startZookeeper (int clientPort) throws IOException, InterruptedException {
        int numConnections = 5000;
        int tickTime = 2000;
        String dataDirectory = System.getProperty("java.io.tmpdir");
        File dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();
        ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
        standaloneServerFactory = ServerCnxnFactory.createFactory(clientPort, numConnections);
        standaloneServerFactory.startup(server);
    }

    public void stopZookeeper (){
        standaloneServerFactory.shutdown();
    }
}
