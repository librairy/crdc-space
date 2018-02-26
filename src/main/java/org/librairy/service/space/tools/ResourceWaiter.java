package org.librairy.service.space.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;

/**
 * @author Badenes Olmedo, Carlos <cbadenes@fi.upm.es>
 */
public class ResourceWaiter {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceWaiter.class);

    public static Boolean waitFor(String host, Integer port){

        Random random = new Random();
        while(true){
            try{
                Socket ServerSok = new Socket(host,port);
                LOG.debug("Port in use: " + port + " at " + host );
                ServerSok.close();
                break;
            } catch (UnknownHostException e) {
                LOG.debug("Unreachable host: " + host + ":" + port, e);
            } catch (IOException e) {
                LOG.debug("Connection error to host: " + host + ":" + port, e);
            } catch (Exception e){
                LOG.debug("Connection error to host: " + host + ":" + port, e);
            }
            //int delay = random.nextInt(100) * 250;
            int delay = 2000;
            LOG.info("Waiting for '" + host+":"+port +"' ...");
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                break;
            }
        }
        return true;
    }
}
