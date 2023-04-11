package cis5550.webserver;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import cis5550.tools.Logger;

public class Server {
  private static final int NUM_WORKERS = 100;

  private static final Logger logger = Logger.getLogger(Server.class);

  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("Written by Charles Cheng");
      return;
    }

    int portNumber = Integer.parseInt(args[0]);
    String directory = args[1];

    BlockingQueue<Socket> queue = new LinkedBlockingQueue<>();

    try {
      ServerSocket serverSocket = new ServerSocket(portNumber);

      for (int i = 0; i < NUM_WORKERS; i++) {
        Worker worker = new Worker(queue, directory);
        worker.start();
      }

      while (true) {
        Socket socket = serverSocket.accept();
        logger.info("Connection accepted!");

        queue.put(socket);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
