package cis5550.webserver;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import cis5550.tools.Logger;

public class Server {
  private static final int NUM_WORKERS = 100;

  private static final Logger logger = Logger.getLogger(Server.class);

  public static Server server = null;
  public static boolean flag = false;
  private static int portNumber = 80;
  private static String directory = null;

  private static Map<String, Map<String, Route>> routingTable = new HashMap<>();

  public static class staticFiles {
    public static void location(String s) {
      if (server == null) {
        server = new Server();
      }
      directory = s;
      if (!flag) {
        flag = true;
        Thread thread = new Thread(() -> {
          server.run();
        });
        thread.start();
      }
    }
  }

  public static void get(String s, Route r) {
    if (server == null) {
      server = new Server();
    }

    if (!routingTable.containsKey("GET")) {
      routingTable.put("GET", new HashMap<>());
    }
    routingTable.get("GET").put(s.split("\\?")[0], r);

    if (!flag) {
      flag = true;
      Thread thread = new Thread(() -> {
        server.run();
      });
      thread.start();
    }
  }

  public static void post(String s, Route r) {
    if (server == null) {
      server = new Server();
    }

    if (!routingTable.containsKey("POST")) {
      routingTable.put("POST", new HashMap<>());
    }
    routingTable.get("POST").put(s.split("\\?")[0], r);

    if (!flag) {
      flag = true;
      Thread thread = new Thread(() -> {
        server.run();
      });
      thread.start();
    }
  }

  public static void put(String s, Route r) {
    if (server == null) {
      server = new Server();
    }

    if (!routingTable.containsKey("PUT")) {
      routingTable.put("PUT", new HashMap<>());
    }
    routingTable.get("PUT").put(s.split("\\?")[0], r);

    if (!flag) {
      flag = true;
      Thread thread = new Thread(() -> {
        server.run();
      });
      thread.start();
    }
  }

  public static void port(int num) {
    if (server == null) {
      server = new Server();
    }
    portNumber = num;
  }

  public void run() {
    BlockingQueue<Socket> queue = new LinkedBlockingQueue<>();

    try {
      ServerSocket serverSocket = new ServerSocket(portNumber);

      for (int i = 0; i < NUM_WORKERS; i++) {
        Worker worker = new Worker(queue, directory, routingTable, server);
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