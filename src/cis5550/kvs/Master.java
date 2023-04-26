package cis5550.kvs;
import java.util.Map;

import cis5550.webserver.Server;

public class Master extends cis5550.generic.Master{
  public static void main(String[] args) {
    int portnumber = Integer.parseInt(args[0]);
    Server.port(portnumber);
    Master.registerRoutes();

    Thread thread = new Thread(() -> {
      while (true) {
        for (Map.Entry<String, Long> entry : pingTimes.entrySet()) {
          if (System.currentTimeMillis() - entry.getValue() > 15000) {
            workers.remove(entry.getKey());
            pingTimes.remove(entry.getKey());
          }
        }
      }
    });
    thread.start();

    Server.get("/", (req, res) -> {
      res.type("text/html");
      StringBuilder html = new StringBuilder();
      html.append("<html>");
      html.append("<title>KVS Master</title>");
      html.append(workerTable());
      html.append("</html>");
      return html;    
    });
  }
}