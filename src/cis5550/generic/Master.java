package cis5550.generic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.webserver.Server;

public class Master {
  protected static Map<String, String> workers = new ConcurrentHashMap<>();
  protected static Map<String, Long> pingTimes = new ConcurrentHashMap<>();

  public static Vector<String> getWorkers() {
    return new Vector<>(workers.values());
  }

  public static String workerTable() {
    StringBuilder table = new StringBuilder();
    table.append("<table> <tr> <th>ID</th> <th>Address</th> </tr>");
    for (Map.Entry<String, String> entry : workers.entrySet()) {
      table.append(String.format("<tr> <td><a href=\"http://%s/\">%s</a></td> <td><a href=\"http://%s/\">%s</a></td> </tr>", 
        entry.getValue(), entry.getKey(), entry.getValue(), entry.getValue()));
    }
    table.append("</table>");
    return table.toString();
  }

  public static void registerRoutes() {
    Server.get("/ping", (req, res) -> {
      String ip = req.ip();
      String id = req.queryParams("id");
      String portString = req.queryParams("port");

      if (id == null || portString == null) {
        res.status(400, "ID or Port Missing");
        return "ID or Port Missing";
      } else {
        workers.put(id, ip + ":" + portString);
        pingTimes.put(id, System.currentTimeMillis());
        res.status(200, "OK");
        return "OK";
      }
    });

    Server.get("/workers", (req, res) -> {
      StringBuilder builder = new StringBuilder();
      builder.append(workers.size() + "\n");
      for (Map.Entry<String, String> entry : workers.entrySet()) {
        builder.append(entry.getKey() + "," + entry.getValue() + "\n");
      }
      res.status(200, "OK");
      return builder.toString();
    });

  }
}
