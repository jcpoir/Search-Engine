package cis5550.kvs;

import java.util.concurrent.ConcurrentHashMap;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import cis5550.tools.Logger;

import cis5550.webserver.Server;

public class Worker extends cis5550.generic.Worker {
  private static final Logger logger = Logger.getLogger(Worker.class);
  private static Map<String, Map<String, Row>> map = new ConcurrentHashMap<>();
  private static Set<String> persistentTables = new HashSet<>();

  public static void putRow(String tableName, String row, String storageDirectory) throws IOException {
    if (!map.containsKey(tableName)) {
      map.put(tableName, new HashMap<>());

      if (persistentTables.contains(tableName)) {
        File newFile = new File(String.format("%s/%s.table", storageDirectory, tableName));
        newFile.createNewFile();
      }
    }

    if (persistentTables.contains(tableName)) {
      return;
    } else {
      if (!map.get(tableName).containsKey(row)) {
        map.get(tableName).put(row, new Row(row));
      }
    }
  }

  public static Row getRow(String tableName, String row, String storageDirectory) throws Exception {
    if (persistentTables.contains(tableName)) {
      long position = Long.parseLong(map.get(tableName).get(row).get("pos"));
      RandomAccessFile file = new RandomAccessFile(String.format("%s/%s.table", storageDirectory, tableName), "rw");
      file.seek(position);
      Row findRow = Row.readFrom(file);
      return findRow;
    }

    return map.get(tableName).get(row);
  }
  
  public static String htmlTableList() {
    StringBuilder table = new StringBuilder();
    table.append("<table border=\"1\"> <tr> <th>Table Name</th> <th>Number of Keys</th> <th>Persistence</th> </tr>");
    for (Map.Entry<String, Map<String, Row>> entry : map.entrySet()) {
      String persistence = "";
      boolean isPersistent = persistentTables.contains(entry.getKey());
      if (isPersistent) {
        persistence = "Persistent";
      }
      table.append(
          String.format("<tr> <td><a href=\"/view/%s\">%s</a> <td>%s</td> </td> <td>%s</td> </tr>",
              entry.getKey(), entry.getKey(), Integer.toString(entry.getValue().size()), persistence));
    }
    table.append("</table>");
    return table.toString();
  }
  
  public static String singleTableView(String tableName, int pageNum, String storageDirectory) throws Exception {
    String[] allRows = map.get(tableName).keySet().toArray(new String[map.get(tableName).size()]);
    Arrays.sort(allRows);
    Set<String> allColumnsSet = new HashSet<>();
    for (int i = 0 + (pageNum * 10); i < 10 + (pageNum * 10); i++) {
      if (i >= allRows.length) {
        break;
      }
      allColumnsSet.addAll(getRow(tableName, allRows[i], storageDirectory).columns());
    }
    String[] allColumns = allColumnsSet.toArray(new String[allColumnsSet.size()]);
    Arrays.sort(allColumns);

    StringBuilder table = new StringBuilder();
    // Table Header
    table.append("<table border=\"1\"> <tr> <th>Rows</th>");
    for (String columnName : allColumns) {
      table.append(String.format("<th>%s</th>", columnName));
    }
    table.append("</tr>");
    // Table Body (First 10)
    for (int i = 0 + (pageNum * 10); i < 10 + (pageNum * 10); i++) {
      if (i >= allRows.length) {
        break;
      }
      Row row = getRow(tableName, allRows[i], storageDirectory);
      table.append("<tr>");
      table.append(String.format("<td>%s</td>", allRows[i]));
      for (String columnName: allColumns) {
        String entry = row.get(columnName);
        if (entry == null) {
          entry = "";
        }
        table.append(String.format("<td>%s</td>", entry));
      }
      table.append("</tr>");
    }
    table.append("</table>");

    return table.toString();
  }

  public static void loadPersistentTables(String storageDirectory) throws IOException {
    Thread thread = new Thread(() -> {
      try {
        File f = new File(storageDirectory);

        File[] files = f.listFiles();

        for (File file : files) {
          String[] splitName = file.getName().split("\\.");
          if (splitName.length == 1 || !splitName[1].equals("table")) {
            continue;
          }
          long curOffset = 0;
          HashMap<String, Long> position = new HashMap<>();
          BufferedReader reader = new BufferedReader(new FileReader(file));

          String line = reader.readLine();
          while (line != null) {
            position.put(line.split(" ")[0], curOffset);
            curOffset += line.length() + 1;

            line = reader.readLine();
          }
          reader.close();

          map.put(splitName[0], new HashMap<>());
          persistentTables.add(splitName[0]);
          for (Map.Entry<String, Long> entry : position.entrySet()) {
            Row row = new Row(entry.getKey());
            row.put("pos", Long.toString(entry.getValue()));
            map.get(splitName[0]).put(entry.getKey(), row);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    
    thread.start();
      
  }

  private static void printLog(String str) {
    logger.info(str);
  }
  public static void main(String[] args) throws IOException {
    int portNum = Integer.parseInt(args[0]);
    Server.port(portNum);

    String storageDirectory = args[1];
    String address = args[2];
    startPingThread(address, storageDirectory, portNum);

    loadPersistentTables(storageDirectory);

    Server.put("/data/:table_name/:row/:col", (req, res) -> {
      String tableName = req.params("table_name");
      String row = java.net.URLDecoder.decode(req.params("row"), "UTF-8");
      String col = java.net.URLDecoder.decode(req.params("col"), "UTF-8");

      putRow(tableName, row, storageDirectory);

      if (persistentTables.contains(tableName)) {
        RandomAccessFile file = new RandomAccessFile(String.format("%s/%s.table", storageDirectory, tableName), "rw");
        Row getRow = map.get(tableName).get(row);
        Row retrieveRow = null;
        if (getRow == null) {
          retrieveRow = new Row(row);
          map.get(tableName).put(row, new Row(row));
        } else {
          int getPos = Integer.parseInt(map.get(tableName).get(row).get("pos"));
          file.seek(getPos); 
          retrieveRow = Row.readFrom(file);
        }

        retrieveRow.put(col, req.bodyAsBytes());
        file.seek(file.length());
        map.get(tableName).get(row).put("pos", Long.toString(file.length()));
        file.write(retrieveRow.toByteArray());
        file.write("\n".getBytes());
        file.close();
      } else {
        Row retrieveRow = map.get(tableName).get(row);
        retrieveRow.put(col, req.bodyAsBytes());
      }

      res.status(200, "OK");
      return "OK";
    });

    Server.get("/data/:table_name/:row/:col", (req, res) -> {
      String tableName = req.params("table_name");
      String row = java.net.URLDecoder.decode(req.params("row"), "UTF-8");
      String col = java.net.URLDecoder.decode(req.params("col"), "UTF-8");

      if (!map.containsKey(tableName) || !(map.get(tableName).containsKey(row))) {
        res.status(404, "Not Found");
        return "Not Found";
      }

      Row getRow = getRow(tableName, row, storageDirectory);
      byte[] retrieveItem = getRow.getBytes(col);
      res.header("Content-Length", Integer.toString(retrieveItem.length));
      res.bodyAsBytes(retrieveItem);

      res.status(200, "OK");
      return null;
    });

    Server.get("/", (req, res) -> {
      res.type("text/html");
      StringBuilder html = new StringBuilder();
      html.append("<html>");
      html.append("<title>Table List</title>");
      html.append(htmlTableList());
      html.append("</html>");
      return html;
    });
  
    Server.get("/view/:table_name", (req, res) -> {
      String tableName = req.params("table_name");
      String pageNumStr = req.queryParams("fromRow");
      int pageNum = 0;
      if (pageNumStr != null) {
        pageNum = Integer.parseInt(pageNumStr);
      }

      res.type("text/html");
      StringBuilder html = new StringBuilder();
      html.append("<html>");
      html.append(String.format("<title>View %s</title>", tableName));
      html.append(singleTableView(tableName, pageNum, storageDirectory));

      if ((pageNum * 10) + 10 < map.get(tableName).size()) {
        html.append(String.format("<a href=\"/view/%s?fromRow=%s\">Next</a>", tableName, Integer.toString(pageNum + 1)));
      }

      html.append("</html>");
      return html;
    });
  
    Server.put("/persist/:table_name", (req, res) -> {
      if (persistentTables.contains(req.params("table_name"))) {
        res.status(403, "Table Already Exists");
        return "403 Table Already Exists";
      }

      persistentTables.add(req.params("table_name"));
      putRow(req.params("table_name"), null, storageDirectory);
      return "OK";
    });
  
    Server.get("/data/:table_name/:row", (req, res) -> {
      String tableName = req.params("table_name");
      String row = java.net.URLDecoder.decode(req.params("row"), "UTF-8");

      if (!map.containsKey(tableName) || !(map.get(tableName).containsKey(row))) {
        res.status(404, "Not Found");
        return "Not Found";
      }
    
      Row getRow = getRow(tableName, row, storageDirectory);
      res.type("text/plain");
      res.header("Content-Length", Integer.toString(getRow.toByteArray().length));
      res.bodyAsBytes((getRow.toByteArray()));

      return null;
    });

    Server.get("/data/:table_name", (req, res) -> {
      String tableName = req.params("table_name");
      String startRow = req.queryParams("startRow");
      String endRowExclusive = req.queryParams("endRowExclusive");

      if (startRow == null) {
        startRow = "";
      }

      if (!map.containsKey(tableName)) {
        res.status(404, "Not Found");
        return "Not Found";
      }

      res.type("text/plain");

      for (Map.Entry<String, Row> entry : map.get(tableName).entrySet()) {
        if (entry.getKey().compareTo(startRow) < 0 || (endRowExclusive != null && entry.getKey().compareTo(endRowExclusive) >= 0)) {
          continue;
        }

        Row row = getRow(tableName, entry.getKey(), storageDirectory);
        res.write(row.toByteArray());
        res.write("\n".getBytes());
      }
      res.write("\n".getBytes());

      return null;
    });
  
    Server.put("/data/:table_name", (req, res) -> {
      String tableName = req.params("table_name");

      putRow(tableName, null, storageDirectory);
      Map<String, Row> getMap = map.get(tableName);

      InputStream stream = new ByteArrayInputStream(req.bodyAsBytes());
      Row getRow = Row.readFrom(stream);
      while (getRow != null) {
        getMap.put(getRow.key, getRow);
        getRow = Row.readFrom(stream);
      }

      return "OK";
    });

    Server.put("/rename/:table_name", (req, res) -> {
      String tableName = java.net.URLDecoder.decode(req.params("table_name"), "UTF-8");
      String newName = req.body();

      if (!map.containsKey(tableName)) {
        res.status(404, "Not Found");
        return "Not Found";
      }
      if (map.containsKey(newName)) {
        res.status(409,"Table Name Already Exists");
        return "Table Name Already Exists";
      }

      Map<String, Row> getOldTable = map.get(tableName);
      map.put(newName, getOldTable);
      map.remove(tableName);
      if (persistentTables.contains(tableName)) {
        persistentTables.add(newName);
        persistentTables.remove(tableName);

        File newFile = new File(String.format("%s/%s.table", storageDirectory, newName));
        File oldFile = new File(String.format("%s/%s.table", storageDirectory, tableName));
        oldFile.renameTo(newFile);
      }

      return "OK";
    });
  
    Server.put("/delete/:table_name", (req, res) -> {
      String tableName = java.net.URLDecoder.decode(req.params("table_name"), "UTF-8");

      if (!map.containsKey(tableName)) {
        res.status(404, "Not Found");
        return "Not Found";
      }

      map.remove(tableName);
      if (persistentTables.contains(tableName)) {
        persistentTables.remove(tableName);

        File oldFile = new File(String.format("%s/%s.table", storageDirectory, tableName));
        oldFile.delete();
      }
      return "OK";
    });
  
    Server.get("/count/:table_name", (req, res) -> {
      String tableName = req.params("table_name");

      if (!map.containsKey(tableName)) {
        res.status(404, "Not Found");
        return "Not Found";
      }

      res.type("text/plain");
      res.header("Content-Length", Integer.toString(Integer.toString(map.get(tableName).size()).length()));
      res.body(Integer.toString(map.get(tableName).size()));

      return null;
    });
  
    Server.get("/tables", (req, res) -> {
      StringBuilder builder = new StringBuilder();
      
      for (String str : map.keySet()) {
        builder.append(str);
        builder.append("\n");
      }

      res.type("text/plain");

      return builder.toString();
    });
  }
}