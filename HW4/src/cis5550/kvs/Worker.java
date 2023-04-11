package cis5550.kvs;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.Map;

import cis5550.webserver.Server;

public class Worker extends cis5550.generic.Worker {
  private static Map<String, Map<String, ArrayList<Row>>> map = new ConcurrentHashMap<>();

  public static void putRow(String tableName, String row) {
    if (!map.get(tableName).containsKey(row)) {
      map.get(tableName).put(row, new ArrayList<>());
    }
    ArrayList<Row> rows = map.get(tableName).get(row);
    rows.add(new Row(row + ": " + (rows.size() + 1)));    
  }

  public static ArrayList<Row> getRow(String tableName, String row) {
    return map.get(tableName).get(row);
  }

  public static void main(String[] args) {
    int portNum = Integer.parseInt(args[0]);
    Server.port(portNum);

    String storageDirectory = args[1];
    String address = args[2];
    startPingThread(portNum, storageDirectory, address);

    Server.put("/data/:table_name/:row/:col", (req, res) -> {
      String tableName = req.params("table_name");
      String row = req.params("row");
      String col = req.params("col");
      String ifCol = req.queryParams("ifcolumn");
      String equalsCheck = req.queryParams("equals");

      if (ifCol != null && equalsCheck != null) {
        if (map.get(tableName) == null) {
          res.status(400, "Failed");
          return "FAIL";
        }
        ArrayList<Row> rows = getRow(tableName, row); 
        if (rows == null || rows.size() == 0) {
          res.status(400, "Failed");
          return "FAIL";
        }

        Row mostRecentRow = null;
        for (Row iterRow : rows) {
          if (iterRow.get(ifCol) != null) {
            mostRecentRow = iterRow;
          } else {
            break;
          }
        }
        if (mostRecentRow == null) {
          res.status(400, "Failed");
          return "FAIL";
        }

        String retrieveItem = mostRecentRow.get(ifCol);
        if (retrieveItem == null || !retrieveItem.equals(equalsCheck)) {
          res.status(400, "Failed");
          return "FAIL";
        }
      }

      if (!map.containsKey(tableName)) {
        map.put(tableName, new ConcurrentHashMap<>());
      }
      if (!(map.get(tableName).containsKey(row))) {
        putRow(tableName, row);
      }
      ArrayList<Row> retrieveRows = map.get(tableName).get(row);
      Row retrieveRow = null;
      int version = 1;
      for (Row iterRow : retrieveRows) {
        if (iterRow.get(col) == null) {
          retrieveRow = iterRow;
          break;
        }

        version += 1;
      }
      if (version > retrieveRows.size()) {
        putRow(tableName, row);
        retrieveRow = retrieveRows.get(retrieveRows.size() - 1);
      }

      retrieveRow.put(col, req.bodyAsBytes());

      res.header("Version", Integer.toString(version));
      res.status(200, "OK");
      return "OK";
    });

    Server.get("/data/:table_name/:row/:col", (req, res) -> {
      String tableName = req.params("table_name");
      String row = req.params("row");
      String col = req.params("col");
      String versStr = req.queryParams("version");
      int vers = -1;
      if (versStr != null) {
        vers = Integer.parseInt(versStr);
      }

      if (!map.containsKey(tableName)) {
        res.status(404, "Not Found");
        return "Not Found";
      }
      if (!(map.get(tableName).containsKey(row))) {
        res.status(404, "Not Found");
        return "Not Found";
      }
      ArrayList<Row> getRows = getRow(tableName, row);
      Row getRow = null;
      int version = 0;
      if (vers == -1) {
        for (Row iterRow : getRows) {
          if (iterRow.get(col) != null) {
            getRow = iterRow;
            version += 1;
          } else {
            break;
          }
        }

        if (getRow == null) {
          res.status(404, "Not Found");
          return "Not Found";
        }
      } else {
        if (vers > getRows.size()) {
          res.status(404, "Not Found");
          return "Not Found";
        }

        getRow = getRows.get(vers - 1);
        version = vers;
      }
      
      byte[] retrieveItem = getRow.getBytes(col);
      res.bodyAsBytes(retrieveItem);
      res.header("Version", Integer.toString(version));

      res.status(200, "OK");
      return null;
    });

  }
}
