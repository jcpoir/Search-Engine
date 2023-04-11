package cis5550.flame;

import java.util.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Serializer;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToString;
import cis5550.kvs.*;

class Worker extends cis5550.generic.Worker {

	public static void main(String args[]) {
    if (args.length != 2) {
    	System.err.println("Syntax: Worker <port> <masterIP:port>");
    	System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
	  startPingThread(server, ""+port, port);
    final File myJAR = new File("__worker"+port+"-current.jar");

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });

    post("/rdd/flatMap", (request, response) -> {
      String oldTable = request.queryParams("oldtable");
      String newTable = request.queryParams("newtable");
      String fromKey = request.queryParams("fromkey");
      String toKey = request.queryParams("tokey");
      String masterArg = request.queryParams("masterarg");

      KVSClient client = new KVSClient(masterArg);

      if (fromKey.equals("null")) {
        fromKey = null;
      }
      if (toKey.equals("null")) {
        toKey = null;
      }

      try {
        StringToIterable lambda = (StringToIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);
        while (iterate.hasNext()) {
          Row row = iterate.next();
          Iterable<String> outputIterable = lambda.op(row.get("value"));
          if (outputIterable != null) {
            Iterator<String> outputIterator = outputIterable.iterator();
            while (outputIterator.hasNext()) {
              String next = outputIterator.next();
              client.put(newTable, UUID.randomUUID().toString(), "value", next);
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }
      
      return "OK";
    });

    post("/rdd/mapToPair", (request, response) -> {
      String oldTable = request.queryParams("oldtable");
      String newTable = request.queryParams("newtable");
      String fromKey = request.queryParams("fromkey");
      String toKey = request.queryParams("tokey");
      String masterArg = request.queryParams("masterarg");

      KVSClient client = new KVSClient(masterArg);

      if (fromKey.equals("null")) {
        fromKey = null;
      }
      if (toKey.equals("null")) {
        toKey = null;
      }

      try {
        StringToPair lambda = (StringToPair) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);
        while (iterate.hasNext()) {
          Row row = iterate.next();
          FlamePair outputIterable = lambda.op(row.get("value"));
          String key = outputIterable.a;
          String val = outputIterable.b;
          client.put(newTable, key, row.key(), val);
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return "OK";
    });

    post("/rdd/foldByKey", (request, response) -> {
      String oldTable = request.queryParams("oldtable");
      String newTable = request.queryParams("newtable");
      String fromKey = request.queryParams("fromkey");
      String toKey = request.queryParams("tokey");
      String zeroElement = request.queryParams("zeroelement");
      if (zeroElement != null) {
        zeroElement = URLDecoder.decode(zeroElement, StandardCharsets.UTF_8);
      }
      String masterArg = request.queryParams("masterarg");

      KVSClient client = new KVSClient(masterArg);
      String accumulator = zeroElement;


      if (fromKey.equals("null")) {
        fromKey = null;
      }
      if (toKey.equals("null")) {
        toKey = null;
      }

      try {
        TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);
        while (iterate.hasNext()) {
          Row row = iterate.next();
          for (String col : row.columns()) {
            String outputIterable = lambda.op(row.get(col), accumulator);
            accumulator = outputIterable;
          }
          client.put(newTable, row.key(), "foldcol", accumulator);
          accumulator = zeroElement;
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return null;
    });

    post("/rdd/convert", (request, response) -> {
      String oldTable = request.queryParams("oldtable");
      String newTable = request.queryParams("newtable");
      String fromKey = request.queryParams("fromkey");
      String toKey = request.queryParams("tokey");
      String masterArg = request.queryParams("masterarg");

      KVSClient client = new KVSClient(masterArg);

      if (fromKey.equals("null")) {
        fromKey = null;
      }
      if (toKey.equals("null")) {
        toKey = null;
      }

      try {
        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);
        while (iterate.hasNext()) {
          Row row = iterate.next();

          for (String col : row.columns()) {
            client.put(newTable, row.get(col), "item", row.get(col));
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return "OK";
    });

    post("/rdd/intersection", (request, response) -> {
      String oldTable = request.queryParams("oldtable");
      String newTable = request.queryParams("newtable");
      String fromKey = request.queryParams("fromkey");
      String toKey = request.queryParams("tokey");
      String masterArg = request.queryParams("masterarg");

      KVSClient client = new KVSClient(masterArg);

      if (fromKey.equals("null")) {
        fromKey = null;
      }
      if (toKey.equals("null")) {
        toKey = null;
      }

      try {
        String otherTableName = new String(request.body());
        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);

        while (iterate.hasNext()) {
          Row row = iterate.next();
          if (client.get(otherTableName, row.key(), "item") != null) {
            client.put(newTable, row.key(), "item", row.key());
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return null;
    });

    post("/rdd/sample", (request, response) -> {
      String oldTable = request.queryParams("oldtable");
      String newTable = request.queryParams("newtable");
      String fromKey = request.queryParams("fromkey");
      String toKey = request.queryParams("tokey");
      String masterArg = request.queryParams("masterarg");

      KVSClient client = new KVSClient(masterArg);

      if (fromKey.equals("null")) {
        fromKey = null;
      }
      if (toKey.equals("null")) {
        toKey = null;
      }

      try {
        Random rd = new Random();
        double prob = ByteBuffer.wrap(request.bodyAsBytes()).getDouble();

        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);
        while (iterate.hasNext()) {
          Row row = iterate.next();
          for (String col : row.columns()) {
            if (prob >= rd.nextDouble()) {
              client.put(newTable, UUID.randomUUID().toString(), "sample", row.get(col));
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return "OK";
    });
  
    post("/rdd/groupBy", (request,response) -> {
      String oldTable = request.queryParams("oldtable");
      String newTable = request.queryParams("newtable");
      String fromKey = request.queryParams("fromkey");
      String toKey = request.queryParams("tokey");
      String masterArg = request.queryParams("masterarg");

      KVSClient client = new KVSClient(masterArg);

      if (fromKey.equals("null")) {
        fromKey = null;
      }
      if (toKey.equals("null")) {
        toKey = null;
      }

      try {
        StringToString lambda = (StringToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);
        while (iterate.hasNext()) {
          Row row = iterate.next();
          
          for (String col : row.columns()) {
            String outputLambda = lambda.op(row.get(col));
            byte[] getSameGroup = client.get(newTable, outputLambda, "group");
            String newGroup = "";
            if (getSameGroup == null) {
              newGroup += row.get(col);
            } else {
              newGroup = new String(getSameGroup, StandardCharsets.UTF_8);
              newGroup += "," + row.get(col);
            }
            client.put(newTable, outputLambda, "group", newGroup);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return "OK";
    });
  }
}
