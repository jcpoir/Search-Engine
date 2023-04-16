package cis5550.flame;

import java.util.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.io.*;

import static cis5550.webserver.Server.*;

import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.*;
import cis5550.flame.FlameRDD.*;
import cis5550.kvs.*;
import cis5550.tools.Serializer;

class Worker extends cis5550.generic.Worker {

  public static void main(String args[]) {
    if (args.length != 2) {
      System.err.println("Syntax: Worker <port> <masterIP:port>");
      System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
//    startPingThread(server, "" + port, port);
	startPingThread(port, "" + port, server);
    final File myJAR = new File("__worker" + port + "-current.jar");

    port(port);

    post("/useJAR", (request, response) -> {
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
            String outputIterable = lambda.op(accumulator, row.get(col));
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

    post("/rdd/groupBy", (request, response) -> {
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
  
    post("/rdd/fromTable", (request, response) -> {
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
        RowToString lambda = (RowToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);
        while (iterate.hasNext()) {
          Row row = iterate.next();
          String outputLambda = lambda.op(row);
          if (outputLambda != null) {
            client.put(newTable, row.key(), "value", outputLambda);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return "OK";
    });
  
    post("/rdd/flatMapToPair", (request, response) -> {
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
        StringToPairIterable lambda = (StringToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);
        while (iterate.hasNext()) {
          Row row = iterate.next();
          Iterable<FlamePair> outputIterable = lambda.op(row.get("value"));
          Iterator<FlamePair> iterator = null;
          if (outputIterable != null) {
            iterator = outputIterable.iterator();
          }
          while (iterator != null && iterator.hasNext()) {
            FlamePair pair = iterator.next();
            String key = pair._1();
            String val = pair._2();
            client.put(newTable, key, UUID.randomUUID().toString(), val);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return "OK";
    });
  
    post("/rdd/pairFlatMapToPair", (request, response) -> {
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
        PairToPairIterable lambda = (PairToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);
        while (iterate.hasNext()) {
          Row row = iterate.next();
          for (String col : row.columns()) {
            Iterable<FlamePair> outputIterable = lambda.op(new FlamePair(row.key(), row.get(col)));;
            if (outputIterable != null) {
              Iterator<FlamePair> outputIterator = outputIterable.iterator();
              while (outputIterator.hasNext()) {
                FlamePair pair = outputIterator.next();
                String key = pair._1();
                String val = pair._2();
                client.put(newTable, key, row.key(), val);
              }
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return "OK";
    });

    post("/rdd/pairFlatMap", (request, response) -> {
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
        PairToStringIterable lambda = (PairToStringIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);
        while (iterate.hasNext()) {
          Row row = iterate.next();
          for (String col : row.columns()) {
            Iterable<String> outputIterable = lambda.op(new FlamePair(row.key(), row.get(col)));
            if (outputIterable != null) {
              Iterator<String> outputIterator = outputIterable.iterator();
              while (outputIterator.hasNext()) {
                String next = outputIterator.next();
                client.put(newTable, UUID.randomUUID().toString(), "value", next);
              }
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return "OK";
    });

    post("/rdd/distinct", (request, response) -> {
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
          client.put(newTable, row.get("value"), "value", row.get("value"));
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return "OK";
    });

    post("/rdd/join", (request, response) -> {
      String oldTable = request.queryParams("oldtable");
      String newTable = request.queryParams("newtable");
      String fromKey = request.queryParams("fromkey");
      String toKey = request.queryParams("tokey");
      String masterArg = request.queryParams("masterarg");
      String joinedTable = request.queryParams("zeroelement");

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
          Row joinRow = client.getRow(joinedTable, row.key());
          if (joinRow != null) {
            for (String col1 : row.columns()) {
              for (String col2: joinRow.columns()) {
                client.put(newTable, row.key(), col1 + "," + col2, row.get(col1) + "," + joinRow.get(col2));
              }
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return "OK";
    });
  
    post("/rdd/fold", (request, response) -> {
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
      byte[] searchAccum = client.get(newTable, "fold", "fold");
      String accumulator = zeroElement;
      if (searchAccum != null && searchAccum.length > 0) {
        accumulator = new String(searchAccum, StandardCharsets.UTF_8);
      }

      if (fromKey.equals("null")) {
        fromKey = null;
      }
      if (toKey.equals("null")) {
        toKey = null;
      }

      try {
        TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);
        boolean isHasNext = false;
        while (iterate.hasNext()) {
          isHasNext = true;
          Row row = iterate.next();
          for (String col : row.columns()) {
            String outputIterable = lambda.op(accumulator, row.get(col));
            accumulator = outputIterable;
            client.put(newTable, "fold", "fold", accumulator);
          }
        }
        if (!isHasNext) {
          client.put(newTable, "fold", "fold", accumulator);
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return null;
    });

    post("/rdd/filter", (request, response) -> {
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
        StringToBoolean lambda = (StringToBoolean) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);
        while (iterate.hasNext()) {
          Row row = iterate.next();
          for (String col : row.columns()) {
            boolean outputIterable = lambda.op(row.get(col));
            if (outputIterable) {
              client.put(newTable, row.key(), col, row.get(col));
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return "OK";
    });
  
    post("/rdd/mapPartitions", (request, response) -> {
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
        IteratorToIterator lambda = (IteratorToIterator) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        Iterator<Row> iterate = client.scan(oldTable, fromKey, toKey);

        List<String> list = new ArrayList<>();
        while (iterate.hasNext()) {
          Row row = iterate.next();
          for (String col : row.columns()) {
            list.add(row.get(col));
          }
        }

        Iterator<String> callLambda = lambda.op(list.iterator());
        iterate = client.scan(oldTable, fromKey, toKey);
        while (iterate.hasNext()) {
          Row row = iterate.next();
          for (String col : row.columns()) {
            client.put(newTable, row.key(), col, callLambda.next());
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return "OK";
    });
  
    post("/rdd/cogroup", (request, response) -> {
      String oldTable = request.queryParams("oldtable");
      String newTable = request.queryParams("newtable");
      String fromKey = request.queryParams("fromkey");
      String toKey = request.queryParams("tokey");
      String masterArg = request.queryParams("masterarg");
      String joinedTable = request.queryParams("zeroelement");

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
          Row joinRow = client.getRow(joinedTable, row.key());

          StringBuilder left = new StringBuilder();
          left.append('[');
          for (String col : row.columns()) {
            left.append(row.get(col));
            left.append(',');
          }
          left.append(']');

          StringBuilder right = new StringBuilder();
          right.append('[');
          for (String col : joinRow.columns()) {
            right.append(joinRow.get(col));
            right.append(',');
          }
          right.append(']');

          client.put(newTable, row.key(), "cogroup", left.toString() + "," + right.toString());
        }
      } catch (Exception e) {
        e.printStackTrace();
        return "Failed";
      }

      return "OK";
    });
  }
}
