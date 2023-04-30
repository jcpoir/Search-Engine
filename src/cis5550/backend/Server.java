package cis5550.backend;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.kvs.KVSClient;
import cis5550.tools.Stemmer;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.IteratorToIterator;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlamePairRDDImpl;

public class Server {

  private static List<String> getWordsFromQuery(String query, Set<String> stopWords) {
    List<String> words = new ArrayList<>();

    String[] spiltString = query.split("\\s+");
    for (String str : splitString) {
      Stemmer stemmer = new Stemmer();
      stemmer.add(str.toCharArray(), str.length());
      stemmer.stem();
      String stemmedWord = stemmer.toString();

      if (!stopWords.contains(stemmedWord)) {
        words.add(stemmedWord);
      }
    }

    return words;
  }

  private static List<String> getURLsFromQuery(KVSClient client, List<String> query) {
    List<String> toReturn = new ArrayList<>();
    for (String str : query) {
      String urlsString = new String(client.get("index", str, str));
      toReturn.addAll(Arrays.asList(urlsString.split(",")));
    }

    return toReturn;
  }

  private static FlamePairRDDImpl getURLsFrequency(KVSClient client, String query, Set<String> stopWords) {
    StringToPair mapToPairURL = url -> {
      return new FlamePair(url, "1");
    };

    TwoStringsToString foldUrlFrequency = (a, b) -> {
      if (a.equals("0")) {
        return b;
      }

      return Integer.parseInt(a) + Integer.parseInt(b);
    };

    List<String> words = getWordsFromQuery(query, stopWords);
    List<String> urls = getURLsFromQuery(client, words);
    FlameRDDImpl parallelizeURLs = context.parallelize(urls);
    FlamePairRDDImpl convertToPair = retrieveURLs.mapToPair(mapToPairURL);
    FlamePairRDDImpl urlFrequency = convertToPair.foldByKey("0", foldUrlFrequency);

    return urlFrequency;
  }
  
  private static FlameRDDImpl topURLs(FlamePairRDDImpl rdd) {
    int i = 0;
    PairToPairIterable p2p = pair -> {
      List<Pair> pairList = new ArrayList<>();
      if (Integer.parseInt(pair._2()) <= i) {
        return pairList;
      }

      pairList.add(pair);
      return pairList;
    };

    PairToStringIterable p2s = pair -> {
      List<String> returnList = new ArrayList<>();
      returnList.add(pair._1());

      return returnList;
    };

    while (rdd.count() > 200) {
      i += 1;
      rdd = rdd.flatMapToPair(p2p);
    }

    FlameRDDImpl urls = rdd.flatMap(p2s);

    return urls;
  }

  public static void main(FlameContext context, String[] args) throws IOException {
      int portNum = Integer.parseInt(args[0]);
      Server.port(portNum);
      KVSClient client = new KVSClient(args[1]);

      Set<String> stopWords = new HashSet<>();
      try (BufferedReader reader = new BufferedReader(new FileReader("stopwords.txt"))) {
        String line;
        while ((line = reader.readLine()) != null) {
          stopWords.add(line);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

      Server.get("/search", (req, res) -> {
        String query = java.net.URLDecoder.decode(req.queryParams("query"), "UTF-8");

        FlamePairRDDImpl urlsFrequency = getURLsFrequency(client, query, stopWords);

        FlameRDDImpl top200 = topURLs(urlsFrequency);

        List<String> urlStrings = top200.collect();
      });
  }
}
  
