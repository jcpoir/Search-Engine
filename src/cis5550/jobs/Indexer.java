package cis5550.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;

public class Indexer {
  public static void run(FlameContext context, String[] args) throws Exception {
    RowToString firstLambda = row -> {
      return row.get("url") + "," + row.get("page");
    };
    StringToPair secondLambda = str -> {
      String[] splitString = str.split(",", 2);
      return new FlamePair(splitString[0], splitString[1]);
    };
    FlameRDD rdd = context.fromTable("crawl", firstLambda);
    FlamePairRDD pairRDD = rdd.mapToPair(secondLambda);
    
    PairToPairIterable p2p = pair -> {
      Map<String, String> wordsAndIndices = new HashMap<>();
      List<FlamePair> pairs = new ArrayList<>();
      String html = pair._2();

      String cleanString = html.replaceAll("<[^>]*>", "")
                               .replaceAll("[.,:;!?’\"()-]|[\r\n\t]", " ")
                               .toLowerCase();
      String[] words = cleanString.split("\\s+");

      int i = 1;
      for (String word : words) {
        Stemmer stemmer = new Stemmer();
        stemmer.add(word.toCharArray(), word.length());
        stemmer.stem();
        String stemmedWord = stemmer.toString();

        if (!wordsAndIndices.containsKey(word)) {
          wordsAndIndices.put(word, Integer.toString(i));
        } else {
          wordsAndIndices.put(word, wordsAndIndices.get(word) + " " + Integer.toString(i));
        }

        if (!word.equals(stemmedWord)) {
          if (!wordsAndIndices.containsKey(stemmedWord)) {
            wordsAndIndices.put(stemmedWord, Integer.toString(i));
          } else {
            wordsAndIndices.put(stemmedWord, wordsAndIndices.get(stemmedWord) + " " + Integer.toString(i));
          }
        }
        i++;
      }

      for (Map.Entry<String, String> item : wordsAndIndices.entrySet()) {
        pairs.add(new FlamePair(item.getKey(), pair._1() + ":" + item.getValue()));
      }
      return pairs;
    };
    FlamePairRDD mapToPair = pairRDD.flatMapToPair(p2p);

    TwoStringsToString twoStringsToString = (a, b) -> {
      if (a.equals("")) {
        return b;
      }

      List<String> splitA = new ArrayList<>(Arrays.asList(a.split(",")));
      int index = 0;
      boolean itemNotAdded = true;
      for (String item : splitA) {
        int numElems = item.split(":")[item.split(":").length - 1].split(" ").length;
        if (b.split(":")[b.split(":").length - 1].split(" ").length >= numElems) {
          splitA.add(index, b);
          itemNotAdded = false;
          break;
        }
        index += 1;
      }
      if (itemNotAdded) {
        splitA.add(b);
      }      

      StringBuilder builder = new StringBuilder();
      builder.append(splitA.get(0));
      for (int i = 1; i < splitA.size(); i++) {
        builder.append("," + splitA.get(i));
      }

      return builder.toString();
    };

    FlamePairRDD folded = mapToPair.foldByKey("", twoStringsToString);
    folded.saveAsTable("index");
  }
}
