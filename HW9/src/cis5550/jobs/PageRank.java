package cis5550.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.kvs.KVSClient;

public class PageRank {
  private static String[] parseURL(String url) {
    String result[] = new String[4];
    int slashslash = url.indexOf("//");
    if (slashslash > 0) {
      result[0] = url.substring(0, slashslash - 1);
      int nextslash = url.indexOf('/', slashslash + 2);
      if (nextslash >= 0) {
        result[1] = url.substring(slashslash + 2, nextslash);
        result[3] = url.substring(nextslash);
      } else {
        result[1] = url.substring(slashslash + 2);
        result[3] = "/";
      }
      int colonPos = result[1].indexOf(':');
      if (colonPos > 0) {
        result[2] = result[1].substring(colonPos + 1);
        result[1] = result[1].substring(0, colonPos);
      }
    } else {
      result[3] = url;
    }

    return result;
  }

  private static String cleanURL(String url, String[] parseURL) {
    String replaceURL = url;
    // Remove hashtags
    if (replaceURL.contains("#")) {
      replaceURL = replaceURL.split("#")[0];
      if (replaceURL == null || url.length() == 0) {
        return null;
      }
    }

    if (url.equals("/")) {
      StringBuilder builder = new StringBuilder();
      builder.append(parseURL[0] + "://");
      builder.append(parseURL[1] + ":" + parseURL[2]);
      builder.append('/');

      replaceURL = builder.toString();
      return replaceURL;
    }

    // Split the hyperlink by slashes
    List<String> splitSlashesHyperlink = new ArrayList<>(Arrays.asList(replaceURL.split("/")));
    // Split last part of url by slashes
    List<String> splitSlashesOriginalURL = new ArrayList<>(Arrays.asList(parseURL[3].split("/")));
    // Remove the first index
    if (splitSlashesOriginalURL.size() > 0) {
      splitSlashesOriginalURL.remove(0);
    }

    // Save the index to update the url from
    int curIndexReplace = splitSlashesOriginalURL.size() - 1;
    if (splitSlashesHyperlink.size() == 1) {
      // Add to the url if there's nothing to replace or all has been replace
      if (curIndexReplace == -1 || curIndexReplace >= splitSlashesOriginalURL.size()) {
        splitSlashesOriginalURL.add(replaceURL);
      } else {
        splitSlashesOriginalURL.set(curIndexReplace, replaceURL);
      }
    } else {
      for (String elem : splitSlashesHyperlink) {
        if (elem.equals("..")) {
          curIndexReplace -= 1;
          continue;
        } else if (elem.length() == 0) {
          curIndexReplace = 0;
          continue;
        }
        // Add to the url if there's nothing to replace or all has been replace
        if (curIndexReplace >= splitSlashesOriginalURL.size()) {
          splitSlashesOriginalURL.add(elem);
        } else {
          // Replace element of the current index
          splitSlashesOriginalURL.set(curIndexReplace, elem);
        }
        curIndexReplace += 1;
      }
    }

    // Delete remaining elements if new url has fewer elements than old
    if (curIndexReplace < splitSlashesOriginalURL.size() && curIndexReplace >= 0) {
      splitSlashesOriginalURL.subList(curIndexReplace, splitSlashesOriginalURL.size()).clear();
    }

    StringBuilder builder = new StringBuilder();
    builder.append(parseURL[0] + "://");
    builder.append(parseURL[1] + ":" + parseURL[2]);
    for (String str : splitSlashesOriginalURL) {
      builder.append("/" + str);
    }
    if (splitSlashesOriginalURL.size() == 0) {
      builder.append("/");
    }

    replaceURL = builder.toString();
    return replaceURL;
  }

  private static String cleanCompleteURL(String url) {
    String[] parseHyperlink = parseURL(url);

    if (parseHyperlink[0] == null || (!parseHyperlink[0].equals("http") && !parseHyperlink[0].equals("https"))) {
      return null;
    }

    if (parseHyperlink[3] == null) {
      return null;
    }
    String[] splitSlashes = parseHyperlink[3].split("/");
    if (splitSlashes.length > 1) {
      String[] getFileExtension = splitSlashes[splitSlashes.length - 1].split("\\.");
      if (getFileExtension.length > 1) {
        HashSet<String> prohibitedFileExtensions = new HashSet<String>(Set.of("jpg", "jpeg", "gif", "png", "txt"));
        if (prohibitedFileExtensions.contains(getFileExtension[getFileExtension.length - 1])) {
          return null;
        }
      }
    }

    if (parseHyperlink[2] == null) {
      if (parseHyperlink[0].equals("http")) {
        parseHyperlink[2] = "80";
      } else {
        parseHyperlink[2] = "443";
      }
    }

    StringBuilder builder = new StringBuilder();
    builder.append(parseHyperlink[0] + "://");
    builder.append(parseHyperlink[1] + ":" + parseHyperlink[2]);
    builder.append(parseHyperlink[3]);

    return builder.toString();
  }

  private static List<String> getURLsFromPage(String html, String originalUrl) {
    Set<String> urls = new HashSet<>();
    Pattern pattern = Pattern.compile("<a\\s+href=\"([^\"]*)\"[^>]*>");
    Matcher matcher = pattern.matcher(html);

    String[] parseURL = parseURL(originalUrl);
    while (matcher.find()) {
      String url = matcher.group(1);
      if (parseURL(url)[0] != null) {
        url = cleanCompleteURL(url);
      } else {
        url = cleanURL(url, parseURL);
      }

      if (url != null) {
        urls.add(url);
      }
    }

    List<String> toReturn = new ArrayList<>();
    toReturn.addAll(urls);

    return toReturn;
  }

  public static void run(FlameContext context, String[] args) throws Exception {
    RowToString firstLambda = row -> {
      return row.get("url") + "," + row.get("page");
    };
    StringToPair secondLambda = str -> {
      String[] splitString = str.split(",", 2);
      List<String> itemList = getURLsFromPage(splitString[1], splitString[0]);
      StringBuilder builder = new StringBuilder();
      builder.append("1.0,1.0");
      for (String item : itemList) {
        builder.append(',');
        builder.append(item);
      }
      return new FlamePair(splitString[0], builder.toString());
    };

    // Get all hyperlinks of a page for pagerank
    FlameRDD rdd = context.fromTable("crawl", firstLambda);
    FlamePairRDD stateTable = rdd.mapToPair(secondLambda);

    while (true) {
      // Calculate the portion of rank given for each page's child pages
      PairToPairIterable p2p = pair -> {
        List<FlamePair> pairList = new ArrayList<>();

        String[] splitString = pair._2().split(",");
        double curRank = Double.parseDouble(splitString[0]);

        double numElems = splitString.length - 2;
        double v = 0.85 * (curRank / numElems);
        pairList.add(new FlamePair(pair._1(), Double.toString(0.0)));
        for (int i = 2; i < splitString.length; i++) {
          String curURL = splitString[i];
          pairList.add(new FlamePair(curURL, Double.toString(v)));
        }

        return pairList;
      };
      FlamePairRDD noAggregatedTransferTable = stateTable.flatMapToPair(p2p);

      // Group the pages from above to find new pagerank
      TwoStringsToString aggregateTransfers = (first, second) -> {
        double accumulator = Double.parseDouble(first);
        double curVal = Double.parseDouble(second);

        return Double.toString(accumulator + curVal);
      };
      FlamePairRDD transferTable = noAggregatedTransferTable.foldByKey(Double.toString(0.15), aggregateTransfers);

      // Join old table with new to update state
      FlamePairRDD joined = stateTable.join(transferTable);
      PairToPairIterable p2pJoin = pair -> {
        List<FlamePair> list = new ArrayList<>();

        String[] items = pair._2().split(",");
        items[1] = items[0];
        items[0] = items[items.length - 1];

        StringBuilder builder = new StringBuilder();
        builder.append(items[0]);
        for (int i = 1; i < items.length - 1; i++) {
          builder.append(',');
          builder.append(items[i]);
        }

        list.add(new FlamePair(pair._1(), builder.toString()));
        return list;
      };
      stateTable = joined.flatMapToPair(p2pJoin);

      // Find differences between previous and current
      PairToStringIterable difference = pair -> {
        List<String> string = new ArrayList<>();

        String[] split = pair._2().split(",");
        double diff = Double.parseDouble(split[0]) - Double.parseDouble(split[1]);
        diff = Math.abs(diff);

        string.add(Double.toString(diff));
        return string;
      };
      FlameRDD differences = stateTable.flatMap(difference);

      // Find max difference
      TwoStringsToString findMaxDif = (first, second) -> {
        double firstDouble = Double.parseDouble(first);
        double secondDouble = Double.parseDouble(second);

        double max = Math.max(firstDouble, secondDouble);
        return Double.toString(max);
      };
      double maxDifference = Double.parseDouble(differences.fold(Double.toString(-1), findMaxDif));

      // Terminate if converge
      double convergenceThreshold = Double.parseDouble(args[0]);
      if (maxDifference < convergenceThreshold) {
        break;
      }

    }

    // Add it to the table
    String master = context.getKVS().getMaster();
    PairToPairIterable saveResultIterable = pair -> {
      KVSClient client = new KVSClient(master);
      String rank = pair._2().split(",")[0];
      client.put("pageranks", pair._1(), "rank", rank);

      return new ArrayList<>();
    };
    stateTable.flatMapToPair(saveResultIterable);

  }
}