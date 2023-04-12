package cis5550.jobs;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.*;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class Crawler {
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

  private static String cleanSeedURL(String url) {
    if (url.contains("#")) {
      url = url.split("#")[0];
      if (url == null || url.length() == 0) {
        return null;
      }
    }

    String[] parseHyperlink = parseURL(url);
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

        // Patch: Mistake
        // Delete remaining elements if new url has fewer elements than old
        if (curIndexReplace < splitSlashesOriginalURL.size() && curIndexReplace >= 0) {
          splitSlashesOriginalURL.subList(curIndexReplace, splitSlashesOriginalURL.size()).clear();
        }
      }
    }

    StringBuilder builder = new StringBuilder();
    builder.append(parseURL[0] + "://");
    // Patch: Thought the port number was always going to be specified 
    // in the page URL, so previously parseURL[2] would be null and 
    // add null to the URL
    builder.append(parseURL[1]);
    if (parseURL[2] != null) {
      builder.append(":" + parseURL[2]);
    } else {
      if (parseURL[0].equals("http")) {
        parseURL[2] = "80";
      } else {
        parseURL[2] = "443";
      }
    }
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
    if (url.contains("#")) {
      url = url.split("#")[0];
      if (url == null || url.length() == 0) {
        return null;
      }
    }

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

  private static boolean robot(String content, String url) {
    if ("".equals(content)) {
      return false;
    }

    final String PATTERNS_USERAGENT = "(?i)^User-agent:.*";
    final String PATTERNS_DISALLOW = "(?i)Disallow:.*";
    final String PATTERNS_ALLOW = "(?i)Allow:.*";
    final int PATTERNS_USERAGENT_LENGTH = 11;
    final int PATTERNS_DISALLOW_LENGTH = 9;
    final int PATTERNS_ALLOW_LENGTH = 6;

    boolean inMatchingUserAgent = false;
    HashSet<String> allowSet = new HashSet<>();
    HashSet<String> disallowSet = new HashSet<>();

    StringTokenizer st = new StringTokenizer(content, "\n");
    while (st.hasMoreTokens()) {
      String line = st.nextToken();

      if (line.length() == 0) {
        continue;
      }

      if (line.matches(PATTERNS_USERAGENT)) {
        String ua = line.substring(PATTERNS_USERAGENT_LENGTH).trim().toLowerCase();
        if ("*".equals(ua) || "cis5550-crawler".equals(ua)) {
          inMatchingUserAgent = true;
        } else {
          inMatchingUserAgent = false;
        }
      } else if (line.matches(PATTERNS_DISALLOW)) {
        if (!inMatchingUserAgent) {
          continue;
        }
        String path = line.substring(PATTERNS_DISALLOW_LENGTH).trim();
        if (path.endsWith("*")) {
          path = path.substring(0, path.length() - 1);
        }
        path = path.trim();
        if (path.length() > 0) {
          disallowSet.add(path);
        }
      } else if (line.matches(PATTERNS_ALLOW)) {
        if (!inMatchingUserAgent) {
          continue;
        }
        String path = line.substring(PATTERNS_ALLOW_LENGTH).trim();
        if (path.endsWith("*")) {
          path = path.substring(0, path.length() - 1);
        }
        path = path.trim();
        allowSet.add(path);
      }
    }

    int disallowSize = 0;
    for (String s : disallowSet) {
      if (url.startsWith(s) && (s.length() > disallowSize)) {
        disallowSize = s.length();
      }
    }

    if (disallowSize > 0) {
      for (String s : allowSet) {
        if (url.startsWith(s) && (s.length() > disallowSize)) {
          return false;
        }
      }

      return true;
    } else {
      return false;
    }

    // System.out.println(disallowSet);
    // System.out.println(allowSet);

  }

  private static boolean checkRateLimit(byte[] hostRobotFile, String str, long lastAccessTime) throws IOException {
    InputStream is = new ByteArrayInputStream(hostRobotFile);
    BufferedReader bfReader = new BufferedReader(new InputStreamReader(is));
    String subLink = parseURL(str)[3];
    boolean containsCrawlDelay = false;
    boolean isFoundAgent = false;

    String temp = bfReader.readLine();
    while (temp != null) {
      if (temp.equals("User-agent: cis5550-crawler")) {
        isFoundAgent = true;
        temp = bfReader.readLine();
        while (temp != null && temp.length() > 1) {
          if (temp.contains("Crawl-delay")) {
            containsCrawlDelay = true;
            float parseDelaySeconds = Float.parseFloat(temp.split(" ")[1]);
            long parseDelayAllowed = (long) (parseDelaySeconds * 1000);
            parseDelayAllowed = parseDelayAllowed * 1000;

            long curTime = System.currentTimeMillis();
            if (curTime - lastAccessTime <= parseDelayAllowed) {
              return true;
            }
          }

          temp = bfReader.readLine();
        }
      }
      temp = bfReader.readLine();
    }
    if (isFoundAgent) {
      is = new ByteArrayInputStream(hostRobotFile);
      bfReader = new BufferedReader(new InputStreamReader(is));
      temp = bfReader.readLine();
      while (temp != null) {
        if (temp.equals("User-agent: *")) {
          isFoundAgent = true;
          temp = bfReader.readLine();
          while (temp != null && temp.length() > 1) {
            if (temp.contains("Crawl-delay")) {
              containsCrawlDelay = true;
              float parseDelaySeconds = Float.parseFloat(temp.split(" ")[1]);
              long parseDelayAllowed = (long) (parseDelaySeconds * 1000);
              parseDelayAllowed = parseDelayAllowed * 1000;

              long curTime = System.currentTimeMillis();
              if (curTime - lastAccessTime <= parseDelayAllowed) {
                return true;
              }
            }

            temp = bfReader.readLine();
          }
        }
        temp = bfReader.readLine();
      }
    }

  
    if (!containsCrawlDelay) {
      long curTime = System.currentTimeMillis();
      if (curTime - lastAccessTime <= 1000) {
        return true;
      }
    }

    return false;
  }

  public static void run(FlameContext context, String args[]) throws Exception {
    if (args.length != 1) {
      context.output("ERROR: Args length not equal 1");
      return;
    }

    String master = context.getKVS().getMaster();

    StringToIterable lambda = (String str) -> {
      List<String> list = new ArrayList<>();
      KVSClient client = new KVSClient(master);

      // Check if webpage is already crawled
      if (client.getRow("crawl", Hasher.hash(str)) != null) {
        return list;
      }

      if (str.equals(args[0])) {
        str = cleanSeedURL(str);
      }
      URL url = new URL(str);

      String hostKey = parseURL(str)[1] + ":" + parseURL(str)[2];
      // Read Robots
      byte[] hostRobot = client.get("hosts", hostKey, "robots");
      if (hostRobot == null) {
        String[] parseURL = parseURL(str);
        String robotsURLstr = parseURL[0] + "://" + parseURL[1] + ":" + parseURL[2] + "/robots.txt";
        URL robotsURL = new URL(robotsURLstr);
        HttpURLConnection con = (HttpURLConnection) robotsURL.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("User-Agent", "cis5550-crawler");

        if (con.getResponseCode() == 200) {
          Row newRow = new Row(hostKey);
          InputStream stream = con.getInputStream();
          byte[] input = stream.readAllBytes();
          newRow.put("robots", input);
          client.putRow("hosts", newRow);
        } else {
          client.putRow("hosts", new Row(hostKey));
        }
      }

      // Check Robot Allowed or Disallowed
      hostRobot = client.get("hosts", parseURL(str)[1] + ":" + parseURL(str)[2], "robots");
      boolean notProceed = robot(new String(hostRobot, "utf-8"), parseURL(str)[3]);
      if (notProceed) {
        return list;
      }

      // Rate Limiting
      byte[] timeBytes = client.get("hosts", parseURL(str)[1], "time");
      long lastAccessTime = -1;
      if (timeBytes != null && timeBytes.length > 0) {
        lastAccessTime = ByteBuffer.wrap(timeBytes).getLong();
      }
      boolean outsideRateLimit = checkRateLimit(hostRobot, str, lastAccessTime);
      if (outsideRateLimit) {
        list.add(str);
        return list;
      } else {
        byte[] curTimeBytes = ByteBuffer.allocate(Long.BYTES).putLong(System.currentTimeMillis()).array();
        client.put("hosts", parseURL(str)[1], "time", curTimeBytes);
      }

      // Initial HEAD Connection
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("HEAD");
      con.setRequestProperty("User-Agent", "cis5550-crawler");
      con.setInstanceFollowRedirects(false);
      
      Row row = new Row(Hasher.hash(str));
      row.put("url", str);
      row.put("responseCode", Integer.toString(con.getResponseCode()));
      if (con.getContentType() != null) {
        row.put("contentType", con.getContentType());
      }
      if (con.getContentLengthLong() != -1) {
        row.put("length", Long.toString(con.getContentLengthLong()));
      }

      // Redirection
      Set<Integer> redirectCodes = Set.of(301, 302, 303, 307, 308);
      if (redirectCodes.contains(con.getResponseCode())) {
        String redirectURL = con.getHeaderField("Location");
        String[] parseURL = parseURL(str);
        if (parseURL(redirectURL)[0] != null) {
          redirectURL = cleanCompleteURL(redirectURL);
        } else {
          redirectURL = cleanURL(redirectURL, parseURL);
        }
        list.add(redirectURL);
      }
      
      // Crawl Webpage
      if (con.getResponseCode() == 200) {
        con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("User-Agent", "cis5550-crawler");
        row.put("responseCode", Integer.toString(con.getResponseCode()));
        if (con.getContentType().equals("text/html")) {
          InputStream stream = con.getInputStream();
          byte[] input = stream.readAllBytes();
          row.put("page", input);

          list = getURLsFromPage(new String(input), str);
        }
      }
      
      client.putRow("crawl", row);

      return list;
    };

    FlameRDD urlQueue = context.parallelize(Arrays.asList(args));
    while (urlQueue.count() != 0) {
      urlQueue = urlQueue.flatMap(lambda);
      Thread.sleep(500);
    }
    
    context.output("OK");
  }
}
