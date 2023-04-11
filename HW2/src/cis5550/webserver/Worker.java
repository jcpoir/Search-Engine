package cis5550.webserver;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import cis5550.tools.Logger;

public class Worker extends Thread {
  private static BlockingQueue<Socket> queue;
  private static Map<String, String> hostToDirectoryMap;
  private static Map<String, Map<String, Map<String, Route>>> hostMap;
  private static Server server;
  private static final Logger logger = Logger.getLogger(Worker.class);

  private static String parseRequest(ArrayList<String> lines) {
    boolean containsHost = false;
    String requestString = lines.get(0);

    HashSet<String> responseCodes = new HashSet<>() {
      {
        add("GET");
        add("HEAD");
        add("POST");
        add("PUT");
      }
    };

    for (String header : lines.subList(1, lines.size())) {
      String[] splitted = header.split(" ");
      if (splitted[0].toLowerCase().equals("host:")) {
        containsHost = true;
      }
    }

    String[] splitRequestString = requestString.split(" ");

    if (splitRequestString.length != 3 || !containsHost) {
      return "400 Bad Request";
    }

    if (!splitRequestString[2].equals("HTTP/1.1")) {
      return "505 HTTP Version Not Supported";
    }

    if (!responseCodes.contains(splitRequestString[0])) {
      return "501 Not Implemented";
    }

    return "200 OK";
  }

  private static int getContentLength(ArrayList<String> lines) {
    int contentLength = 0;

    for (String header : lines.subList(1, lines.size())) {
      String[] splitted = header.split(" ");
      if (splitted.length == 2 && splitted[0].toLowerCase().equals("content-length:")) {
        contentLength = Integer.parseInt(splitted[1]);
      }
    }

    return contentLength;
  }

  private static String getFilePath(String requestString) {
    String[] splitRequestString = requestString.split(" ");

    if (splitRequestString.length == 3) {
      return splitRequestString[1].split("\\?")[0];
    }

    return null;
  }

  private static String getFilePathWithQueryParams(String requestString) {
    String[] splitRequestString = requestString.split(" ");

    if (splitRequestString.length == 3) {
      return splitRequestString[1];
    }

    return null;
  }

  private static String getRequestType(String requestString) {
    String[] splitRequestString = requestString.split(" ");

    if (splitRequestString.length == 3) {
      return splitRequestString[0];
    }

    return null;
  }

  private static String getProtocol(String requestString) {
    String[] splitRequestString = requestString.split(" ");

    if (splitRequestString.length == 3) {
      return splitRequestString[2];
    }

    return null;
  }

  private static Map<String, String> getHeaders(ArrayList<String> lines) {
    Map<String, String> headers = new HashMap<>();

    for (String header : lines.subList(1, lines.size())) {
      String[] splitted = header.split(": ");
      if (splitted.length == 2) {
        headers.put(splitted[0].toLowerCase(), splitted[1]);
      }
    }

    return headers;
  }

  private static long getFileLength(String filePath) {
    File file = new File(filePath);

    return file.length();
  }

  private static String getContentType(String requestString) {
    String[] splitRequestString = requestString.split(" ");

    if (splitRequestString.length == 3) {
      String[] splitFileName = splitRequestString[1].split("\\.");
      if (splitFileName.length == 1) {
        return "application/octet-stream";
      }

      String fileExtension = splitFileName[1];

      if (fileExtension.equals("jpg") || fileExtension.equals("jpeg")) {
        return "image/jpeg";
      }

      if (fileExtension.equals("txt")) {
        return "text/plain";
      }

      if (fileExtension.equals("html")) {
        return "text/html";
      }
    }

    return "application/octet-stream";
  }

  private static Map<String, String> getParams(String splitPath, String filePath) {
    Map<String, String> params = new HashMap<>();

    if (splitPath.split("/").length <= 2) {
      return params;
    }

    String[] originalPathSplit = splitPath.split("/");
    String[] filePathSplit = filePath.split("/");
    for (int i = 1; i < originalPathSplit.length; i++) {
      if (originalPathSplit[i].charAt(0) == ':') {
        params.put(originalPathSplit[i].substring(1), filePathSplit[i]);
      }
    }
    return params;
  }

  private static Map<String, String> getQueryParams(String path) {
    Map<String, String> map = new HashMap<>();
    path = URLDecoder.decode(path, StandardCharsets.UTF_8);
    String[] separate = path.split("\\?");
    if (separate.length == 1) {
      return map;
    }

    String[] queries = separate[1].split("\\&");
    for (String query : queries) {
      String[] keyValuePair = query.split("\\=");
      map.put(keyValuePair[0], keyValuePair[1]);
    }

    return map;
  }

  private static Map<String, String> getQueryParamsBody(Map<String, String> initialQueryParams, String messageBody) {
    Map<String, String> map = initialQueryParams;
    messageBody = URLDecoder.decode(messageBody, StandardCharsets.UTF_8);

    if (messageBody.length() == 0) {
      return map;
    }

    String[] queries = messageBody.split("\\&");
    for (String query : queries) {
      String[] keyValuePair = query.split("\\=");
      if (keyValuePair.length > 1) {
        map.put(keyValuePair[0], keyValuePair[1]);
      }
    }

    return map;
  }

  private static String getHost(ArrayList<String> lines) {
    String host = "localhost";

    for (String header : lines.subList(1, lines.size())) {
      String[] splitted = header.split(" ");
      if (splitted.length == 2 && splitted[0].toLowerCase().equals("host:")) {
        host = splitted[1].split("\\:")[0];
      }
    }

    return host;
  }

  private static String checkFilePath(String filePath, ArrayList<String> lines) throws ParseException {
    if (filePath.contains("..")) {
      return "403 Forbidden";
    }

    File file = new File(filePath);

    if (!file.exists()) {
      return "404 Not Found";
    }

    if (!file.canRead()) {
      return "403 Forbidden";
    }

    // EXTRA CREDIT NO 2
    long lastDownloadedTime = getLastDownloadedTime(lines);
    long fileLastModified = file.lastModified();

    if (lastDownloadedTime >= fileLastModified) {
      return "304 Not Modified";
    }

    return "200 OK";
  }

  private static String lookupPath(Map<String, Route> map, String filePath) {
    String result = null;

    String[] splitFilePath = filePath.split("/");
    if (splitFilePath.length <= 2) {
      return result;
    }

    Set<String> keys = map.keySet();
    for (String key : keys) {
      boolean isMatch = true;
      String[] splitted = key.split("/");
      for (int i = 1; i < splitted.length; i++) {
        if (splitted[i].charAt(0) == ':') {
          continue;
        }
        if (!(splitted[i].equals(splitFilePath[i]))) {
          isMatch = false;
          break;
        }
      }

      if (isMatch) {
        result = key;
        break;
      }
    }

    return result;
  }

  private static void sendHeaderResponse(OutputStream outputStream, String initialResponseCode, long contentLength,
      String contentType) {
    PrintWriter out = new PrintWriter(outputStream, true);
    String response = "HTTP/1.1 " +
        initialResponseCode +
        "\r\n" +
        "Content-Type: " +
        contentType +
        "\r\n" +
        "Server: localhost\r\n" +
        "Content-Length: " +
        (contentLength) +
        "\r\n\r\n";
    out.print(response);
    out.flush();
  }

  private static void sendFile(OutputStream outputStream, String filePath) throws IOException {
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    int bytes = 0;

    File file = new File(filePath);
    FileInputStream fileInputStream = new FileInputStream(file);
    byte[] buffer = new byte[(int) file.length()];
    while ((bytes = fileInputStream.read(buffer)) != -1) {
      dataOutputStream.write(buffer, 0, buffer.length);
      dataOutputStream.flush();
    }

    fileInputStream.close();
  }

  private static void sendStaticResponse(String filePath, String responseCode, Socket socket, ArrayList<String> lines)
      throws IOException, ParseException {
    if (responseCode.equals("200 OK")) {
      responseCode = checkFilePath(filePath, lines);
    }

    if (!responseCode.equals("200 OK")) {
      sendHeaderResponse(socket.getOutputStream(), responseCode, 0, getContentType(lines.get(0)));
    } else {
      String rangeRequest = getRange(lines);

      if (rangeRequest != null) {
        responseCode = "206 OK";
        sendHeaderResponse(socket.getOutputStream(), responseCode, getFileLengthRange(rangeRequest, filePath), getContentType(lines.get(0)));
      } else {
        sendHeaderResponse(socket.getOutputStream(), responseCode, getFileLength(filePath), getContentType(lines.get(0)));
      }
      
      if (getRequestType(lines.get(0)).equals("GET") && rangeRequest != null) {
        sendFileRange(socket.getOutputStream(), filePath, rangeRequest);
      } else if (getRequestType(lines.get(0)).equals("GET")) {
        sendFile(socket.getOutputStream(), filePath);
      }
    }
  }

  private static void sendDynamicResponse(OutputStream outputStream, String responseCode,
      Map<String, ArrayList<String>> headers, byte[] body) throws IOException {
    PrintWriter out = new PrintWriter(outputStream, true);
    String initialResponse = "HTTP/1.1 " + responseCode + "\r\n";
    StringBuilder builder = new StringBuilder(initialResponse);
    for (Map.Entry<String, ArrayList<String>> entry : headers.entrySet()) {
      String header = entry.getKey().toUpperCase();
      for (String value : entry.getValue()) {
        builder.append(header + ": " + value + "\r\n");
      }
    }

    builder.append("\r\n");
    out.print(builder.toString());
    out.flush();

    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    dataOutputStream.write(body, 0, body.length);
    dataOutputStream.flush();
  }
  
  private static void sendDynamicContent(ArrayList<String> lines, Socket socket, Map<String, Route> table, StringBuilder messageBuilder, String splitPath) throws IOException {    
    String method = getRequestType(lines.get(0));
    String url = getFilePath(lines.get(0));
    String protocol = getProtocol(lines.get(0));
    InetSocketAddress inetSocketAddress = new InetSocketAddress(socket.getInetAddress(), socket.getPort());
    Map<String, String> headers = getHeaders(lines);
    Map<String, String> params = getParams(splitPath, getFilePath(lines.get(0)));
    Map<String, String> queryParams = getQueryParams(getFilePathWithQueryParams(lines.get(0)));
    
    if (getContentType(lines.get(0)).equals("application/octet-stream")) {
      String queries = messageBuilder.toString();
      queryParams = getQueryParamsBody(queryParams, queries);
    }

    Route route = table.get(splitPath);
    RequestImpl request = new RequestImpl(method, url, protocol, headers,
        queryParams, params,
        inetSocketAddress, messageBuilder.toString().getBytes(), server);

    
    ResponseImpl response = new ResponseImpl(socket.getOutputStream());

    try {
      Object handleCall = route.handle(request, response);

      if (response.getIsWriteCalled()) {
        socket.close();
      }

      if (!response.getIsWriteCalled() && handleCall != null) {
        response.type(getContentType(lines.get(0)));
        response.header("Content-Length", Integer.toString(handleCall.toString().length()));
        response.header("Server", getHost(lines));
        if (!response.getHeaders().containsKey("Content-Type")) {
          response.header("Content-Type", "text/html");
        }
        response.body(handleCall.toString());

        sendDynamicResponse(socket.getOutputStream(), response.getStatusCode(), response.getHeaders(),
            response.getBody());
      } else if (!response.getIsWriteCalled() && handleCall == null && response.getBody() == null) {
        response.type(getContentType(lines.get(0)));
        response.header("Content-Length", Integer.toString(0));
        response.header("Server", getHost(lines));
        if (!response.getHeaders().containsKey("Content-Type")) {
          response.header("Content-Type", "text/html");
        }

        sendDynamicResponse(socket.getOutputStream(), response.getStatusCode(), response.getHeaders(),
            response.getBody());
      } else if (!response.getIsWriteCalled() && handleCall == null) {
        response.header("Server", getHost(lines));
        if (!response.getHeaders().containsKey("Content-Type")) {
          response.header("Content-Type", "text/html");
        }

        sendDynamicResponse(socket.getOutputStream(), response.getStatusCode(), response.getHeaders(),
            response.getBody());
      }

    } catch (Exception e) {
      if (response.getIsWriteCalled()) {
        socket.close();
      } else {
        response.status(500, "Internal Server Error");
        response.type(getContentType(lines.get(0)));
        response.header("Content-Length", Integer.toString(0));
        response.header("Server", getHost(lines));
        if (!response.getHeaders().containsKey("Content-Type")) {
          response.header("Content-Type", "text/html");
        }

        sendDynamicResponse(socket.getOutputStream(), response.getStatusCode(), response.getHeaders(),
            response.getBody());
      }
    }
  }

  // EXTRA CREDIT 2 HW1: IF-MODIFIED SINCE METHOD
  private static long getLastDownloadedTime(ArrayList<String> lines) throws ParseException {
    long lastDownloaded = -1;

    for (String header : lines.subList(1, lines.size())) {
      String[] splitted = header.split(" ");
      if (splitted.length == 2 && splitted[0].toLowerCase().equals("if-modified-since:")) {
        String lastDownloadedString = splitted[1];
        SimpleDateFormat dateFormat = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss 'GMT'");
        Date date = dateFormat.parse(lastDownloadedString);
        lastDownloaded = date.getTime();
      }
    }

    return lastDownloaded;
  }

  // EXTRA CREDIT 3 HW1: RANGE REQUEST
  private static String getRange(ArrayList<String> lines) {
    String byteRanges = null;

    for (String header : lines.subList(1, lines.size())) {
      String[] splitted = header.split(" ");
      if (splitted.length == 2 && splitted[0].toLowerCase().equals("range:")) {
        if ((splitted[1].substring(0, 5)).equals("bytes")) {
          byteRanges = splitted[1].substring(6);
        }
      }
    }

    return byteRanges;
  }
  
  private static void sendFileRange(OutputStream outputStream, String filePath, String rangeString) throws IOException {
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    
    String[] ranges = rangeString.split(",");
    
    int bytes = 0;
    File file = new File(filePath);
    FileInputStream fileInputStream = new FileInputStream(file);
    byte[] buffer = new byte[(int) file.length()];
    while ((bytes = fileInputStream.read(buffer)) != -1) {
      for (String range : ranges) {
        String[] valueStrings = range.split("-");
        int off = Integer.parseInt(valueStrings[0]);

        if (off < 0) {
          off = buffer.length + off;
        }
        int len = buffer.length - off;
        if (valueStrings.length == 2) {
          len = Integer.parseInt(valueStrings[1]) - off + 1;
        }

        dataOutputStream.write(buffer, off, len);
        dataOutputStream.flush();
      }
    }

    fileInputStream.close();
  }

  private static int getFileLengthRange(String rangeString, String filePath) {
    int total = 0;
    File file = new File(filePath);
    
    String[] ranges = rangeString.split(",");
    for (String range : ranges) {
      String[] valueStrings = range.split("-");
      int off = Integer.parseInt(valueStrings[0]);

      if (off < 0) {
        off = (int) file.length() + off;
      }
      int len = (int) file.length() - off;
      if (valueStrings.length == 2) {
        len = Integer.parseInt(valueStrings[1]) - off + 1;
      }

      total += len;
    }

    return total;
  }

  public Worker(BlockingQueue<Socket> queue, Map<String, String> hostToDirectoryMap, Map<String, Map<String, Map<String, Route>>> hostMap, Server server) {
    Worker.queue = queue;
    Worker.hostToDirectoryMap = hostToDirectoryMap;
    Worker.hostMap = hostMap;
    Worker.server = server;
  }

  @Override
  public void run() {
    while (true) {
      try {
        Socket socket = queue.take();
        InputStream input = socket.getInputStream();

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] bytes = null;

        boolean encounteredCRLF = false;
        BufferedReader reader;
        ArrayList<String> lines = new ArrayList<>();
        String line;
        String responseCode = null;
        int contentLength = 0;
        StringBuilder messageBuilder = new StringBuilder();
        int counter = 0;

        while (!socket.isClosed() && ((nRead = input.read()) != -1)) {
          buffer.write(nRead);
          bytes = buffer.toByteArray();

          // ENCOUNTERED DOUBLE CRLF
          if (bytes.length > 4 && bytes[bytes.length - 4] == 13 && bytes[bytes.length - 3] == 10
              && bytes[bytes.length - 2] == 13
              && bytes[bytes.length - 1] == 10) {
            encounteredCRLF = true;
            reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes)));
            while ((line = reader.readLine()) != null && line.length() > 0) {
              lines.add(line);
            }
            responseCode = parseRequest(lines);
            contentLength = getContentLength(lines);


            // NO MESSAGE BODY
            if (contentLength == 0) {
              Map<String, Map<String, Route>> hostRoutingTable = hostMap.get(getHost(lines));
              if (hostRoutingTable == null) {
                hostMap.put(getHost(lines), new HashMap<>());
                hostRoutingTable = hostMap.get(getHost(lines));
              }
              Map<String, Route> table = hostRoutingTable.get(getRequestType(lines.get(0)));

              if (table == null || !(table.containsKey(getFilePath(lines.get(0))))) {
                // PERFORM PATH MATCHING LOOKUP 
                String lookup = lookupPath(table, getFilePath(lines.get(0)));
                if (lookup != null) {
                  sendDynamicContent(lines, socket, table, messageBuilder, lookup);
                } else {
                  // SEND STATIC FILES
                  String directory = hostToDirectoryMap.get(getHost(lines));
                  if (directory == null) {
                    directory = "";
                  }
                  String filePath = directory + "/" + getFilePath(lines.get(0));
                  sendStaticResponse(filePath, responseCode, socket, lines);
                }
              } else {
                // SEND DYNAMIC CONTENT
                sendDynamicContent(lines, socket, table, messageBuilder, getFilePath(lines.get(0)));
              }
              
              // RESET FOR NEXT REQUEST
              buffer = new ByteArrayOutputStream();
              lines = new ArrayList<>();
              encounteredCRLF = false;
              messageBuilder = new StringBuilder();
              counter = 0;
            }
          } else if (encounteredCRLF) {
            // READING MESSAGE BODY
            messageBuilder.append((char) bytes[bytes.length - 1]);
            counter += 1;

            // MESSAGE BODY FINISHED READING
            if (counter == contentLength) {
              Map<String, Map<String, Route>> hostRoutingTable = hostMap.get(getHost(lines));
              if (hostRoutingTable == null) {
                hostMap.put(getHost(lines), new HashMap<>());
                hostRoutingTable = hostMap.get(getHost(lines));
              }
              Map<String, Route> table = hostRoutingTable.get(getRequestType(lines.get(0)));

              if (table == null || !(table.containsKey(getFilePath(lines.get(0))))) {
                // PERFORM PATH MATCHING LOOKUP
                String lookup = lookupPath(table, getFilePath(lines.get(0)));
                if (lookup != null) {
                  sendDynamicContent(lines, socket, table, messageBuilder, lookup);
                } else {
                  // SEND STATIC FILES
                  String directory = hostToDirectoryMap.get(getHost(lines));
                  if (directory == null) {
                    directory = "";
                  }
                  String filePath = directory + "/" + getFilePath(lines.get(0));
                  sendStaticResponse(filePath, responseCode, socket, lines);
                }
              } else {
                // SEND DYNAMIC CONTENT
                sendDynamicContent(lines, socket, table, messageBuilder, getFilePath(lines.get(0)));
              }

              // RESET FOR NEXT REQUEST
              buffer = new ByteArrayOutputStream();
              lines = new ArrayList<>();
              encounteredCRLF = false;
              messageBuilder = new StringBuilder();
              counter = 0;
            }
          }
        }
        socket.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  } 
}

