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
import java.net.Socket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.BlockingQueue;

import cis5550.tools.Logger;

public class Worker extends Thread {
  BlockingQueue<Socket> queue;
  String directory;

  private static final Logger logger = Logger.getLogger(Worker.class);

  private static String parseRequest(ArrayList<String> lines) {
    boolean containsHost = false;
    String requestString = lines.get(0);

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

    if (splitRequestString[0].equals("POST") || splitRequestString[0].equals("PUT")) {
      return "405 Not Allowed";
    }

    if (!splitRequestString[0].equals("GET") && !splitRequestString[0].equals("HEAD")) {
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

  private static String getFilePath(String requestString) {
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

  private static void sendResponse(String filePath, String responseCode, Socket socket, ArrayList<String> lines)
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

  // EXTRA CREDIT 2: IF-MODIFIED SINCE METHOD
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

  // EXTRA CREDIT 3: RANGE REQUEST
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

  public Worker(BlockingQueue<Socket> queue, String directory) {
    this.queue = queue;
    this.directory = directory;
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

        while ((nRead = input.read()) != -1) {
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
              String filePath = directory + "/" + getFilePath(lines.get(0));
              sendResponse(filePath, responseCode, socket, lines);

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
              String filePath = directory + "/" + getFilePath(lines.get(0));
              sendResponse(filePath, responseCode, socket, lines);

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

