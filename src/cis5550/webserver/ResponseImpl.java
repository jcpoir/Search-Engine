package cis5550.webserver;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import cis5550.tools.Logger;

public class ResponseImpl implements Response{
  private static final Logger logger = Logger.getLogger(ResponseImpl.class);

  private String statusCode = "200 OK";
  private byte[] body = null;
  private Map<String, ArrayList<String>> headers = new HashMap<>();
  private OutputStream outputStream;
  private boolean isWriteCalled = false;
  private DataOutputStream dataOutputStream;

  public ResponseImpl(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  public Map<String, ArrayList<String>> getHeaders() {
    return headers;
  }

  public String getStatusCode() {
    return statusCode;
  }

  public byte[] getBody() {
    return body;
  }

  public boolean getIsWriteCalled() {
    return isWriteCalled;
  }

  @Override
  public void body(String body) {
    if (isWriteCalled) {
      return;
    }

    this.body = body.getBytes();
  }
  @Override
  public void bodyAsBytes(byte[] bodyArg) {
    if (isWriteCalled) {
      return;
    }

    this.body = bodyArg;
  }

  @Override
  public void header(String name, String value) {
    if (isWriteCalled) {
      return;
    }

    if (!headers.containsKey(name)) {
      headers.put(name, new ArrayList<>());
    }
    headers.get(name).add(value);
  }

  @Override
  public void type(String contentType) {
    header("Content-Type", contentType);
  }

  @Override
  public void status(int statusCode, String reasonPhrase) {
    if (isWriteCalled) {
      return;
    }

    this.statusCode = (statusCode + " " + reasonPhrase);   
  }

  private void sendInitialResponse(String responseCode,
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

    this.dataOutputStream = new DataOutputStream(outputStream);
    this.dataOutputStream.write(body, 0, body.length);
    this.dataOutputStream.flush();
  }

  @Override
  public void write(byte[] b) throws Exception {    
    if (!isWriteCalled) {
      this.header("Connection", "close");
      if (!this.headers.containsKey("Content-Type")) {
        this.header("Content-Type", "text/html");
      }
      sendInitialResponse(statusCode, headers, b);
      isWriteCalled = true;
    } else {
      this.dataOutputStream.write(b);
      this.dataOutputStream.flush();
    }
  }

  @Override
  public void redirect(String url, int responseCode) {
    // TODO Auto-generated method stub
  }

  @Override
  public void halt(int statusCode, String reasonPhrase) {
    // TODO Auto-generated method stub
  }
  
}
