package cis5550.webserver;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SessionImpl implements Session{
  private String id = UUID.randomUUID().toString();
  private long creationTime = System.currentTimeMillis();
  private long lastAccessedTime = System.currentTimeMillis();
  private int maxActiveInterval = 300;
  private Map<String, Object> attributes = new HashMap<>();
  private boolean isSessionExpired = false;
  
  public String id() {
    return id;
  }

  public long creationTime() {
    return creationTime;
  }

  public long lastAccessedTime() {
    return lastAccessedTime;
  }

  public void updateLastAccessedTime() {
    lastAccessedTime = System.currentTimeMillis();
  }

  public void maxActiveInterval(int seconds) {
    maxActiveInterval = seconds;
  }

  public void invalidate() {
    isSessionExpired = true;
  }

  public Object attribute(String name) {
    return attributes.get(name);
  }

  public void attribute(String name, Object value) {
    attributes.put(name, value);
  }
  
  public boolean getIsSessionExpired() {
    return isSessionExpired;
  }

  public int getMaxActiveInterval() {
    return maxActiveInterval;
  }
}
