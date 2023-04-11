package cis5550.webserver;

import static cis5550.webserver.Server.*;

public class ServerLaunch {
  public static void main(String args[]) throws Exception {
    securePort(443);
    host("chacheng.cis5550.net", "keystore.jks", "secret");
    get("/", (req, res) -> {
      return "Hello World - this is Charles Cheng";
    });
  }
}
