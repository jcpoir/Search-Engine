package cis5550.generic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.net.URL;

public class Worker {
  private static String generateNewID(String storageDirectory) throws IOException {
    StringBuilder randomID = new StringBuilder();
    for (int i = 0; i < 5; i++) {
      Random rnd = new Random();
      char c = (char) ('a' + rnd.nextInt(26));
      randomID.append(c);
    }

    File newFile = new File(storageDirectory + "id");
    newFile.createNewFile();
    BufferedWriter writer = new BufferedWriter(new FileWriter(storageDirectory + "id", false));
    writer.write(randomID.toString());
    writer.close();

    return randomID.toString();
  }

  public static void startPingThread(String address, String storageDirectory, int portNum) {
    Thread thread = new Thread(() -> {
      while (true) {
        try {
          File file = new File(storageDirectory + "/id");

          String id;
          if (!file.exists()) {
            id = generateNewID(storageDirectory);
          } else {
            try {
              BufferedReader br = new BufferedReader(new FileReader(file));
              String line = br.readLine();
              if (line == null) {
                id = generateNewID(storageDirectory);
              } else {
                id = line;
              }
              br.close();
            } catch (Exception e) {
              e.printStackTrace();
              id = generateNewID(storageDirectory);
            }
          }

          String urlString = String.format("http://%s/ping?id=%s&port=%s", address, id, Integer.toString(portNum));
          URL url = new URL(urlString);
          url.getContent();

          Thread.sleep(5000);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    thread.start();
  }
}
