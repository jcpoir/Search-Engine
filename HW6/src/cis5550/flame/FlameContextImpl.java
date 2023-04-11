package cis5550.flame;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Partitioner;
import cis5550.tools.Partitioner.Partition;

public class FlameContextImpl implements FlameContext{
  private static KVSClient client; 
  private static String masterArg;
  private String storeOutput = null;
  private String jarName;

  public String getOutput() {
    if (storeOutput == null) {
      storeOutput = "The body has no output";
    }

    return storeOutput;
  }

  public FlameContextImpl(String jar, String masterArg) {
    FlameContextImpl.masterArg = masterArg;
    this.jarName = jar;
    FlameContextImpl.client = new KVSClient(masterArg);
  }

	@Override
	public KVSClient getKVS() {
		return client;
	}

	@Override
	public void output(String s) {
    if (storeOutput == null)  {
      storeOutput = "";
    }

		storeOutput += s;
	}

	@Override
	public FlameRDD parallelize(List<String> list) throws Exception {
		String jobID = UUID.randomUUID().toString();
    for (String item : list) {
      client.put(jobID, UUID.randomUUID().toString(), "value", item);
    }

    FlameRDDImpl flameRDD = new FlameRDDImpl(jobID, masterArg);
    return flameRDD;
	}

  public static String invokeOperation(String requestType, String operationName, byte[] lambda, String oldTable,
    String zeroElementFold) throws IOException, InterruptedException {
    String jobID = UUID.randomUUID().toString();
    Partitioner partitioner = new Partitioner();

    // Part 1: Add Workers to the partitioner
    int lastWorkerIndex = client.numWorkers() - 1;
    for (int i = 0; i < lastWorkerIndex; i++) {
      partitioner.addKVSWorker(client.getWorkerAddress(i), client.getWorkerID(i), client.getWorkerID(i + 1));
    }
    partitioner.addKVSWorker(client.getWorkerAddress(lastWorkerIndex), null, client.getWorkerID(0));
    partitioner.addKVSWorker(client.getWorkerAddress(lastWorkerIndex), client.getWorkerID(lastWorkerIndex), null);

    Vector<String> flameWorkers = cis5550.generic.Master.getWorkers();
    for (String str : flameWorkers) {
      partitioner.addFlameWorker(str);
    }

    // Part 2: Send requests to each worker
    Vector<Partition> partitions = partitioner.assignPartitions();

    Thread threads[] = new Thread[partitions.size()];
    String results[] = new String[partitions.size()];    
    for (int i = 0; i < partitions.size(); i++) {
      final String url = "http://" + partitions.elementAt(i).assignedFlameWorker + operationName + "?" + 
        getQueryParams(partitions.elementAt(i), oldTable, jobID, zeroElementFold);
      final int j = i;
      threads[i] = new Thread(() -> {
        try {
          results[j] = new String(HTTP.doRequest(requestType, url, lambda).body());
				} catch (IOException e) {
          results[j] = "Exception: " + e;
					e.printStackTrace();
				}
      });
      threads[i].start();
    }
    for (Thread thread : threads) {
      thread.join();
    }

    return jobID;
  }
  
  private static String getQueryParams(Partition p, String oldTable, String newTable, String zeroElementFold) throws UnsupportedEncodingException {
    StringBuilder queryParams = new StringBuilder();
    queryParams.append("oldtable=" + oldTable + "&");
    queryParams.append("newtable=" +  newTable + "&");
    queryParams.append("kvsworker=" + p.kvsWorker + "&");
    queryParams.append("fromkey=" + p.fromKey + "&");
    queryParams.append("tokey=" + p.toKeyExclusive + "&");
    queryParams.append("flameworker=" + p.assignedFlameWorker + "&");
    queryParams.append("masterarg=" + masterArg + "&");

    if (zeroElementFold != null) {
      queryParams.append("zeroelement=" + URLEncoder.encode(zeroElementFold, StandardCharsets.UTF_8));
    }

    return queryParams.toString();
  }
}
