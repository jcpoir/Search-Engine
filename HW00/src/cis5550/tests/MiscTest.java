package cis5550.tests;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import cis5550.tools.URLParser;
import cis5550.jobs.Crawler;
import cis5550.kvs.KVSClient;
import cis5550.jobs.Indexer;
import cis5550.tools.Stemmer;

public class MiscTest {
	
	public static String compToURL(String[] components) {
		
		String protocol = components[0]; String hostname = components[1]; String port = components[2]; String path = components[3];
		
		if (!Objects.isNull(protocol)) {protocol = protocol + "://";} else {protocol = "";}
		if (Objects.isNull(hostname)) {hostname = "";}
		if (!Objects.isNull(port)) {port =  ":" + port;} else {port = "";}
		if (Objects.isNull(path)) {path = "";}
	
		return protocol + hostname + port + path;
	}
	
	public static List<Double> sortTopN(List<Double> input, int n) {
		
		List<Double> output = new LinkedList<Double>(); int size = 1;
		
		output.add(input.get(0)); input.remove(0);
		
		for (double N_in : input) {
			
			int i = 0; boolean done = false;
			for (double N_out : output) {
				
				if (N_in >= N_out) {
					
					done = true;
					output.add(i, N_in); size++;
					
					if (size > n) {
						output.remove(size - 1); size--;
					}
					
					break;
				}
				
				i++;
				}
				
			if (!done) {
				output.add(N_in); 
				size++;
				if (size > n) {
					output.remove(size - 1); size--;
				}
			}
				
		}
		
		return output;
	}
	
	public static List<Double> sortTopN2(List<Double> input, int n) {
		
		List<Double> output = new LinkedList<Double>(); int size = 1;

		Queue<Double> maxHeap = new PriorityQueue<Double>(Collections.reverseOrder());
		
		maxHeap.addAll(input);
		
		for (int i = 0; i < n; i++) {
			output.add(maxHeap.poll());
		}
		
		return output;
	}
		
	public static void main(String[] args) throws IOException {
		
		// testing speed for sorting 200K doubles on one device.
		Random random = new Random();
		
		List<Double> scores = new LinkedList<Double>();
		
		int n = 1000000;
		
		for (int i = 0; i < n; i++) {
			scores.add(random.nextDouble());
		}
		
		// top "N" function
		long time = System.currentTimeMillis();
		
		List<Double> out = sortTopN2(scores, 1000);
		
		time = System.currentTimeMillis() - time;
		
		System.out.format("[EXP] Execution for n = %d finished in t = %d.3 ms\n\n", n, time);
		
		time = System.currentTimeMillis();
		
		Collections.sort(scores);
		
		time = System.currentTimeMillis() - time;
		
		System.out.format("[CONTROL] Execution for n = %d finished in t = %d.3 ms", n, time);
	}

}
