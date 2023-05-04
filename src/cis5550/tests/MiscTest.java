package cis5550.tests;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import cis5550.tools.URLParser;
import cis5550.jobs.Crawler;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.jobs.Indexer;
import cis5550.jobs.PageRank;
import cis5550.jobs.Search;
import cis5550.tools.EnglishFilter;
import cis5550.tools.Hasher;
import cis5550.tools.Stemmer;
import cis5550.tools.EnglishFilter;

public class MiscTest {
	
	static String delimiter1 = "^"; static String delimiter2 = "~";
	static Stemmer stemmer;
	
	static Set<String> skips = new HashSet<String> (Arrays.asList(
			"http", "https", "com", "net", "edu", "org", "gov", "www", "xml", "ttl", "xmlj","rdf"
	));
	
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
		
		KVSClient kvs = new KVSClient("localhost:8000");
		
		Iterator<Row> rows = kvs.scan("crawl"); kvs.delete("url_index");
		
		int i = 0;
		while (rows.hasNext()) {
			
			// if (i == 500) {break;}
			i++;
			
			String url = rows.next().get("url");
			
			if (skips.contains(url) | Objects.isNull(EnglishFilter.filter(url))) {continue;}
			
			System.out.println("("+i+") " + url);
			
			String[] components = PageRank.parseURL(url);
			
			Set<String> element_set = new HashSet<String>();
			
			if (Objects.isNull(components[1])) {components[1] = "";}
			if (Objects.isNull(components[3])) {components[3] = "";}
			
			System.out.println(Arrays.asList(components).toString());
			
			element_set.addAll(Arrays.asList(components[1].split("\\P{Alnum}+"))); element_set.addAll(Arrays.asList(components[3].split("\\P{Alnum}+")));
			
			for (String element : element_set) {
				
				if ("".equals(element)) {continue;}
				
				try {
					
					element = element.toLowerCase();
					stemmer = new Stemmer(); stemmer.add(element.toCharArray(), element.length()); stemmer.stem(); 
					element = stemmer.toString();
					
					byte[] pages_byte = kvs.get("url_index", element, "url");
					
					if (Objects.isNull(pages_byte)) {
						kvs.put("url_index", element, "url", url);
					}
					
					else {
						String pages = new String(pages_byte) + delimiter1 + url;
						kvs.put("url_index", element, "url", pages);
						
					}
				} catch (Exception e) {
					System.out.println("Exception: " + element);
				}
			}
		}
		
		kvs.persist("url_index_persist");
		
		rows = kvs.scan("url_index");
		
		while(rows.hasNext()) {
			Row row = rows.next();
			kvs.putRow("url_index_persist", row);
		}
	}

}
