package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.EnglishFilter;

// import static cis5550.jobs.Crawler.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Map;
import java.util.Map.Entry;

public class PageRank {
	
	public static String delimiter1 = "~";
	
	public static float DECAY = (float) 0.85; public static float CONV_THRESH = (float) 0.01; public static float CONV_P = (float) 0.98;
	
	public static String randomName() {
		
		// (1) Pick a table name (using hash)
		String tabname = ""; Random r = new Random();
		
		// generate a unique ID string of 20 ASCII characters by generating random ints and casting to char
		for (int i = 0; i < 50; i++) {
			tabname = tabname + (char) (r.nextInt(26) + 'a');
		}
		
		return tabname;
	}
	
	public static String removeDocFrag(String url) {
		if (url.startsWith("#")) {return null;}
		String out = url.split("#")[0]; return out; // isolate secion of the url before pound sign
	}
	
	public static String[] parseURL(String url) {
	    String result[] = new String[4];
	    int slashslash = url.indexOf("//");
	    if (slashslash>0) {
	      result[0] = url.substring(0, slashslash-1);
	      int nextslash = url.indexOf('/', slashslash+2);
	      if (nextslash>=0) {
	        result[1] = url.substring(slashslash+2, nextslash);
	        result[3] = url.substring(nextslash);
	      } else {
	        result[1] = url.substring(slashslash+2);
	        result[3] = "/";
	      }
	      int colonPos = result[1].indexOf(':');
	      if (colonPos > 0) {
	        result[2] = result[1].substring(colonPos+1);
	        result[1] = result[1].substring(0, colonPos);
	      }
	    } else {
	      result[3] = url;
	    }

	    return result;
	  }

	public static String compToURL(String[] components) {
		
		String protocol = components[0]; String hostname = components[1]; String port = components[2]; String path = components[3];
		
		if (!Objects.isNull(protocol)) {protocol = protocol + "://";} else {protocol = "";}
		if (Objects.isNull(hostname)) {hostname = "";}
		if (!Objects.isNull(port)) {port =  ":" + port;} else {port = "";}
		if (Objects.isNull(path)) {path = "";}
	
		return protocol + hostname + port + path;
	}
	
	public static List<String> getURLs(String term, String protocol) {
		
		List<String> URLs = new LinkedList<String>();
		
		// get individual words from file
		String[] words = term.split("\n");
		
		for (String word : words) {
			
			// scan word looking for urls
			if (word.contains("<") && word.contains(">")) {
				
				// split word by space & iterate
				String[] sections = word.split(" ");
				
				for (String section : sections) {
					
					// if a secition starts with the href tag, isolate the url using a quote split
					if (section.toLowerCase().startsWith("href=")) {
						
						String[] subSections = section.split("\"");
						
						if (subSections.length < 2) {continue;}
						
						// second split item will be the URL (first will be the "href=" tag itself)
						String url = subSections[1];
						
						// add url iff (1) URL is not a banned filetype & (2) URL uses a valid protocol
						if ((!url.endsWith(".jpg")) & (!url.endsWith(".jpeg")) & (!url.endsWith(".png")) & (!url.endsWith(".gif")) & (!url.endsWith(".txt")) & ("http".equals(protocol) | ("https").equals(protocol))) {URLs.add(subSections[1]);}
					}
				}
			}
		}
		
		return URLs;
	}
	
	public static List<String> normalizeURLs(List<String> urls, String[] components) {
		
		List<String> norm_urls = new LinkedList<String>();
		
		for (String url: urls) {
			
			// remove pound sign and everything after it. If this eliminates full URL, don't record URL.
			url = removeDocFrag(url);
			if (Objects.isNull(url)) {continue;}
			
			
			String[] components1 = parseURL(url);
			
			// if there are any empty components in the url, try to fill them with default values
			int i = 0;
			for (String component1 : components1) {
				
				if (i == 2) {
					if (components1[0].equals("https")) {components1[2] = "443";}
					else {components1[2] = "80";}
				}
				
				// replace null components with corresponding components from parent url
				else if (component1 == null) {components1[i] = components[i];}
					
				
				else if (i == 3) {
					
					// if we get a "..", skip it
					if (components1[i].startsWith("..")) {components1[i] = components1[i].substring(2);}
					
					// handle non-route html case (blah.html#test from handout)
					else if (!components1[i].startsWith("/")) {
						
						// remove last element of path (to be replaced)
						List<String> path_elements = Arrays.asList(components[i].split("/"));
						
						if (path_elements.size() > 0) {

							path_elements = path_elements.subList(0, path_elements.size() - 1);
							
							// reform string array, then join and concat new path element
							components1[i] = String.join("/", path_elements) + "/" + components1[i];
						}
						
						else {components1[i] = "/" + components1[i];}

					}
				
				}
				i++;
			}
			
			// add normalized url
			// System.out.println(url + " => " + compToURL(components1));
			norm_urls.add(compToURL(components1));
		}
		
		return norm_urls;
	}
	
	public static void run(FlameContext ctx, String[] args) throws FileNotFoundException, IOException {
		
		/*
		 * Initialize State
		 */
		
		// Unpack command line threshold, proportion
		if (args.length >= 1) {CONV_THRESH = Float.parseFloat(args[0]);}
		if (args.length >= 2) {CONV_P = Float.parseFloat(args[1]+".0") / 100;}
		
		// Initialize key-value-store client using KVS Master address
		KVSClient kvs = new KVSClient("Localhost:8000");

		// state map (instead of table)
		Map<String,Map<String,String>> state = new HashMap<String,Map<String,String>>();
		
		boolean initialize = true;
		
		if (initialize) {
		
			// Use kvs to get the crawl table
			Iterator<Row> crawlTable = kvs.scan("crawl");
			
			int i = 1; System.out.println("\n== INITIALIZATION ==\n");
			
			// Iterate through the crawl table, extracting urls from page data
			while (crawlTable.hasNext()) {
				
				System.out.println("Pages extracted: " + i); i++;
				
				// Get next row, get page data
				Row row = crawlTable.next(); String page = row.get("page"); String url = row.get("url");
				
				if (i % 8 == 3) {continue;}
				
				if (Objects.isNull(page) | Objects.isNull(url)) {continue;}
				if (Objects.isNull(EnglishFilter.filter(url))) {continue;}
				
				page = page.toLowerCase(); url = url.toLowerCase();
				
				// Get components of current URL
				String[] components = parseURL(url);
				
				// Use crawler method to extract URLs
				List<String> urls_ = normalizeURLs(getURLs(page, "https"), components);
				String urls = String.join(delimiter1, urls_);
				
				try {
					Map<String, String> ref = new HashMap<String,String>();
					ref.put("rank0", "1.0"); ref.put("rank1", "1.0"); ref.put("n", String.valueOf(urls_.size())); ref.put("url", urls);
					state.put(url, ref);
				}
				catch (Exception e) {
					System.out.println("[403 ERROR] Route Forbidden: " + url);
				}
				
			}
		}
		
		Map<String,Double> transfer = new HashMap<String,Double>();
		
		for (int i = 0; i < 20; i++) {
			
			/*
			 * Compute Transfer Table
			 */
			
			System.out.println("\n== EPOCH: " + i + " ==\n");
			
			int url_num = 0;
			
			for (Entry<String,Map<String,String>> entry : state.entrySet()) {
				
				url_num++;
				
				// for each entry in state table get links, ranks
				String url = entry.getKey(); Map<String,String> metrics = entry.getValue();
				double rank1 = Double.parseDouble(metrics.get("rank1")); String urls = metrics.get("url"); String[] url_arr = urls.split(delimiter1);
								
				System.out.println("(" + url_num + ") url: " + url + " url_arr.length: " + url_arr.length + " rank1: " + rank1);
				
				// Only perform algo on linked pages that are in the original table
				List<String> linked = new LinkedList<String>();
				
				for (String out_url : url_arr) {
					if (state.containsKey(out_url)) {
						linked.add(out_url);
					}
				}
				
				int n = linked.size();
				
				if (n == 0) {continue;}
				
				double val = rank1 / n * DECAY; 
				// System.out.println(rank1 + " => " + val);
				
				for (String out_url : linked) {
					
					if (transfer.containsKey(out_url)) {
						transfer.put(out_url, transfer.get(out_url) + val);
					}
					
					else {transfer.put(out_url, val);}
				}
			}
			
			/*
			 * Update State table & check convergence
			 */
			
			int n_converged = 0; int n = 0;
			
			for (Entry<String,Map<String,String>> entry : state.entrySet()) {
				
				String url = entry.getKey(); Map<String,String> metrics = entry.getValue();
				double rank1 = Double.parseDouble(metrics.get("rank1")); String urls = metrics.get("url"); String[] url_arr = urls.split(delimiter1);
				
				// get new rank from transfer table and deposit in rank1
				double new_rank = 0;
				if (transfer.containsKey(url)) {new_rank = transfer.get(url);}
				
				// add new rank to state table. Also, add 0.15 rank to each source url.
				new_rank = new_rank + 0.15;
				
				metrics.put("rank0", String.valueOf(rank1)); metrics.put("rank1", String.valueOf(new_rank));
				state.put(url, metrics);
				
				transfer.put(url, 0.0);
				
				// convergence check
				double diff = Math.abs(new_rank - rank1);
				
				// track the number of URLs that have converged
				n++; if (diff < CONV_THRESH) {n_converged++; System.out.println("[✓] " + url + ":" + diff);} 
				else {System.out.println("[✗] " + url + ":" + diff);}
				
			}
			
			if (((float) n_converged / (float) n) >= CONV_P) {System.out.println("CONVERGENCE !!! p = " + n_converged + "/" + n); break;}
			else {System.out.println("No convergence: p = " + n_converged + "/" + n);}
		}
		
		/*
		 * Export results to pagerank table
		 */
		
		String finalTabName = "pageranks";
		kvs.persist(finalTabName);
		
		for (Entry<String,Map<String,String>> entry : state.entrySet()) {
			
			String url = entry.getKey(); Map<String,String> metrics = entry.getValue(); String rank = metrics.get("rank1");
			 
			// add info to final table
			try {
				kvs.put(finalTabName, url, "rank", rank);
			}
			catch (Exception e) {
				System.out.println("[403 ERROR] Route Forbidden: " + url);
			}
		}
		
		ctx.output("OK");
	}
}
