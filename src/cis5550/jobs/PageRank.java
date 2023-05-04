package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.EnglishFilter;

// import static cis5550.jobs.Crawler.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

public class PageRank {
	
	public static float DECAY = (float) 0.85; public static float CONV_THRESH = (float) 0.01; public static float CONV_P = (float) 0.8;
	
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
		 * Initialize State Table
		 */
		
		// Unpack command line threshold, proportion
		if (args.length >= 1) {CONV_THRESH = Float.parseFloat(args[0]);}
		if (args.length >= 2) {CONV_P = Float.parseFloat(args[1]+".0") / 100;}
		
		String prTabName = "pageranks_temp";
		
		// Initialize key-value-store client using KVS Master address
		KVSClient kvs = new KVSClient("Localhost:8000"); kvs.delete("transfer");
		
		boolean initialize = false;
		
		if (initialize) {
			
			kvs.delete(prTabName);
		
			// Use kvs to get the crawl table
			Iterator<Row> crawlTable = kvs.scan("crawl");
			
			// Iterate through the crawl table, extracting urls from page data
			while (crawlTable.hasNext()) {
				
				// Get next row, get page data
				Row row = crawlTable.next(); String page = row.get("page"); String url = row.get("url");
				
				if (Objects.isNull(page) | Objects.isNull(url) | Objects.isNull(EnglishFilter.filter(url))) {continue;}
				
				
				page = page.toLowerCase(); url = url.toLowerCase();
				
				// Get components of current URL
				String[] components = parseURL(url);
				
				// Use crawler method to extract URLs
				List<String> urls_ = normalizeURLs(getURLs(page, "https"), components);
				String urls = String.join(",", urls_);
				
				try {
					kvs.put(prTabName, url, "rank0", "1.0"); 
					kvs.put(prTabName, url, "rank1", "1.0"); 
					kvs.put(prTabName, url, "n", "" + urls_.size());
					kvs.put(prTabName, url, "url", urls);
				}
				catch (Exception e) {
					System.out.println("[403 ERROR] Route Forbidden: " + url);
				}
			}
		}
		
		String transTabName = "transfer";
		
		for (int i = 0; i < 1000; i++) {
			
			/*
			 * Compute Transfer Table
			 */
			
			System.out.println("EPOCH: " + i);
			
			// Use kvs to get the transfer table
			Iterator<Row> transfer = kvs.scan(prTabName);
			
			int url_num = 0;
			
			while (transfer.hasNext()) {
				
				url_num++;
				
				// for each entry in state table get links, ranks
				Row row = transfer.next(); String url = row.key(); String urls = row.get("url"); String[] url_arr = urls.split(","); float rank1 = Float.parseFloat(row.get("rank1"));
				
				System.out.println("(" + url_num + ") url: " + url + " url_arr.length: " + url_arr.length + " rank1: " + rank1);
				
				// Only perform algo on linked pages that are in the original table
				List<String> linked = new LinkedList<String>();
				
				for (String out_url : url_arr) {
					if (!Objects.isNull(kvs.get(prTabName, out_url,  "n"))) {
						linked.add(out_url);
					}
				}
				
				int n = linked.size();
				
				if (n == 0) {continue;}
				
				float val = rank1 / n * DECAY; 
				System.out.println(rank1 + " => " + val);
				
				for (String out_url : linked) {
					
					if (out_url.isEmpty()) {continue;}
					
					// add rows to transfer table
					byte[] curr = kvs.get(transTabName, out_url, "val");
					
					try {
					
					// if there is a current value in the table,
					if (!Objects.isNull(curr)) {
						kvs.put(transTabName, out_url, "val", "" + (Float.parseFloat(new String(curr)) + val));
					}
					else {kvs.put(transTabName, out_url, "val", "" + val);}
					}
					
					catch (Exception e) {
						continue;
					}
				}
			}
			
			/*
			 * Update State table & check convergence
			 */
			
			int n_converged = 0; int n = 0;
			
			Iterator<Row> state = kvs.scan(prTabName);
			
			while(state.hasNext()) {
				
				// get info from each row
				Row row = state.next(); String url = row.key(); String rank0 = row.get("rank1"); 
				
				// get new rank from transfer table and deposit in rank1
				byte[] rank1 = kvs.get(transTabName, url, "val"); if (Objects.isNull(rank1)) {rank1 = "0".getBytes();}
				
				// add new rank to state table. Also, add 0.15 rank to each source url.
				float r = Float.parseFloat(new String(rank1)); r = (float) (r + 0.15); rank1 = String.valueOf(r).getBytes();
				
				try {
					kvs.put(prTabName, url, "rank0", rank0);
					kvs.put(transTabName, url, "val", "0"); // reset all transfer values to zero
					kvs.put(prTabName, url, "rank1", rank1);
				}
				
				catch (Exception e) {
					System.out.println("[403 ERROR] Route Forbidden: " + url);
				}
				
				// convergence check
				float r1 = Float.parseFloat(new String(rank1)); float r0 = Float.parseFloat(new String(rank0));
				float diff = Math.abs(r1 - r0);
				
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
		
		Iterator<Row> state = kvs.scan(prTabName);
		
		while(state.hasNext()) {
			
			// get info from row
			Row row = state.next(); String url = row.key(); String rank = row.get("rank1");
			
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