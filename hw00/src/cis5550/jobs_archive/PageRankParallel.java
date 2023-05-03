package cis5550.jobs_archive;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameContext.RowToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

// import static cis5550.jobs.Crawler.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

public class PageRankParallel {
	
	public static float DECAY = (float) 0.85; public static float CONV_THRESH = (float) 0.01; public static float CONV_P = (float) 0.95;
	
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
		String out = url.split("#")[0]; return out; // isolate section of the url before pound sign
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
					if (components1[0].equals("https")) {components1[2] = "8000";}
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
	
	public static void run(FlameContext ctx, String[] args) throws Exception {
		
		String delimiter = "~~~";
		
		/*
		 * Initialize State Table
		 */
		
		// Unpack command line threshold, proportion
		if (args.length >= 1) {CONV_THRESH = Float.parseFloat(args[0]);}
		if (args.length >= 2) {CONV_P = Float.parseFloat(args[1]+".0") / 100;}
		
		String prTabName = "pageranks_temp";
		
		StringToPair extract_links = s -> {
			
			String[] contents = s.split(delimiter);
			String url = contents[0]; String page = contents[1];
			
			// Get components of current URL
			String[] components = parseURL(url);
			
			// Use crawler method to extract URLs
			List<String> urls_ = normalizeURLs(getURLs(page, "http"), components);
			String urls = String.join(",", urls_);
			
			FlamePair out = new FlamePair(url, "1.0,1.0" + delimiter + urls);
			
			return out;
		};
		
		PairToPairIterable compute_transfer = s -> {
			
			String url = s._1(); String contents_string = s._2();
			
			List<FlamePair> out = new LinkedList<FlamePair>();
			
			String[] contents = contents_string.split(delimiter);
			
			if (contents.length == 2) {
				String ranks_string = contents[0]; String links_string = contents[1];
				
				String[] links = links_string.split(","); String[] ranks = ranks_string.split(",");
				
				double rank = Double.parseDouble(ranks[1]);
				System.out.println(rank);
				String val = String.valueOf(rank / links.length * DECAY);
				
				for (String link: links) {
					
					out.add(new FlamePair(link, val));
				}
				
				// rank source
				out.add(new FlamePair(url, "0.15"));
			}
			
			return out;
		};
		
		PairToPairIterable update_states = s -> {
			
			String url = s._1(); String contents_string = s._2();
			
			List<FlamePair> out = new LinkedList<FlamePair>();
			
			String[] contents = contents_string.split(delimiter);
			
			String scores_string = contents[0]; String links_string = contents[1];
			
			if (contents.length == 2) {
				
				String[] links = links_string.split(",");
				
				String out_links = String.join(",", Arrays.copyOfRange(links, 0, links.length - 1));
				
				String out_scores = scores_string.split(",")[1] + "," + links[links.length - 1];
				
				out.add(new FlamePair(url, out_scores + delimiter + out_links));
			}
			
			return out;
		};
		
		PairToStringIterable check_convergence = s -> {
			
			List<String> out = new LinkedList<String>();
			
			String[] scores = s._2().split(delimiter)[0].split(",");
			
			// check if values have converged (diff < threshold)
			boolean isConverged = Math.abs(Double.parseDouble(scores[0]) - Double.parseDouble(scores[1])) < CONV_THRESH;
			
			String val = String.valueOf(isConverged ? 1 : 0); out.add(val);
					
			return out;
		};
		
		PairToPairIterable save_result = s -> {
			
			KVSClient kvs = new KVSClient("localhost:8000");
			
			String url = s._1(); String content = s._2();
			
			String pageRank = content.split(delimiter)[0].split(",")[1];
			
			kvs.put("pageranks", url, "rank", pageRank);
			
			return new LinkedList<FlamePair>();
		};
		
		
		/*
		 * Parallelized job
		 */
		
		KVSClient kvs = new KVSClient("localhost:8000");

		// convert pages, urls from crawl to flat mapping of "url,page" strings
		RowToString saveCrawl = (r -> {return r.get("url") + delimiter + r.get("page");});
		FlameRDD intermediate1 = ctx.fromTable("crawl", saveCrawl);

		// convert "url,page" string to mapping of url, score,score + delim + normalized url list
		FlamePairRDD stateTab = intermediate1.mapToPair(extract_links).foldByKey("", (s1,s2) -> (s1+s2));
		
		String name2 = "state";
		stateTab.saveAsTable(name2);
		kvs.persist(name2);
		
		// get total # docs.
		byte[] crawl_byte = kvs.get("index", "_", "url"); double n = Integer.valueOf(new String(crawl_byte).split(",").length); 
		
		for (int i = 0; i < 100; i++) {
		
			FlamePairRDD transferTab = stateTab.flatMapToPair(compute_transfer);
			
			FlamePairRDD aggTransferTab = transferTab.foldByKey("0.0", (s1,s2) -> {return String.valueOf(Double.parseDouble(s1) + Double.parseDouble(s2));});
			
			stateTab = stateTab.join(aggTransferTab).flatMapToPair(update_states).foldByKey("", (s1,s2) -> (s1+s2));
			
			double c = Integer.valueOf(stateTab.flatMap(check_convergence).fold("0", (s1,s2) -> (String.valueOf(Integer.parseInt(s1) + Integer.parseInt(s2)))));
			
			double p_conv = c/n;
			
			if (p_conv >= CONV_P) {System.out.println("CONVERGED!! " + c + "/" + n); break;}
			
			else {System.out.println("Continuing . . . " + c + "/" + n + " p_conv = " + p_conv);}
			
		}
		
		stateTab.flatMapToPair(save_result);
		
		/*
		 * End Parallelized job
		 */
		
		ctx.output("OK");
	}
}