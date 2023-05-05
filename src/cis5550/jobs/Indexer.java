package cis5550.jobs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlameContextImpl;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.EnglishFilter;
import cis5550.tools.Hasher;
import cis5550.tools.Stemmer;

import java.io.FileNotFoundException;
import java.io.IOException;

public class Indexer {
	
	static Stemmer stemmer; public static int MAX_WORD_LEN = 20; public static int MAX_PAGE_LEN = 15000; public static int MAX_URL_LEN = 100;
	static String delimiter1 = "^"; static String delimiter2 = "~"; public static boolean load_index = false; public static int interval = 500;
	
	static Set<String> skips = new HashSet<String> (Arrays.asList(
			"http", "https", "com", "net", "edu", "org", "gov", "www", "xml", "ttl", "xmlj","rdf"
	));
	
	public static String randomName() {
		
		// (1) Pick a table name (using hash)
		String tabname = ""; Random r = new Random();
		
		// generate a unique ID string of 20 ASCII characters by generating random ints and casting to char
		for (int i = 0; i < 50; i++) {
			tabname = tabname + (char) (r.nextInt(26) + 'a');
		}
		
		return tabname;
	}

	public static String extract_word(String word) {
		
		String out = ""; char[] info = word.toCharArray(); int start = 0; int end = 0;
		
		// get index of first alphabetic character
		for (int i = 0; i < info.length; i++) {
			if (Character.isLetterOrDigit(info[i])) {start = i; break;}
		}
		
		// get index of last alphabetic character
		for (int j = info.length - 1; j >= 0; j--) {
			if (Character.isLetterOrDigit(info[j])) {end = j; break;}
		}
		
		// slice out word and return
		out = String.valueOf(Arrays.copyOfRange(info, start, end+1)); return out;
	}
	
	public static List<String> extract_html(String word) {
		
		List<String> words = new LinkedList<String>();
		
		String currWord = "";
		
		// for html: only look for words where bracket number == 0 
		int bracketNum = 0;
		
		// Go letter by letter, assembling word array
		for (char letter : word.toCharArray()) {
			
			// Use these conditions to track html tags. If currently in a tag (bracketNum != 0), only keep track of tag conditions
			if ('<' == letter) {
				// At the start of each tag, add the running current word to the output list
				if (!(currWord.length() == 0)) {words.add(currWord); currWord = "";}
				bracketNum++; 
				continue;
			}
			if (bracketNum > 0 & '>' == letter) {bracketNum--; continue;}
			if (bracketNum != 0) {continue;}
			
			// replace all non-letters w/ space
			if (!Character.isLetterOrDigit(letter)) {letter = ' ';}
			
			// if not in a tag, start tracking the word
			currWord = currWord + letter;
			
		}
		
		// add remaining word components to word
		if (!(currWord.length() == 0)) {words.add(currWord); currWord = "";}
		
		return words;
	}
	
	public static Set<String> split_page(String page) {
		
		// Ensure all lowercase
		page = page.toLowerCase();
		
		// remove html tags and return html-free page fragments
		List<String> segments = extract_html(page); 
		Set<String> words = new HashSet<String>(); 
		
		// split each segment by space to pseudo-words
		for (String segment : segments) {
			
			for (String p_word : segment.split(" ")) {
				
				// clean up word, then add to words
				words.add(extract_word(p_word));
			}
		}
		
		return words;
	}
	
	public static Map<String, List<Integer>> split_page_locs(String page) {
		/*
		 * [EC1] Word positions
		 */
		
		// Ensure all lowercase
		if (page.length() > MAX_PAGE_LEN) {page = page.substring(0, MAX_PAGE_LEN);}
		page = page.toLowerCase();
		
		// remove html tags and return html-free page fragments
		List<String> segments = extract_html(page); 
		Map<String, List<Integer>> words = new HashMap<String, List<Integer>>(); 
		
		// split each segment by space to pseudo-words
		int i = 1;
		for (String segment : segments) {
			
			for (String p_word : segment.split("\\P{Alnum}+")) {
				
				if (!"".equals(p_word)) {
				
					// clean up word, removing leading/trailing non-alphanum characters
					String cleaned_word = extract_word(p_word);
					
					List<Integer> pos = words.get(cleaned_word);
					if (Objects.isNull(pos)) {pos = new LinkedList<Integer>();}
					
					// clean up word, then add to words
					pos.add(i); words.put(cleaned_word, pos);
					
					// System.out.println(p_word + " => " + cleaned_word + " (CLEANED)");
					
					i++;
				}
			}
		}
		
		words.remove("");
		
		return words;
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

	public static void load_from_persistent(String persistent_name, String memory_name, KVSClient kvs) throws FileNotFoundException, IOException {
		
		Iterator<Row> persistent = kvs.scan(persistent_name);
		
		System.out.println("\n== Loading Existing Index ==\n");
		
		int i = 0;
		
		while(persistent.hasNext()) {
			i++;
			System.out.println(i); 
			
			Row row = persistent.next();
			kvs.putRow(memory_name, row);
		}
	}
	
	public static void deposit(String intervalTabName, String indexTabName, KVSClient kvs) throws FileNotFoundException, IOException {
		
		Iterator<Row> rows = kvs.scan(intervalTabName);
		
		System.out.println("IntervalTabName: " + intervalTabName + " IndexTabName: " + indexTabName);
		
		while (rows.hasNext()) {
			try {
				Row row = rows.next();
				
				byte[] url_byte = kvs.get(indexTabName, row.key(), "url"); String url = "";
				if (!Objects.isNull(url_byte)) {url = new String(url_byte);}
				
				kvs.put(indexTabName, row.key(), "url", row.get("url") + delimiter2 + url);}
			catch (Exception e) {}
		}
	}
	
	public static Map<String,String> get_pageranks(KVSClient kvs) throws FileNotFoundException, IOException {
		
		Map<String,String> out = new HashMap<String,String>();
		
		Iterator<Row> rows = kvs.scan("pageranks");
		
		int i = 0;
		while(rows.hasNext()) {
			i++; System.out.println("Pagerank load: " + i);
			Row row = rows.next(); out.put(row.key(), new String(row.get("rank")));
		}
		
		return out;
	}
	
	public static void run(FlameContext ctx, String[] url_arg) throws Exception {
		
		// Initialize kvs client
		KVSClient kvs = new KVSClient("Localhost:8000");
		
		// load existing index
		String prevIndexName = "index_old"; String indexTabName = "index_temp"; String outTabName = "index";
		kvs.delete("index_temp");
		
		if (load_index) {load_from_persistent(prevIndexName, indexTabName, kvs);}

		// Get rows of the crawl table, which contains page data
		String prevCrawlName = "crawl_old"; Iterator<Row> crawl = kvs.scan("crawl");
		
		int i = 1; int startRow = 0; int endRow = interval; String intervalTabName = indexTabName + String.valueOf(endRow);
		
		Map<String,String> pageranks = get_pageranks(kvs);
		
		// Iterate through webpages in crawl
		while (crawl.hasNext()) {

			// extract page data, url
			Row row = crawl.next(); String page = row.get("page"); String url = row.get("url");
			
			if (i > endRow) {
				System.out.println("\n== COMBINING TABLES ==\n");
				endRow = endRow + interval;
				deposit(intervalTabName, indexTabName, kvs);
				kvs.delete(intervalTabName);
				intervalTabName = indexTabName + String.valueOf(endRow);
			}
			
			System.out.print("(" + i + ") " + url); i++;
			
			if (Objects.isNull(page) | Objects.isNull(url) | Objects.isNull(EnglishFilter.filter(url))) {System.out.println("[✗]"); continue;}
			if (url.length() > MAX_URL_LEN) {System.out.println("[✗]"); continue;}
			if (page.length() > MAX_PAGE_LEN) {page = page.substring(0, MAX_PAGE_LEN);}
			
			System.out.println("[✓]");

			// initialze unique word set and add all words from the page to it
			String[] components = parseURL(url);
			
			Map<String, List<Integer>> words = split_page_locs(components[1] + " " + components[3] + " " + page);

			// add a dummy "word" for deterimining document length
			List<Integer> dummy_pos = new LinkedList<Integer>();
			
			String pagerank = "0.1";
			if (pageranks.containsKey(url)) {pagerank = pageranks.get(url);}

			// add the current url under each word in the index table
			for (Entry<String, List<Integer>> entry : words.entrySet()) {

				// extract word, word positions as key-value pair from split entry set
				String word = entry.getKey(); List<Integer> pos = entry.getValue();
				
				if (word.length() > MAX_WORD_LEN | skips.contains(word)) {continue;}
				
				// assemble a string of word positions, later add to the end of the URL to be indexed
				String pos_string = pos.size() + " ";

				for (int p : pos) {pos_string = pos_string + p + " ";}
				
				/*
				 * Get & add pagerank
				 */
				
				pos_string = "^" + pagerank + " " + pos_string;

				stemmer = new Stemmer(); stemmer.add(word.toCharArray(), word.length()); stemmer.stem(); String stem_word = stemmer.toString();

				// System.out.println(word + "(" + word.length() + ") => " + stem_word + "(" + stem_word.length() + ") (STEM) ");

				byte[] curr = kvs.get(intervalTabName, word, "url");
				byte[] curr_stem = kvs.get(intervalTabName, stemmer.toString(), "url");

				/*
				 * NORMAL
				 */

				String mod_url = url + pos_string;

				// if entry for word exists in the table, append new url
				if (!Objects.isNull(curr)) {kvs.put(intervalTabName, word, "url", new String(curr) + delimiter2 + mod_url);}

				// otherwise, start a new entry with the current url
				else {kvs.put(intervalTabName, word, "url", mod_url);}

				/*
				 * STEMMED
				 */

				// if entry for word exists in the table, append new url
				if (!Objects.isNull(curr_stem)) {kvs.put(intervalTabName, stem_word, "url", new String(curr_stem) + delimiter2 + mod_url);}

				// otherwise, start a new entry with the current url
				else {kvs.put(intervalTabName, stem_word, "url", mod_url);}
			}		
		}
		
		deposit(intervalTabName, indexTabName, kvs);
		kvs.delete(intervalTabName);
		
		kvs.persist(outTabName);
		
		Iterator<Row> indices = kvs.scan(indexTabName);
		
		while (indices.hasNext()) {
			Row row = indices.next();
			kvs.putRow(outTabName, row);
		}
		
		ctx.output("OK");
	}
}
