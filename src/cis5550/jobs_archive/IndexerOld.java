package cis5550.jobs_archive;

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
import cis5550.tools.Stemmer;

import java.io.FileNotFoundException;
import java.io.IOException;

public class IndexerOld {
	
	static Stemmer stemmer;
	
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
		page = page.toLowerCase();
		
		// remove html tags and return html-free page fragments
		List<String> segments = extract_html(page); 
		Map<String, List<Integer>> words = new HashMap<String, List<Integer>>(); 
		
		// split each segment by space to pseudo-words
		int i = 1;
		for (String segment : segments) {
			
			for (String p_word : segment.split(" ")) {
				
				if (!"".equals(p_word)) {
				
					// clean up word, removing leading/trailing non-alphanum characters
					String cleaned_word = extract_word(p_word);
					
					List<Integer> pos = words.get(cleaned_word);
					if (Objects.isNull(pos)) {pos = new LinkedList<Integer>();}
					
					// clean up word, then add to words
					pos.add(i); words.put(cleaned_word, pos);
					
					i++;
				}
			}
		}
		
		words.remove("");
		
		return words;
	}
	
	public static void run(FlameContext ctx, String[] url_arg) throws Exception {
		
		// Initialize kvs client
		KVSClient kvs = new KVSClient("Localhost:8000");
		
		// Get rows of the crawl table, which contains page data
		Iterator<Row> crawl = kvs.scan("crawl");
		
		String indexTabName = "indexOld";
		
		// Iterate through webpages in crawl
		while (crawl.hasNext()) {
			
			// extract page data, url
			Row row = crawl.next(); String page = row.get("page"); String url = row.get("url");
			
			// initialze unique word set and add all words from the page to it
			Map<String, List<Integer>> words = split_page_locs(page);
			
			// add a dummy "word" for deterimining document length
			List<Integer> dummy_pos = new LinkedList<Integer>(); dummy_pos.add(0); words.put("_",dummy_pos);
						
			// add the current url under each word in the index table
			for (Entry<String, List<Integer>> entry : words.entrySet()) {
				
				// extract word, word positions as key-value pair from split entry set
				String word = entry.getKey(); List<Integer> pos = entry.getValue();
				
				// assemble a string of word positions, later add to the end of the URL to be indexed
				String pos_string = ";" + pos.size() + " ";
				for (int p : pos) {pos_string = pos_string + p + " ";}
				
				stemmer = new Stemmer(); stemmer.add(word.toCharArray(), word.length()); stemmer.stem(); String stem_word = stemmer.toString();
				
				System.out.println(word + "(" + word.length() + ") => " + stem_word + "(" + stem_word.length() + ") (STEM) ");
				
				byte[] curr = kvs.get(indexTabName, word, "url");
				byte[] curr_stem = kvs.get(indexTabName, stemmer.toString(), "url");
				
				/*
				 * NORMAL
				 */
				
				String mod_url = url + pos_string;
				
				// if entry for word exists in the table, append new url
				if (!Objects.isNull(curr)) {kvs.put(indexTabName, word, "url", new String(curr) + "," + mod_url);}
				
				// otherwise, start a new entry with the current url
				else {kvs.put(indexTabName, word, "url", mod_url);}
				
				/*
				 * STEMMED
				 */
				
				// if entry for word exists in the table, append new url
				if (!Objects.isNull(curr_stem)) {kvs.put(indexTabName, stem_word, "url", new String(curr_stem) + "," + mod_url);}
				
				// otherwise, start a new entry with the current url
				else {kvs.put(indexTabName, stem_word, "url", mod_url);}
			}		
		}
	}
}
