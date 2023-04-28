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
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Stemmer;

import java.io.FileNotFoundException;
import java.io.IOException;

public class Indexer {
	
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
		
		words.put("_", Arrays.asList(-1));
		
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
		
		String indexTabName = "index";
		
		PairToPairIterable pageToWord = (s -> {
			
			// extract url, page from intermediate table 2
			String url = s._1(); String page = s._2();
			
			// extract words, locations of words from page string
			Map<String, List<Integer>> word_locs = split_page_locs(page);
			
			// define output list
			List<FlamePair> out = new LinkedList<FlamePair>();
			
			for (Entry<String, List<Integer>> info : word_locs.entrySet()) {
				
				String w = info.getKey(); List<Integer> pos = info.getValue();
				
				// assemble a string of word positions, later add to the end of the URL to be indexed
				String pos_string = ";" + pos.size() + " ";
				for (int p : pos) {pos_string = pos_string + p + " ";}
				
				String url_pos = url + pos_string;
				
				stemmer = new Stemmer(); stemmer.add(w.toCharArray(), w.length()); stemmer.stem(); String stem_w = stemmer.toString();
				
				// add a url with the word and the url string to the table
				out.add(new FlamePair(w, url_pos)); out.add(new FlamePair(stem_w, url_pos));
				
				System.out.println(w + " => " + url_pos);
			}
			
			return out;
		});

		// Parallelized job

		// convert pages, urls from crawl to flat mapping of "url,page" strings
		String delimiter = "~~~";
		RowToString saveCrawl = (r -> {return r.get("url") + delimiter + r.get("page");});
		FlameRDD intermediate1 = ctx.fromTable("crawl", saveCrawl);
		
		String name1 = "intermediate1";
		intermediate1.saveAsTable(name1);
		kvs.persist(name1);
		
		System.out.println("INT 1");

		// convert "url,page" string to mapping of url, page pairs
		FlamePairRDD intermediate2 = intermediate1.mapToPair(s -> new FlamePair(s.split(delimiter)[0], s.split(delimiter)[1]));
		
		String name2 = "intermediate2";
		intermediate2.saveAsTable(name2);
		kvs.persist(name2);
		
		System.out.println("INT 2");

		// convert url -> page to url -> word
		FlamePairRDD intermediate3 = intermediate2.flatMapToPair(pageToWord);
		
		String name3 = "intermediate3";
		intermediate3.saveAsTable(name3);
		kvs.persist(name3);
		
		System.out.println("INT 3");
		
		// fold the previous result to an index
		FlamePairRDD index = intermediate3.foldByKey("", (s1,s2) -> {return s1 + "," + s2;});
		
		String name4 = "index_output";
		index.saveAsTable(name4);
		kvs.persist(name4);
		
		System.out.println("INDEX OUT");
		
		Iterator<Row> rows = kvs.scan(name4);
		
		while (rows.hasNext()) {
			Row row = rows.next();
			String word = row.key(); String urls = row.get("acc");
			kvs.put("index", word, "url", urls);
		}
		
		ctx.output("OK");
	
	}
}
