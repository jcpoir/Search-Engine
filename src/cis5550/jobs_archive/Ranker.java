package cis5550.jobs_archive;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameContextImpl;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Stemmer;
import cis5550.tools.EnglishFilter;

public class Ranker {
	
	static double pagerank_weight = 0.0; static double tf_idf_weight = 1 - pagerank_weight; static double URL_bonus = 10;
	
	static Stemmer stemmer; static String delimiter1 = "^"; static String delimiter2 = "~";
	
	public static String[] split_stem(String query) {
		
		String[] out = query.toLowerCase().split("\\P{Alnum}+");
		
		stemmer = new Stemmer();
		
		for (int i = 0; i < out.length; i++) {
			
			String word = out[i];
			
			stemmer.add(word.toCharArray(), word.length()); stemmer.stem(); 
			out[i] = stemmer.toString();	
		}
		
		return out;
	}
	
	public static void display_results(List<Double> scores, Map<Double, Set<String>> rankings, String resultsTabName, String rankingTabName, KVSClient kvs) throws IOException {
		
		// create ranking
		int rank = 1; String index = "";
		
		for (double score : scores) {
			
			// get set of URLs with the current score
			Set<String> urls = rankings.get(score);
			
			// for each URL at this score level, add url, page contents
			for (String url : urls) {
				
				System.out.println("Rank: " + rank);
				
				index = index + "|";
				
				kvs.put(resultsTabName, index, "url", url);
				kvs.put(resultsTabName, index, "pagerank", "0.0");
				kvs.put(resultsTabName, index, "pagerank", kvs.get("pageranks", url, "rank"));	
				kvs.put(resultsTabName, index, "tf_idf", kvs.get(rankingTabName, url, "tf_idf"));
				kvs.put(resultsTabName, index, "score", String.valueOf(score));
				
				rank++;
				
				if (rank > 200) {System.out.println("HERE"); return;}
			}
		}
	}
	
	public static String[] lookup(String word, KVSClient kvs) throws IOException {
		
		System.out.println("WORD: " + word);
		
		byte[] word_index_byte = kvs.get("index", word, "url");
	
		// if word isn't in the index, end execution
		if (Objects.isNull(word_index_byte)) {return null;}
		String word_index = new String(word_index_byte);

		// split info into a list of relevant documents
		String[] documents = word_index.split(delimiter2);
		
		return documents;
	}
	
	public static int getLength(String[] words, KVSClient kvs) throws IOException {
		
		Set<String> URLs = new HashSet<String>();
		
		for (String word : words) {
			byte[] result = kvs.get("index", word, "url"); if (Objects.isNull(result)) {continue;}
			String[] new_urls = new String(result).split(delimiter2);
			URLs.addAll(Arrays.asList(new_urls));
		}
		
		return URLs.size();
	}
	
	public static double getScore(double tf_idf, double pagerank) {
		return tf_idf_weight * Math.log(tf_idf) + pagerank_weight * Math.log(pagerank);
	}
	
	public static boolean url_contains(String term, String url) {
		
		String[] components = url.split("\\P{Alnum}+");
		Set<String> component_set = new HashSet<String>(); component_set.addAll(Arrays.asList(components));
		
		if (component_set.contains(term)) {return true;}
		
		return false;
	}
	
	public static void run(FlameContext ctx, String[] args) throws Exception {
		
		/*
		 * 0. INITIALIZATION
		 */

		final String KVS_address = args[0];
		
		KVSClient kvs_ = new KVSClient(args[0]);
		String query = args[1]; String[] words = split_stem(query); final int n_words = words.length;
		String rankingTabName = "tf-idf_rankings:" + query; String resultsTabName = "results:" + query;
		
		if (args.length < 2) {ctx.output("Ranker needs a search query to run!"); return;}
		ctx.output("OK");
		
		// get total number of documents in web crawl
		byte[] crawl_byte = kvs_.get("index", "_", "url"); double N = getLength(words, kvs_);
		
		/*
		 * 1. TF-IDF CALCULATION
		 */
		
		List<Integer> df = new LinkedList<Integer>(); List<String> word_list = new LinkedList<String>();
		
		StringToIterable lambda = (document -> {
			
			Set<String> dummy = new HashSet<String>();	// define a dummy output iterable
			
			KVSClient kvs = new KVSClient(KVS_address);
			String[] info = document.split(delimiter1); if (info.length < 2) {System.out.println("No ; \n" + document + "\n"); return dummy;}
			
			// each position info should be formatted as <n_positions> <pos1> <pos2> . . . <pos_n>
			String url = info[0]; String[] pos = info[1].split(" "); if (pos.length < 1) {return dummy;}
			
			try {
				double tf = (double) Integer.parseInt(pos[0]); int df_ = df.get(0); double tf_idf = (tf * Math.log(N/df_)) / n_words;
				
				String word = word_list.get(0);
				if (url_contains(word, url)) {tf += URL_bonus;}
				
				byte[] tf_idf_byte = kvs.get(rankingTabName, url, "tf_idf");
				if (Objects.isNull(tf_idf_byte)) {kvs.put(rankingTabName, url, "tf_idf", String.valueOf(tf_idf));}
				
				else {
					double tf_idf_old = Double.parseDouble(new String(tf_idf_byte));
					tf_idf = tf_idf_old + tf_idf; kvs.put(rankingTabName, url, "tf_idf", String.valueOf(tf_idf));
				}
				
				kvs.put(rankingTabName, url, "tf", String.valueOf(tf)); kvs.put(rankingTabName, url, "df", String.valueOf(df_)); kvs.put(rankingTabName, url, "N", String.valueOf(N));
				
				return dummy;
			} 
			catch (Exception e) {
				return dummy;
			}
		});
		
		for (String word : words) {
			
			String[] documents = lookup(word, kvs_);
			if (Objects.isNull(documents)) {continue;}
			
			df.add(0, documents.length); // add document frequency to a list to be used in lambda. Circumvents variable finality requirement
			word_list.add(0, word);
			
			FlameRDD doc_table = ctx.parallelize(Arrays.asList(documents)); // add tf-idf to score table
			
			doc_table.flatMap(lambda);
		}
		
		/*
		 * 2. Score Aggregation
		 */
		
		Iterator<Row> ranking_info = kvs_.scan(rankingTabName); Map<Double,Set<String>> rankings = new HashMap<Double,Set<String>>();
		
		while (ranking_info.hasNext()) {
			
			Row row = ranking_info.next(); String url = row.key(); 
			String tf_idf_str = row.get("tf_idf"); if (Objects.isNull(tf_idf_str)) {tf_idf_str = "0";}; double tf_idf = Double.parseDouble(tf_idf_str);
			byte[] pagerank_byte = kvs_.get("pageranks", url, "rank"); double pagerank = 0.1; if (!Objects.isNull(pagerank_byte)) {pagerank = Double.parseDouble(new String(pagerank_byte));}
			
			Set<String> urls = null;
			
			// Weighted sum scoring formula
			double score = getScore(tf_idf, pagerank);
			
			// if score has not been seen, let's make a new set & add
			if (!rankings.containsKey(score)) {
				urls = new HashSet<String>(); urls.add(url);
				rankings.put(score, urls);
			}
			
			// otherwise, get the existing set and add
			else {
				urls = rankings.get(score); urls.add(url);
				rankings.put(score, urls);
			}
		}
		
		// sort URLs by score
		List<Double> scores = new LinkedList<Double>(); scores.addAll(rankings.keySet());
		Collections.sort(scores, Collections.reverseOrder());
		
		display_results(scores, rankings, resultsTabName, rankingTabName, kvs_);
	}
}
