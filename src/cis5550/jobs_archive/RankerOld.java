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
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class RankerOld {
	
	static double pagerank_weight = 0.9; static double tf_idf_weight = 0.1;
	
	public static void run(FlameContext ctx, String[] args) throws Exception {
		
		/*
		 * 0. INITIALIZATION
		 */
		
		// init kvs
		KVSClient kvs_ = new KVSClient("Localhost:8000");
		
		// get search query as first arg
		String query = args[0].toLowerCase();
		
		// create search-specific table names
		String rankingTabName = "tf-idf_rankings:" + query; String resultsTabName = "results:" + query;
		
		if (args.length < 1) {ctx.output("Ranker needs a search query to run!"); return;}
		ctx.output("OK");
		
		// get total number of documents in web crawl
		byte[] crawl_byte = kvs_.get("index", "_", "url"); double N = (double) new String(crawl_byte).split(",").length; System.out.println("N: " + N);
		
		// for each word in the query, seperated by all non-alphanum characters, perform ranking
		String[] words = query.split("\\P{Alnum}+"); final double n_words = (double) words.length;
		
		/*
		 * SCORE CALCULATION
		 */
		
		List<Integer> df = new LinkedList<Integer>();
		
		StringToIterable lambda = (document -> {
			
			// define a dummy output iterable
			Set<String> dummy = new HashSet<String>();
			
			// init kvs
			KVSClient kvs = new KVSClient("Localhost:8000");
			
			// each document in indexer should be formatted as <URL>;<position info>
			String[] info = document.split(";");
			
			if (info.length < 2) {System.out.println("No ;" + document); return dummy;}
			
			// each position info should be formatted as <n_positions> <pos1> <pos2> . . . <pos_n>
			String[] pos = info[1].split(" ");
			
			if (pos.length < 1) {return dummy;}
			
			// term frequency
			double tf = (double) Integer.parseInt(pos[0]);
			
			int df_ = df.get(0);
			
			// need to store total number of documents from web crawl somewhere. Divided by N to yield average.
			double tf_idf = (tf * Math.log(N/df_)) / n_words;
			
			// from "deposit rankings"
			String url = info[0]; System.out.println("URL: |" + url + "|");
			
			// deposit tf-idf data
			
			// try to get existing tf-idf. If it doesn't exist, start a new entry.
			byte[] tf_idf_byte = kvs.get(rankingTabName, url, "tf_idf");
			if (Objects.isNull(tf_idf_byte)) {kvs.put(rankingTabName, url, "tf_idf", String.valueOf(tf_idf));}
			
			// if it does, add old + new
			else {
				double tf_idf_old = Double.parseDouble(new String(tf_idf_byte));
				tf_idf = tf_idf_old + tf_idf;
				kvs.put(rankingTabName, url, "tf_idf", String.valueOf(tf_idf));
			}
			
			// deposit pagerank, combined score (weighted sum)
			byte[] pagerank_byte = kvs.get("pageranks", url, "rank");
			kvs.put(rankingTabName, url, "pagerank", pagerank_byte);
			
			System.out.println("pagerank: " + pagerank_byte);
			
			// parse pagerank from table to float
			float pagerank = (float) 0;
			if (!Objects.isNull(pagerank_byte)) {
				pagerank = Float.parseFloat(new String(pagerank_byte));}
			
			// calculate score (final evaulation of url relevancy) as a weighted product
			double score = (pagerank_weight * pagerank + tf_idf_weight * tf_idf);
			
			// add score to the processing table
			kvs.put(rankingTabName, url, "score", "" + score);
			
			return dummy;
		});
		
		for (String word : words) {
			
			System.out.println("WORD: " + word);
			
			// get urls/word poistions from index table
			byte[] word_index_byte = kvs_.get("index", word, "url");
		
			// if word isn't in the index, end execution
			if (Objects.isNull(word_index_byte)) {continue;}
			String word_index = new String(word_index_byte);
	
			// split info into a list of relevant documents
			String[] documents = word_index.split(",");
			
			// get document frequency
			df.add(0, documents.length);
			
			// parallelize documents
			FlameRDD doc_table = ctx.parallelize(Arrays.asList(documents));
			
			// apply document transformation to docs table
			doc_table.flatMap(lambda);
		}
		
		/*
		 * 1. SORTING BY RANK (non-parallelized)
		 */
		
		// get whole score table
		Iterator<Row> ranking_info = kvs_.scan(rankingTabName);
		
		// define output map
		Map<Double,Set<String>> rankings = new HashMap<Double,Set<String>>();
		
		while (ranking_info.hasNext()) {
			
			// get next ranking
			Row row = ranking_info.next(); String url = row.key(); String score_str = row.get("score");
			
			// this shouldn't happen, but if any of the get requests returns a null value, just skip the entry to avoid errors
			if (Objects.isNull(url) || Objects.isNull(score_str)) {continue;}
			
			double score = Double.parseDouble(score_str); Set<String> urls = null;
			
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
		
		System.out.println("SCORES: " + scores.size());
		
		/*
		 * 2. OUTPUTTING TO RESULTS
		 */
		
		// create ranking
		int rank = 1;
		for (double score : scores) {
			
			// get set of URLs with the current score
			Set<String> urls = rankings.get(score);
			
			// for each URL at this score level, add url, page contents
			for (String url : urls) {
				
				System.out.println(resultsTabName + ":" + String.valueOf(rank) + ":" + url);
				
				// index w/ binary so results show up in order
				String index = "";
				for (int j = 0; j < rank; j++) {
					index = index + "|";
					if (j % 10 == 9) {
						index = index + "\n";
					}
				}
				
				kvs_.put(resultsTabName, index, "url", url);
				// kvs_.put(resultsTabName, index, "page", kvs_.get("crawl", Hasher.hash(url), "page"));
				
				// temporary: see score breakdown
				for (String metric : new String[] {"pagerank", "tf_idf", "score"}) {
					
					String val_str = "0.0";
					byte[] output = kvs_.get(rankingTabName, url, metric); 
					if (!Objects.isNull(output)) {val_str = new String(output); if (val_str.length() == 0) {val_str = "0.0";};}
					
					byte[] value = String.format("%.2f", Float.parseFloat(val_str)).getBytes();
					kvs_.put(resultsTabName, index, metric, value);
				}
				
				rank++;
				
				if (rank >= 200) {break;}
			}
		}
	}
}
