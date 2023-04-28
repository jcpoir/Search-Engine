package cis5550.jobs;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

public class RankerBackup {
	
	public static void run(FlameContext ctx, String[] args) throws IOException {
		
		// init kvs
		KVSClient kvs = new KVSClient("Localhost:8000");
		
		// get search query as first arg
		String query = args[0].toLowerCase();
		
		// create search-specific table names
		String rankingTabName = "tf-idf_rankings:" + query; String resultsTabName = "results:" + query;
		
		if (args.length < 1) {ctx.output("Ranker needs a search query to run!"); return;}
		ctx.output("OK");
		
		// set tf_idf, pagerank weights
		float pagerank_weight = 1; float tf_idf_weight = 1;
		if (args.length >= 2) {pagerank_weight = Float.parseFloat(args[1]);}
		if (args.length >= 3) {tf_idf_weight = Float.parseFloat(args[2]);}
		
		// get urls/word poistions from index table
		byte[] word_index_byte = kvs.get("index", query, "url");
		
		// if word isn't in the index, end execution
		if (Objects.isNull(word_index_byte)) {return;}
		String word_index = new String(word_index_byte);

		// split info into a list of relevant documents
		String[] documents = word_index.split(",");
		
		int[] test = null;
		
		// define output map
		Map<Double,Set<String>> rankings = new HashMap<Double,Set<String>>();
		
		// get document frequency
		int df = documents.length;
		
		for (String document : documents) {
			
			// each document in indexer should be formatted as <URL>;<position info>
			String[] info = document.split(";");
			
			if (info.length < 2) {System.out.println("No ;" + document); continue;}
			
			// each position info should be formatted as <n_positions> <pos1> <pos2> . . . <pos_n>
			String[] pos = info[1].split(" ");
			
			if (pos.length < 1) {continue;}
			
			// term frequency
			int tf = Integer.parseInt(pos[0]);
			
			// need to store total number of documents from web crawl somewhere
			double tf_idf = (tf * Math.log(815/df));
			
			// from "deposit rankings"
			String url = info[0];
			
			// deposit tf-idf data
			kvs.put(rankingTabName, url, "tf", String.valueOf(tf));
			kvs.put(rankingTabName, url, "df", String.valueOf(df));
			kvs.put(rankingTabName, url, "tf_idf", String.valueOf(tf_idf));
			
			// deposit pagerank, combined score (weighted sum)
			byte[] pagerank_byte = kvs.get("pageranks", url, "rank");
			kvs.put(rankingTabName, url, "pagerank", pagerank_byte);
			
			// parse pagerank from table to float
			float pagerank = (float) 0;
			if (!Objects.isNull(pagerank_byte)) {
				pagerank = Float.parseFloat(new String(pagerank_byte));}
			
			// calculate score (final evaulation of url relevancy) as a weighted product
			double score = (pagerank_weight * pagerank + tf_idf_weight * tf_idf);
			
			// add score to the processing table
			kvs.put(rankingTabName, url, "score", "" + score);
			
			// add scores to a score - set of URLs hashmap, for sorting
			if (Objects.isNull(rankings.get(score))) {Set<String> results = new HashSet<String>(); results.add(url); rankings.put(score, results);}
			else {Set<String> results = rankings.get(score); results.add(url); rankings.put(score, results);}
			
		}
		
		// sort URLs by score
		List<Double> scores = new LinkedList<Double>(); scores.addAll(rankings.keySet());
		Collections.sort(scores, Collections.reverseOrder());
		
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
				
				kvs.put(resultsTabName, index, "url", url);
				kvs.put(resultsTabName, index, "page", kvs.get("crawl", Hasher.hash(url), "page"));
				
				// temporary: see score breakdown
				for (String metric : new String[] {"pagerank", "tf_idf", "score"}) {
					kvs.put(resultsTabName, index, metric, kvs.get(rankingTabName, url, metric));
				}
				
				rank++;
			}
		}
	}
}
