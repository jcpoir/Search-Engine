package cis5550.jobs;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.Date;

import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.tools.Stemmer;

public class Search {
	
	static double pagerank_weight = 0.05; static double tf_idf_weight = 0.95; static double stopword_penalty = 0.05;
	static double URL_bonus = 0.1; static int n_results = 200;
	static Stemmer stemmer; static String delimiter1 = "\\^"; static String delimiter2 = "~"; 
	public static double N = -1; public static int df = -1; public static double n_words = -1;
	static boolean ignore_stopwords = true;

	static String host = "https://api.bing.microsoft.com/";
	static String path = "v7.0/spellcheck";
	static String key = "7b2a9676b18244959a31d036256c46e3";
	static String mkt = "en-US";
	static String mode = "proof";

	private static Map<String, Double> pageRanksHashmap = new HashMap<>();
	
	public static Set<String> stopWords = new HashSet<>(Arrays.asList(
		    "a", "an", "the", "and", "but", "or", "for", "nor", "so", "yet",
		    "at", "by", "in", "of", "on", "to", "up", "as", "it", "is",
		    "be", "am", "are", "was", "were", "been", "do", "does", "did", "has",
		    "have", "had", "can", "could", "may", "might", "must", "shall", "should", "will",
		    "would", "ought", "about", "above", "across", "after", "against", "along", "among", "around",
		    "before", "behind", "below", "beneath", "beside", "between", "beyond", "concerning", "considering", "despite",
		    "during", "except", "following", "inside", "into", "like", "near", "next", "off", "onto",
		    "outside", "over", "past", "regarding", "round", "since", "through", "throughout", "toward", "under",
		    "underneath", "unlike", "until", "upon", "with", "within", "without", "according", "alongside", "also",
		    "any", "anybody", "anyone", "anything", "both", "each", "either", "everybody", "everyone", "everything",
		    "few", "he", "her", "hers", "herself", "him", "himself", "his", "i", "it's",
		    "its", "itself", "many", "me", "mine", "more", "most", "my", "myself", "neither",
		    "no", "nobody", "none", "nothing", "one", "other", "others", "our", "ours", "ourselves",
		    "several", "she", "some", "somebody", "someone", "something", "theirs", "them", "themselves", "these",
		    "they", "this", "those", "us", "we", "what", "whatever", "which", "whichever", "who",
		    "whoever", "whom", "whomever", "whose", "you", "your", "yours", "yourself", "yourselves"
		));
	
	public static Map<String,Map<String,String>> analytics = new HashMap<String,Map<String,String>>();
	
	public static String[] split_stem(String query) {
		
		String[] out = query.toLowerCase().split("\\P{Alnum}+");
		
		for (int i = 0; i < out.length; i++) {
			
			String word = out[i];
			
			stemmer = new Stemmer();
			stemmer.add(word.toCharArray(), word.length()); stemmer.stem(); 
			
			out[i] = stemmer.toString();	
		}
		
		return out;
	}
	
	public static int getLength(String[] words, KVSClient kvs) throws IOException {
		
		Set<String> URLs = new HashSet<String>();
		
		for (String word : words) {
			byte[] result = kvs.get("index1000", word, "url"); if (Objects.isNull(result)) {continue;}
			String[] new_urls = new String(result).split(delimiter2);
			URLs.addAll(Arrays.asList(new_urls));
		}
		
		return URLs.size();
	}
	
	public static double[] calc_tf_idf_pr(String term, String document) {
		
		String[] info = document.split(delimiter1); if (info.length < 2) {return new double[] {0,0.1};}
		
		// each position info should be formatted as <n_positions> <pos1> <pos2> . . . <pos_n>
		String url = info[0]; String[] pos = info[1].split(" "); if (pos.length < 1) {return 0;}

		pageRanksHashmap.put(url, Double.parseDouble(pos[0]));
		
		double tf = (double) Integer.parseInt(pos[1]); if (url_contains(term, url)) {tf += URL_bonus;}
		
		double tf_idf = (Math.max(tf,25)  * Math.log(N/5*df)) / n_words;
		
		double pagerank = Double.parseDouble(pos[0]); 
		
		Map<String,String> url_data = new HashMap<String,String>();
		url_data.put("tf", String.valueOf(tf)); url_data.put("df", String.valueOf(df)); 
		analytics.put(url, url_data);
		
		return new double[] {tf_idf, pagerank};
	}
	
	public static double getScore(double tf_idf, double pagerank) {
		
		return (tf_idf_weight * Math.log(tf_idf)) + (pagerank_weight * Math.log(pagerank));
	}
	
	public static List<Double> sortTopN(List<Double> input, int n) {
		
		List<Double> output = new LinkedList<Double>(); int size = 1;

		Queue<Double> maxHeap = new PriorityQueue<Double>(Collections.reverseOrder());
		
		maxHeap.addAll(input);
		
		n = Math.min(input.size(), n);
		
		for (int i = 0; i < n; i++) {
			output.add(maxHeap.poll());
		}
		
		return output;
	}
	
	public static List<String> poll_results(List<Double> ranking, Map<Double,Set<String>> scores, int n) {
		
		List<String> results = new LinkedList<String>(); int i = 0;
		
		for (double score : ranking) {
			Set<String> url_set = scores.get(score);
			
			for (String url : url_set) {
				results.add(url); i++;
				if (i >= n) {return results;}
			}
		}
		
		return results;
	}
	
	public static boolean url_contains(String term, String url) {
		
		String[] components = url.split("\\P{Alnum}+");
		Set<String> component_set = new HashSet<String>();
		
		for (String component : components) {
			
			stemmer = new Stemmer(); stemmer.add(component.toCharArray(), component.length()); stemmer.stem();
			component_set.add(stemmer.toString());
		}
		
		if (component_set.contains(term)) {return true;}
		
		return false;
	}
	
	public static boolean is_stopword(String word) {
		if (stopWords.contains(word)) {return true;}
		return false;
	}
	
	public static boolean all_stopwords(String[] words) {
		
		for (String word : words) {if (!is_stopword(word)) {return false;}}
		return true;
	}
	
	public static double get_word_n(String[] words) {
		
		double n = 0;
		
		for (String word : words) {
			if (is_stopword(word)) {
				if (!ignore_stopwords) {
					n = n + stopword_penalty;
				}
			}
			else {n++;}
		}
		
		return n;
	}
	
	public static void run(FlameContext ctx, String[] args) throws IOException {
		
		long startTime = new Date().getTime();
		final String KVS_address = args[0];
		KVSClient kvs = new KVSClient(KVS_address);

		Server.get("/search", (req, res) -> {
			long startstime = System.currentTimeMillis();
			res.header("Access-Control-Allow-Origin", "*");
			res.header("Access-Control-Allow-Credentials", "true");

			String query = URLDecoder.decode(req.queryParams("query"), "UTF-8");
			String[] words = split_stem(query);
			long spellcheckTime = System.currentTimeMillis();
			String[] spellCheck = spellCheck(words);
			System.out.println("SPELLCHECK TIME: " + (System.currentTimeMillis() - spellcheckTime));

			if (all_stopwords(spellCheck)) {
				System.out.println("All stopwords!");
				ignore_stopwords = false;
			}

			n_words = get_word_n(spellCheck);

			long counttime = System.currentTimeMillis();
			N = kvs.count("hosts");
			System.out.println("COUNT TIME: " + (System.currentTimeMillis() - counttime));

			/*
			 * 1. Building tf-idf map
			 */

			long time1 = System.currentTimeMillis();
			Map<String, Double> tf_idfs = new HashMap<String, Double>();
			System.out.println("TIME 1: " + (System.currentTimeMillis() - time1));

			for (String word : spellCheck) {

				boolean stopword = is_stopword(word);

				if (ignore_stopwords & stopword) {
					continue;
				}

				byte[] documents_byte = kvs.get("index1000", word, "url");
				if (Objects.isNull(documents_byte)) {
					continue;
				}

				String documents_str = new String(documents_byte);
				String[] documents = documents_str.split(delimiter2);

				for (String document : documents) {

					df = documents.length;

					double tf_idf = calc_tf_idf(word, document);

					if (stopword) {
						tf_idf = tf_idf * stopword_penalty;
					}

					String[] info = document.split(delimiter1);
					if (info.length < 2) {
						continue;
					}
					String url = info[0];

					if (tf_idfs.containsKey(url)) {
						tf_idfs.put(url, tf_idfs.get(url) + tf_idf);
					}

					else {
						tf_idfs.put(url, tf_idf);
					}
				}
			}

			/*
			 * 2. Adding pageranks via weighted sum of logs
			 */

			long time2 = System.currentTimeMillis();
			Map<Double, Set<String>> scores = new HashMap<Double, Set<String>>();

			for (Entry<String, Double> entry : tf_idfs.entrySet()) {

				try {
					String url = entry.getKey();
					double tf_idf = entry.getValue();

					double pagerank = pageRanksHashmap.get(url);
					double score = getScore(tf_idf, pagerank);

					try {
						Map<String, String> url_data = analytics.get(url);
						url_data.put("pagerank", String.valueOf(pagerank));
						url_data.put("score", String.valueOf(score));
						url_data.put("tf_idf", String.valueOf(tf_idf));
						analytics.put(url, url_data);
					} catch (Exception e) {
					}

					if (scores.containsKey(score)) {
						Set<String> urls = scores.get(score);
						urls.add(url);
						scores.put(score, urls);
					}

					else {
						Set<String> urls = new HashSet<String>();
						urls.add(url);
						scores.put(score, urls);
					}
				} catch (Exception e) {
					continue;
				}

			}
			System.out.println("TIME 2: " + (System.currentTimeMillis() - time2));

			/*
			 * 3. Sort results & report
			 */

			List<Double> ranking = new LinkedList<Double>();
			ranking.addAll(scores.keySet());

			long sortTime = System.currentTimeMillis();
			ranking = sortTopN(ranking, n_results);
			System.out.println(ranking);
			System.out.println("Sort Time: " + (System.currentTimeMillis() - sortTime));

			List<String> results = poll_results(ranking, scores, n_results);

			List<Map<String, String>> urlAndPage = new LinkedList<>();

			for (String url : results) {
				Map<String, String> map = new HashMap<>();
				map.put("domain", URLParser.parseURL(url)[1]);
				map.put("url", url);
				byte[] htmlContentsBytes = kvs.get("crawl", Hasher.hash(url), "page");
				String htmlString = new String(htmlContentsBytes);
				map.put("title", getPageTitle(htmlString));
				urlAndPage.add(map);
			}

			Map<String, Object> json = new HashMap<>();
			if (!Arrays.equals(spellCheck, words)) {
				json.put("spellcheck", spellCheck);
			}
			json.put("results", urlAndPage);
			System.out.println("TOTAL TIME: " + (System.currentTimeMillis() - startstime));
			return convertMapToJson(json);
		});
	}
}
