package cis5550.jobs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.util.Iterator;

import java.io.*;
import java.util.Scanner;
import java.lang.Thread;
import java.util.Set;
import java.util.LinkedHashSet;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;
import cis5550.webserver.Server;
import cis5550.tools.EnglishFilter;

public class Crawler {
	private static final Logger logger = Logger.getLogger(Server.class);
	public static String normalize(String s, String base) {
		String orig = s;
		boolean secure = s.length() >= 8 && s.substring(0, 8).equals("https://") || base.length() >= 8 && base.substring(0, 8).equals("https://");
		/* Strip # */
		int index = s.indexOf('#');
		if (index != -1) {
			s = s.substring(0, index);
		}
		if (s.length() < 2) {
			return null;
		}

		/* Strip everything after ? */
		index = s.indexOf('?');
		if (index != -1) {
			s = s.substring(0, index);
		}

		if (s.length() > 2048) {
			logger.debug("=== Ignoring URL of length " + s.length());
			return null; /* Probably not legit */
		}

		if (s.contains(" ")) {
			/* Links can't contain spaces, silly */
			logger.debug("=== Bad URL: " + s); /* Bad URL, or more likely a bug in parsing it */
			return null;
		}
		if (s.startsWith("mailto:")) {
			return null; /* No mail links */
		}
		/* Relative */
		boolean relative = false;
		index = base.lastIndexOf('/');
		if (index != -1) {
			base = base.substring(0, index);
			//logger.debug("Base URL truncated to " + base);
		}
		while (s.length() > 2 && s.substring(0, 2).equals("..")) {
			relative = true;
			index = base.lastIndexOf('/');
			if (index != -1) {
				base = base.substring(0, index);
				//logger.debug("Base URL truncated to " + base);
			}
			s = s.substring(3);
		}
		if (relative) {
			s = base + s;
		}
		/* returns protocol, hostname, port, resource */
		String[] up = URLParser.parseURL(s);
		String[] up2 = URLParser.parseURL(base);
		/* If relative, prepend base domain */
		if (up[1] == null) {
			up[1] = up2[1];
		}
		/* Use same protocol */
		if (up[0] == null) {
			up[0] = up2[0];
		}
		/* Port */
		if (up[2] == null) {
			up[2] = secure ? "443" : "80";
		}
		if (up[3].length() > 0 && up[3].charAt(0) != '/') {
			/* was turning into e.g. http://info.cern.ch:80Technical.html
			 * Need a slash after port */
			up[3] = "/" + up[3];
		}
		s = up[0] + "://" + up[1] + ":" + up[2] + up[3];
		logger.debug("Normalized " + orig + " to " + s);
		return s;
	}
	public static boolean crawlable(String link) {
		int index;
		if (link != null) {
			if (link.substring(0, 5).equals("http:") || link.substring(0, 6).equals("https:")) {
				index = link.lastIndexOf('.');
				if (index != -1) {
					String extension = link.substring(index).toLowerCase();
					if (extension.equals("jpg") || extension.equals("jpeg") || extension.equals("gif") || extension.equals("png") || extension.equals("text")) {
						logger.debug("Discarding URL " + link);
						return false;
					}
				}
			}
		}
		return true;
	}
	public static void extract(List<String> l, String s, String base) {
		int linksFound = 0;
		for (;;) {
			int index = s.indexOf("href=");
			int index2 = s.indexOf("HREF=");
			if (index == -1 && index2 == -1) {
				break;
			}
			if (index2 == -1) {
				;
			} else if (index == -1) {
				index = index2;
			} else { /* Both */
				index = Integer.min(index, index2);
			}
			index += 5;
			s = s.substring(index);
			//logger.debug("Remaining: " + s);
			if (s.charAt(0) == '"') {
				/* Look for " to end */
				index = s.substring(1).indexOf('"');
			} else {
				/* Look for ' to end */
				index = s.substring(1).indexOf("'");
			}
			if (index == -1) {
				logger.warn("Unterminated open quote for hyperlink");
				break;
			}
			String link = s.substring(1, index + 1); /* Add 1 since the substring search started at 1, not 0 */
			linksFound++;
			s = s.substring(index + 1);
			link = normalize(link, base);
			//logger.debug("Link: " + link);
			if (link != null && crawlable(link)) {
				if (!link.startsWith("http") && !link.startsWith("https")) { /* Ignore other protocols */
					continue;
				}
				if (link.contains("..")) {
					continue; /* ERROR Link was not normalized properly! */
				}
				l.add(link);
			}
		}
		logger.debug(linksFound + " links found");
	}
	public static boolean parseRobots(String robots, String url) {
		Scanner scanner = new Scanner(robots);
		boolean inMatch = false;
		int isAllowed = 0, isDisallowed = 0;
		int sawRules = 0;
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			if (line.length() < 1) {
				continue;
			}
			if (line.startsWith("User-agent:")) {
				if (sawRules > 0 && inMatch) {
					break; /* Finished all rules for this agent */
				}
				sawRules = 0;
				if (!inMatch) {
					line = line.substring(11);
					if (line.charAt(0) == ' ') {
						line = line.substring(1);
					}
					if (line.equals("*") || line.equals("cis5550-crawler")) {
						inMatch = true;
					}
				}
			}
			if (!inMatch) {
				continue;
			}
			if (line.startsWith("Allow:")) {
				sawRules++;
				line = line.substring(6);
				if (line.charAt(0) == ' ') {
					line = line.substring(1);
				}
				if (url.startsWith(line)) {
					isAllowed = line.length();
				}
			} else if (line.startsWith("Disallow:")) {
				sawRules++;
				line = line.substring(9);
				if (line.charAt(0) == ' ') {
					line = line.substring(1);
				}
				if (url.startsWith(line)) {
					isDisallowed = line.length();
				}
				if (url.contains(line)) {
					scanner.close();
					return false;
				}
			}
		}
		scanner.close();
		logger.debug("isDisallowed:" + isDisallowed + ",isDisallowed:" + isDisallowed);
		boolean ret = isDisallowed == 0 || isAllowed > isDisallowed;
		return ret;
	}
	public static String exceptionString(Exception e) {
		StringWriter errors = new StringWriter();
		e.printStackTrace(new PrintWriter(errors));
		return errors.toString();
	}
	private static final int maxCrawls = 500000;
	private static final int minCrawlInterval = 950; /* ms */
	private static final int maxQueueSize = 90000;
	private static FlameRDD urlQueue = null;
	private static boolean done = false;
	public static void run(FlameContext ctx, String[] strings) {
		int index, crawlround = 0;

		if (strings.length != 1) {
			ctx.output("Must provide exactly one seed");
			return;
		}
		ctx.output("OK");
		List<String> sl = new ArrayList<String>();

		/* Normalize seed URL */
		String seed = strings[0];
		/* Strip # */
		index = seed.indexOf('#');
		if (index != -1) {
			seed = seed.substring(0, index);
		}
		if (seed.length() < 2) {
			logger.warn("Invalid seed");
			return;
		}
		String[] up = URLParser.parseURL(seed);
		if (up[2] == null) {
			up[2] = up[0].equals("https") ? "443" : "80";
		}
		seed = up[0] + "://" + up[1] + ":" + up[2] + up[3];

		sl.add(seed);
		try {
			KVSClient kvsclientOuter = new KVSClient(ctx.getKVS().getMaster());
			kvsclientOuter.persist("crawl");
			kvsclientOuter.persist("hosts"); /* XXX Doesn't really *need* to be persistent... */
			//kvsclientOuter.persist("queue");
			/* Queue is persistent, load anything still in the queue when we start back up */
			int oldsize = 0, oldqueue = 0, oldqueue2 = 0;

			oldqueue = 0;
			String line = null;
			File file = new File("queuefile.txt"); /* periodic permanent backup */
			File file2 = new File("queuefile2.txt"); /* active queue backup */
			try {
				Scanner scanner = new Scanner(file);
				while (scanner.hasNextLine()) {
					line = scanner.nextLine();
					sl.add(line);
					oldqueue++;                    
				}
				scanner.close();
				
				Scanner scanner2 = new Scanner(file2);
				while (scanner2.hasNextLine()) {
					line = scanner2.nextLine();
					sl.add(line);
					oldqueue2++;                    
				}
				scanner2.close();
				
				/* Eliminate duplicates */
				Set<String> urlset = new LinkedHashSet<>(sl);

				/* Need to remove any URLs that are already crawled */
				int removed = 0;
				Iterator<Row> i = kvsclientOuter.scan("crawl", null, null);
				while (i.hasNext()) {
					Row r = i.next();
					for (String col : r.columns()) {
						if (col.equals("url")) {
							/* Already crawled */
							if (urlset.contains(r.get(col))) {
								urlset.remove(r.get(col));
								removed++;
							}
						}
					}
				}

				sl.clear();
				sl.addAll(urlset);
				oldsize = sl.size();

				/* Remove anything that's not English */
				sl = EnglishFilter.filter(sl);

				/* Rewrite the file with all duplicates eliminated, from queuefile.txt and queuefile2.txt
				 * otherwise we'll just have these be stuck here until we finish the next round and do a complete rewrite of the queue file
				 * (and queuefile2.txt is never truncated/rewritten anywhere else) */
				int realsize = 0;
				/* XXX For some reason this never seems to happen? */
				FileWriter fw = new FileWriter("queuefile.txt", false);
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw);
				for (String url2 : sl) {
					out.println(url2);
					realsize++;
				}
				out.close();
				fw.close();
				logger.debug("=== Actual queue size " + realsize + "  - already crawled " + removed);
				/* Truncate queuefile2.txt */
				FileWriter fw2 = new FileWriter("queuefile2.txt", false);
				fw.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}           

			logger.debug("==== Added from old queue: " + oldqueue + " + " + oldqueue2);
			urlQueue = ctx.parallelize(sl);

			if (false) {
				Thread runthread = new Thread(new Runnable() {
					public void run() {
						for (;;) {
							try {
								Thread.sleep(2000);
							} catch (Exception e) {
								e.printStackTrace();
							}
							try {
								Thread.sleep(600000); /* Every 10 minutes */
								if (done) {
									return;
								}
								/*
								urlQueue.saveAsTable("queue");
								Iterator<Row> i = kvsclientOuter.scan("queue", null, null);
								while (i.hasNext()) {
									Row r = i.next();
									for (String col : r.columns()) {
										oldsize++;
										String val = r.get(col);
										logger.debug("==== " + col + "/" + val);
										out.println(val);
									}
								}
								*/
								List<String> queueurls = urlQueue.collect();
								int oldsize = 0;

								FileWriter fw = new FileWriter("queuefile.txt", false);
								BufferedWriter bw = new BufferedWriter(fw);
								PrintWriter out = new PrintWriter(bw);
								for (String url2 : queueurls) {
									out.println(url2);
									oldsize++;
								}
								out.close();
								fw.close();
								logger.debug("=== Backup queue size " + oldsize);
							} catch (Exception e) {
								e.printStackTrace();
								logger.error("==== Backup Exception: " + exceptionString(e));
							}
						}
					}});
				runthread.start();
			}

			while (urlQueue.count() > 0) {
				crawlround++;
				int crawlCount = kvsclientOuter.count("crawl");
				if (crawlCount > maxCrawls) {
					logger.debug("==== Reached max crawls allowed: " + maxCrawls);
					break;
				}
				final int currentQueueSize = urlQueue.count();
				logger.debug("========== Round " + crawlround + ": Queue contains " + currentQueueSize + " URLs, " + crawlCount + " already finished");
				String kvsMaster = ctx.getKVS().getMaster();

				StringToIterable l =  (crawlUrl) -> {
					String hash = Hasher.hash(crawlUrl);
					KVSClient kvsclient2 = new KVSClient(kvsMaster);
					Row r = kvsclient2.getRow("crawl", hash);
					List<String> newlinks = new ArrayList<String>();
					if (r != null) {
						//logger.debug("*** Skipping repeat crawl of " + crawlUrl);
						return newlinks;
					}
					int responseCode;
					String inputLine;
					String contentType = null, contentLength = null;

					String keyHash = Hasher.hash(crawlUrl);
					KVSClient kvsclient = new KVSClient(kvsMaster);

					String[] up3 = URLParser.parseURL(crawlUrl);

					long unixTime = System.currentTimeMillis(); /* epoch but in ms */
					boolean firstHost = false;
					if (up3[1] == null) {
						logger.error("Host was NULL?");
						/* Avoid null pointer exception */
						return newlinks;
					}
					byte[] tmp = kvsclient.get("hosts", up3[1], "timestamp");
					if (tmp != null) {
						String tmps = new String(tmp);
						//logger.debug("String: " + tmps);
						long t = Long.parseLong(tmps);
						if (unixTime < t + minCrawlInterval) {
							newlinks.add(crawlUrl); /* Crawl later */
							//logger.debug("Will need to delay crawl of " + crawlUrl);
							return newlinks;
						}
					} else {
						firstHost = true;
					}

					kvsclient.put("hosts", up3[1], "timestamp", Long.toString(unixTime));

					if (true) {
						/* Keep track of # pages crawled on this host.
						 * Since there could be multiple workers, this is not necessarily atomic,
						 * but that's fine if this is approximate.
						 * Main purpose is to bail out if this gets too large. */
						tmp = kvsclient.get("hosts", up3[1], "pagecount");
						if (tmp != null) {
							String tmps = new String(tmp);
							int pagecount = Integer.parseInt(tmps);
							pagecount++;
							kvsclient.put("hosts", up3[1], "pagecount", Integer.toString(pagecount));
							if (pagecount > 10000) { /* XXX What about big sites, e.g. Wikipedia? */
								logger.debug("==== Reached max crawls for host " + up3[1]);
								return newlinks; /* Return the empty list to avoid crawling further links on this page */
							}
						} else {
							kvsclient.put("hosts", up3[1], "pagecount", "1");
						}
					}

					///* Shouldn't be a race condition since each URL only goes out to one worker */
					//kvsclient.put("queue", "url", crawlUrl, ""); /* "Delete" from queue */

					try {
						URL url;
						String robots = null;
						if (firstHost) {
							url = new URL(up3[0] + "://" + up3[1] + "/robots.txt");
							HttpURLConnection con = (HttpURLConnection) url.openConnection();
							con.setRequestMethod("GET");
							con.setConnectTimeout(10000); /* 10 second timeout */
							con.setRequestProperty("User-Agent", "cis5550-crawler");
							con.connect();
							responseCode = con.getResponseCode();
							if (responseCode == 200) {
								InputStreamReader isr = new InputStreamReader(con.getInputStream());
								BufferedReader in = new BufferedReader(isr);
								StringBuffer response = new StringBuffer();
								while ((inputLine = in.readLine()) != null) {
									response.append(inputLine);
								}
								robots = response.toString();
								kvsclient.put("hosts",  up3[1], "robots", robots);
							}
						} else {
							tmp = kvsclient.get("hosts", up3[1], "robots");
							if (tmp != null) {
								robots = new String(tmp);
							}
						}
						if (robots != null && robots.length() > 0) {
							boolean allowed = parseRobots(robots, up3[3]);
							if (!allowed) {
								logger.debug("===== DISALLOWED by robots.txt: " + crawlUrl);
								return newlinks;
							}
						}

						url = new URL(crawlUrl);
						HttpURLConnection con = (HttpURLConnection) url.openConnection();
						con.setRequestMethod("HEAD");
						con.setConnectTimeout(10000); /* 10 second timeout */
						con.setRequestProperty("User-Agent", "cis5550-crawler");
						con.connect();
						responseCode = con.getResponseCode();
						int headResponse = responseCode;
						if (responseCode > 0) {
							boolean isRedirect = responseCode == 301 || responseCode == 302 || responseCode == 303 || responseCode == 307 || responseCode == 308;
							contentLength = con.getHeaderField("Content-Length");
							contentType = con.getHeaderField("Content-Type");
							String location = con.getHeaderField("Location");
							if (isRedirect && location != null) {
								logger.debug("==== Redirect: " + location);
								String link = normalize(location, crawlUrl);
								//logger.debug("Link: " + link);
								if (link != null && crawlable(link)) {
									newlinks.add(link); /// TODO: should this be newlinks, not sl? Okay, changed
								}
							}
							//logger.debug("Done parsing headers");
						} else {
							//logger.debug("No headers");
						}

						kvsclient.put("crawl", keyHash, "responseCode", Integer.toString(responseCode));
						kvsclient.put("crawl", keyHash, "url", crawlUrl);
						if (contentLength != null) {
							kvsclient.put("crawl", keyHash, "length", contentLength);
						}
						if (contentType != null) {
							kvsclient.put("crawl", keyHash, "contentType", contentType);
						}
						if (responseCode != 200) {
							logger.debug("==== Not crawling HTTP code " + responseCode + ": " + crawlUrl);
							return newlinks;
						}
						/* May contain more things like charset too */
						if (contentType != null && !contentType.contains("text/html")) {
							logger.debug("==== Not crawling non HTML " + crawlUrl + ": " + contentType);
							return newlinks;
						}
						con.disconnect();

						con = (HttpURLConnection) url.openConnection();
						con.setRequestMethod("GET");
						con.setConnectTimeout(10000); /* 10 second timeout */
						con.setRequestProperty("User-Agent", "cis5550-crawler");
						con.connect();
						responseCode = con.getResponseCode();
						if (headResponse == 200) {
							kvsclient.put("crawl", keyHash, "responseCode", Integer.toString(responseCode));
						}
						if (responseCode != 200) {
							logger.debug("== Not storing non 200 " + crawlUrl);
							return newlinks;
						}
						InputStreamReader isr = new InputStreamReader(con.getInputStream());
						BufferedReader in = new BufferedReader(isr);
						StringBuffer response = new StringBuffer();

						while ((inputLine = in.readLine()) != null) {
							response.append(inputLine);
							response.append("\n");
						}
						con.disconnect();
						in.close();
						isr.close();
						kvsclient.put("crawl", keyHash, "page", response.toString());

						if (currentQueueSize > maxQueueSize) {
							logger.debug("=== Crawled only (@" + currentQueueSize + ") " + crawlUrl + " (links found: " + newlinks.size() + "), already at " + currentQueueSize);
							return newlinks; /* Prevent queue from growing too large to avoid running out of memory */
						}

						extract(newlinks, response.toString(), crawlUrl);
						logger.debug("=== Crawled (@" + currentQueueSize + ") " + crawlUrl + " (links found: " + newlinks.size() + ")");
					} catch (Exception e) {
						e.printStackTrace();
						logger.debug("==== Crawl exception occured in crawler (" + crawlUrl + "): " + exceptionString(e));
					}

					/* Eliminate duplicates */
					Set<String> urlset = new LinkedHashSet<>(newlinks);
					newlinks.clear();
					newlinks.addAll(urlset);

					/* Remove anything that's not English */
					newlinks = EnglishFilter.filter(newlinks);

					/* Append since FlameRDD doesn't update */
					FileWriter fw = new FileWriter("queuefile2.txt", true); /* DO append here */
					BufferedWriter bw = new BufferedWriter(fw);
					PrintWriter out = new PrintWriter(bw);
					for (String url2 : newlinks) {
						out.println(url2);
					}
					out.close();
					fw.close();

					return newlinks;
				};
				try {
					urlQueue = urlQueue.flatMap(l);
					if (urlQueue == null) {
						logger.debug("=== Queue is empty after " + crawlround + " rounds");
						break;
					}
				} catch (Exception e) {
					logger.debug("==== Flatmap exception occured: " + exceptionString(e));
					e.printStackTrace();
					break;
				}
				try {
					/*
					urlQueue.saveAsTable("queue");
					Iterator<Row> i = kvsclientOuter.scan("queue", null, null);
					while (i.hasNext()) {
						Row r = i.next();
						for (String col : r.columns()) {
							oldsize++;
							String val = r.get(col);
							logger.debug("==== " + col + "/" + val);
							out.println(val);
						}
					}
					*/
					List<String> queueurls = urlQueue.collect();
					oldsize = 0;

					FileWriter fw = new FileWriter("queuefile.txt", false);
					BufferedWriter bw = new BufferedWriter(fw);
					PrintWriter out = new PrintWriter(bw);
					for (String url2 : queueurls) {
						out.println(url2);
						oldsize++;
					}
					out.close();
					fw.close();
					logger.debug("=== Backup queue size " + oldsize);
				} catch (Exception e) {
					e.printStackTrace();
					logger.error("==== Backup Exception: " + exceptionString(e));
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.debug("==== Fatal exception occured in crawler" + exceptionString(e));
		}
		logger.debug("==== Crawler finally exiting after " + crawlround + " rounds (queue empty)");
		done = true;
	}
}
