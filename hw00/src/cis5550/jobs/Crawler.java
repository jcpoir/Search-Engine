package cis5550.jobs;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.Date.*;
import java.util.HashMap;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import cis5550.tools.Hasher;

public class Crawler {
	
	public static int BASE_DELAY = 0; public static Set<String> banned_types = new HashSet<String>(); 
	
	public static String randomName() {
		
		// (1) Pick a table name (using hash)
		String tabname = ""; Random r = new Random();
		
		// generate a unique ID string of 20 ASCII characters by generating random ints and casting to char
		for (int i = 0; i < 50; i++) {
			tabname = tabname + (char) (r.nextInt(26) + 'a');
		}
		
		return tabname;
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
	
	public static String normalizeURL(String url, String[] components) {
		
		// remove pound sign and everything after it. If this eliminates full URL, don't record URL.
		url = removeDocFrag(url);
		if (Objects.isNull(url)) {return null;}

		String[] components1 = parseURL(url);

		// if there are any empty components in the url, try to fill them with default values
		for (int i = 0; i < 4; i++) {

			if (i == 2) {
				if (components1[0].equals("https")) {components1[2] = "8000";}
				else {components1[2] = "80";}
			}

			// replace null components with corresponding components from parent url
			else if (components1[i] == null) {components1[i] = components[i];}

			else if (i == 3) {

				// if we get a "..", skip it
				if (components1[i].startsWith("..")) {
					
					// get number of levels in seed url, for each ".." subtract one level from the original
					String[] seed_path = components[i].split("/"); int levels = components[i].split("/").length - 2;
					
					String new_path = components1[i];
					
					while (new_path.startsWith("..")) {

						new_path = new_path.substring(3);
						levels--;
					}
						
					components1[i] = new_path;
					
					for (levels = levels; levels > 0; levels--) {
						new_path = seed_path[levels] + "/" + new_path;
					}
					components1[i] = "/" + new_path;
				}

				// handle non-route html case (blah.html#test from handout)
				if (!components1[i].startsWith("/")) {
					
					// remove last element of path (to be replaced)
					List<String> path_elements = Arrays.asList(components[i].split("/"));

					if (path_elements.size() > 0) {

						path_elements = path_elements.subList(0, path_elements.size() - 1);

						// reform string array, then join and concat new path element
						components1[i] = String.join("/", path_elements) + "/" + components1[i];
					}
				}
			}
		}
		return compToURL(components1);	
	}
	
	public static List<String> normalizeURLs(List<String> urls, String[] components) {
		
		List<String> norm_urls = new LinkedList<String>();
		
		for (String url : urls) {
			norm_urls.add(normalizeURL(url, components));
		}
		
		return norm_urls;
	}
	
	public static String removeDocFrag(String url) {
		if (url.startsWith("#")) {return null;}
		String out = url.split("#")[0]; return out; // isolate secion of the url before pound sign
	}
	
	public static String compToURL(String[] components) {
		
		String protocol = components[0]; String hostname = components[1]; String port = components[2]; String path = components[3];
		
		if (!Objects.isNull(protocol)) {protocol = protocol + "://";} else {protocol = "";}
		if (Objects.isNull(hostname)) {hostname = "";}
		if (!Objects.isNull(port)) {port =  ":" + port;} else {port = "";}
		if (Objects.isNull(path)) {path = "";}
	
		return protocol + hostname + port + path;
	}
	
	public static List<String> head(URL url) throws IOException {

		// create new HttpURLConnection
		HttpURLConnection connect = (HttpURLConnection) url.openConnection();

		// set request method to "HEAD" & add header to identify crawler
		connect.setRequestMethod("HEAD"); connect.setRequestProperty("User-agent", "cis5550-crawler");
		connect.setInstanceFollowRedirects(false);
		
		// connect (prepare to read output)
		try {connect.connect();} catch (Exception e) {return Arrays.asList(null, null, "500", null);}
		
		// get required info from head
		List<String> out = new LinkedList<String>();
		String contentLength = "" + connect.getContentLength(); String contentType = "" + connect.getContentType(); 
		String responseCode = "" + connect.getResponseCode(); String location = connect.getHeaderField("location");
		out.add(contentLength); out.add(contentType); out.add(responseCode); out.add(location);

		// return useful headers as string list
		return out;
	}
	
	public static boolean checkRoute(String url, Set<String> forbiddenRoutes) {
		
		// isolate the route (this is all we care about)
		String[] urlComponents = parseURL(url); String route = urlComponents[3];
		
		// section route by "/" . . . we'll want to compare these with those from robots.txt
		String[] routeComponents = route.split("/"); String[] fRouteComponents = new String[1000];
		
		for (String forbiddenRoute : forbiddenRoutes) {
			
			// do same sectioning for forbidden route
			fRouteComponents = forbiddenRoute.split("/");
			
			int i = 0;
			while (i < fRouteComponents.length & i < routeComponents.length) {
				
				// if any pair of components aren't equal
				if (!fRouteComponents[i].equals(routeComponents[i]) & !"*".equals(fRouteComponents[i])) {return true;}
				i++;
			}
		}
		
		if (fRouteComponents.length > routeComponents.length) {return true;}
		else {return false;}
	}
	
	public static Set<String> robots(String url) throws IOException {
		
		if (!url.endsWith("/")) {url = url + "/";}
		url = url + "robots.txt";
		
		URL newURL = new URL(url);
		
		// create new HttpURLConnection
		HttpURLConnection connect = (HttpURLConnection) newURL.openConnection();

		// set request method to "HEAD" & add header to identify crawler
		connect.setRequestMethod("GET"); connect.setRequestProperty("User-agent", "cis5550-crawler");
		connect.setInstanceFollowRedirects(false);

		// connect (prepare to read output)
		try {connect.connect();} catch (Exception e) {return new HashSet<String>();}
		
		// out will be a list of forbidden routes
		Set<String> out = new HashSet<String>();
		
		// read data from URL via input stream reader
		InputStream is = connect.getInputStream();
		
		// convert to buffered reader (I want to be able to use readline()
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		
		// active: track whether rules apply to our crawler: if user agent is "cis5550-crawler" or "*", then it does
		String line = ""; boolean active = false; List<String> names = Arrays.asList(new String[] {"cis5550-crawler", "*"});
		while (true) {
			
			// get next line of robots.txt
			line = br.readLine();
			
			if (Objects.isNull(line)) {break;}
			
			String[] contents = line.split(":");
			
			if (contents.length >= 2) {
				String header = contents[0].strip().toLowerCase(); String body = contents[1].strip();
				
				// if rules are designated for all users or "cis5550-crawler", we need to report the rules (active = true)
				if (("user-agent".equals(header)) & (names.contains(body))) {active = true;}				
				// if we're currently active (listening for rules), add rules to the output
				if (active & ("disallow".equals(header))) {out.add(body);}
			}
		}
		
		is.close();
		
		// return useful headers as string list
		return out;
	}
	
	public static Set<String> ansTable() {
		
		Set<String> ans = new HashSet<String>();
		ans.addAll(Arrays.asList(
				"http://advanced.crawltest.cis5550.net:80/aVzUge/mbm2HwD3a7Cv.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/fY9UHdX4tiRgR9gdRt.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/aq7oJSwZ6sCi9cLYU.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ju7mo18w.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Gt5baib1We0sfA.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/zr2GLq9s.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/jbn9pM3jbFN5LEWO.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Nh1wKMFq3Bxt0xLYdP.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Op8sfA3YXnB2vZzpo.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/ND4CT6J3BPULq.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/fv5bju59zr.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Ta46MdWa0SwJMI.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/eg3FX45.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/yo1qQI6npky0JByLm.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/UN5meoou07KpkPN.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/vVJ9KpkPN4IEXzL1m.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Ox4Nec7rqw2XZEHJ.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/cY9Qz1iOfK3yxat.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/tE7ieWTn13oC.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/EM7ikR0WVP7Exs.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Ri5U63U.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Kr3Q.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/hW2bXmQ81KA.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/NS3RijOU8kZt4xhL.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/nI91hOZs9om.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/XH6HV6It6R.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Ayy3vZzpo4i7MdWa.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/YE3m8rP8GH.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Cw38mTUVM5WVP.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/jJ2zWn6AF9mbEd.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/aK9AgW7Uo9.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/CQf1fnBp0ML.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/xX9W8d6.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/zl7Dj1WG1yxat.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/yZ85xXmhq8fLYk.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/vCh9fCtXF7K4.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/WD9fOHvf2Wu9.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/xy5zJMx9GaVr5ddue.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/lnA6Qqw4C2UVJhk.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/dK2oGGl5ODiFp7xt.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Wb49N5l.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/dc3UVJhk3A6.html",
				"http://advanced.crawltest.cis5550.net:80/",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/ct7o64GFXfk.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/gv8sCi8PFi5qB.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/CW1H0D5ZhuM.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/kp76v8dkSm.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/su85TkvQH0.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/xg3s7mGGRm3bvO.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/AX7CiG5ioN4DLq.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Nb5NPT0rP2WUDW.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Nz6YyaBP3SEu2IoNM.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/GF1Scb11n.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Lg86kK6Iofp.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/QD1GyPl1AXKw0WIAaD.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/iO7a8G9LgATt.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Cf1m55yf.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/bUI9RijOU2QrO7iY.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/yZ4QSsE52.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Dd9XZEHJ3g8CKMN.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/EP5v7XHd9mfcn.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/zD34lOZ0.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/AJ1bE33yG.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/uFR4NuySF7U7.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/xL4xR1JatG0HAJD.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/iP7IgdEd78g.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ND8byQW61INH.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/GI2tvrb1IW8ClqSz.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/kH5ieWTn1Qc1on.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/eyx3up6ogArh0dcFeE.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/bh1zNkID7Gmw.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/hfW2t0f7UKBi.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/WJ8VwPy16QtfEB.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/oC2mo4bvg3.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Ym306CpNdk.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Asf2xp6kplH2.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Mm2As8hK9M.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/hfW4o2vtfU0.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Nc1ikR9pXQvm6bUI.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/iP4INZ3gK6BwgT.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/njR9fLYk6NSUp5Url.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/jl6NPl04.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/jM6iPTJ0.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/iZ4wvJ89.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/jD5ZR0R4H.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/xt2OfLLj2l2l.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Bh2bI7WG6DbPtL.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/zm71I.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/yu4qZ97a.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Ef7fLYk28jEXr.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/cw2Ptam8tUD0iPTJ.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/hp8Ro6H8Fmd.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/io9bKMVG1gcFtr4IEXzL.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/vr6SN46GSfdQ.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/jV9UybKc6JyP7PwKxs.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/NP2D1vf5ezc.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/xS8Gmw5f7N.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/oUZ2hOZs4V9TSU.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Az6Scb1c8.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/vG5KDz1HV8N.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/tH8CmeS2COCd0dBoY.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/hp3jM5Y3fY.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/AR4X6NbF4dG.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Ex5AyyZE1suDcq7kQqNG.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/aK7RPI6Nz2UKBi.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ywm6OHGoa72ZNkA.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Oy5GCnw4J1.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/oz9U6ntIh8.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Ri1suDcq8iCtOt9LZ.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/LW3viB8U3epgGc.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/wU4UybKc56Lx.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/AS9eaK0z3.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ven8Gbiq70YfX.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Fv9GFXfk8ddue9vVJZj.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/vwO3AzQCY4xLYdP.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/mD6YysHj7DbPtL9cw.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/EI6AjGP9JatG5AyyZE.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/au2EXJo3yFQ4.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/AXs4UJpiB1VEt9Im.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/ovD8iLv86kW.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/pN4NiMWg9I0.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/vr3VEt0uq4gcnoA.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/qd2jDLTN4m8v.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/dr9qZ7WSr2pM.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/UrT8C9b1.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/tc112i.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ZM9U3.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/AX3C8Ook0o.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ZV2ieWTn42LcEl.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Uf7tvrb2gK1hOZs.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ez6WorqI7Bh6hoZsa.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Cm7bI42GaVr.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/JL4Nc7aKB1LoN.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/qE5MCK9L8U.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/up4Xs5AF1tTZW.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/bI1zmeD3Dj1V.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/UVJ5W7hQrxS5Pe.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/vV5z2XHd9BwgT.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/cF9JCLL4k6.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/txm5p1lOZ9GdRBS.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/uqv6Nqwe39OgAj.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/tB1yf8C0UJpiB.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/WI3bju49I.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Ix423w.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/tK7pufMC.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/qZ41SUlC6ZNkA.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/iV7QSsE26xhd.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/wU449iw.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/pA5AkS7zWn.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/QY1WlHf6jM4JdRQ.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Yv7tA1IgdEd9UybKc.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/UJ9Nec8sCi3cYy.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/NP1U7mfcn2KSVyY.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/yf6bUI90.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/wc9U.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/mbm6iCtOt1bvO1.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/VE6H1E2h.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/HPe8aKB76LQJK.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/nt3g10SDFV.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/nHL3COCd3hQrxS2omA.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Lx6GyPl1ZMYAE3yG.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/JE1KDz1HI.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/AC87o7XKBx.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/IN44oFuln0UGRY.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Sn3Cv08na.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Uc7hOZs5W6Mm.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/yG8p.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/kW5XT8UHdX0aJoR.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Hi3AF3G4KqZP.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/LQ4Zd6C7FKLE.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/eQ1YyaBP2tQI5Qz.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/QI3wNlRn5ahj2jJ.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/lm1AjGP8G4vf.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/iLv3NSUp0bZDWZ2bE.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/db9CT7KA0Nz.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/UMG3t5uq4VWVhi.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/qK5mM4pXQvm.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/BP2XQ9XHd4N.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/hU7NY4DduKC0zvJ.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Ly7t1f4hK.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Zdn31xHdu.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/FW2mbEd87g.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/du3fnBp1EbhW1pAy.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Lx2eI3.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ej3KSVyY1dBoY3vtfU.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/nZs6gcnoA49FW.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/bZD72ny4xHdu.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/CM9vGX9bf1.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/At7Jk8xt2r.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/zT6w1mAz6Qz.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Amp68GdRBS9WMYlo.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/bKM43alHv0.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/CE6QD8yG5z.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/mr5byQW9EbhW2vxtj.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/NXJ5AHAK.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/RH2qQqw13.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/sV8C5fPTu.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/bdn3ddue9Y7Xiis.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/GQe34bKMVG3tTlW.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/suD4UHdX4suDcq6GBhrH.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/UPq2dS8mAz5xXmhq.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/YE7mbEd9f4tHLaX.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/gJL59gcnoA4a.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/mT8QrO73Bh.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/hj8EbhW1ej1.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/bfW6Gj0w8.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/bER4STxex0Xjv5qrwB.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/uy9wvJ9O6f.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Cv8A9oSU7PBy.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/tT1iw9epgGc1SN.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Xo4fU44vb.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/iw5i9vWPe0KqZP.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/gA7R9YXnB0GBzwy.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/bZD3byQW2qvKp6R.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/lO53VWVhi4tjjJI.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Of2bWUL8.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/gK8iPTJ19zSr.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/JV5GaVr3hxx6Cv.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/OuD6Nz3N4KA.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/QJ8oqw2H6T.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/ZLk5NDX6.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/mJ4lLv14.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/vnk3Ro61BYyw.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/mo93yxsz3GBhrH.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/MjR5A9Fmd9tTlW.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Qb7OfLLj2oFuln5tEu.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/xh7tHLaX5Z.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/SN3l70yFQ.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/AXs2Z0uFRs2pufMC.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/LX7sfA7zr.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/vbc4Scb0vYhkV0iY.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Nt6tEu8EXJo4b.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/JM10WMYlo2A.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/RZ4R0suxK1vf.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/fk4C7tA.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/MsT32QJprO1.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/TAy2cPwD6i.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/vo2Yvhw0Bxt4.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Sm637sbb.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Ztf9sVDPn6nrOP4YWUw.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/tHL2iU9hxx7.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Gj8N5o1jbFN.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ZCh359hMvng.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Pm1vuxZ4UHdX9qQI.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Ig25jM2Ro.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/oP1JdRQ2jTVGx4.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Tk453RijOU.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Db1i1viB5pWxqu.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/YX7EI2NX7.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Md6W8CpNdk1.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/KD5oL5uq7hyP.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Xo8fnBp1EPTad8Cr.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/vP6Qz2iCtOt0bI.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/zc2eQlLW5ioN9Lpf.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Zb730NY.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/mN8FX5mN2.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Dt17xHdu3.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/mf16LgATt.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Qc8mJ71Uo.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Cc7O0iPTJ9l.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/oG7Xiis3zmeD3.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/NhF3gdRt8E.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Eb2RHGI2OHGoa4QJprO.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/mM4sVDPn5bE7EwE.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Ak7z7iY0e.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/FTI9CmwX4HAb0aKB.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/zN7AXsTm3VEt2W.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Ro3iY4ka4eyxg.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/vo7Ptam9uIRD1v.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/HB95NtgpO1Xs.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/di27Uijd.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/RP3rq6kBzf.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/NY9D9lLv5g.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/PU2a86bzE.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/ugX4It21Fv.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Jy44nQqE.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Ro6KqZP8Y6NX.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/qv22GLY3BAcC.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Ph3wKMFq9Zd9HI.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/AHA5to3GZblN2PFi.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/na1l2Eh2zNkID.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/hI3tc2BnCGW3iLv.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/JB3oqw19kK.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/xp9U8P.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/re5CyE9Im7bOIDr.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/HG89CBzhL.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/aA9LQqh5rIdLP0.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/DC3W0vWPe1.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/kQ69HPeSU6XZWkd.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/nB9UJpiB5Wtf8tc.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/ZN11tHdfO4ZS.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/xr1N9P2.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/dl9GdRBS1jct6hZVi.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/pX5XZWkd6KpkPN5XZEHJ.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/GZb9NiMWg9o3.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Bx1t0Sdo6HQTp.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/qQ69oFcgw4PUmnH.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/If8vZzpo5O4ZUI.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/jc5dK2tvJE4FKLE.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Ig7yf7Ptam5q.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/lg7mk5rqw7Yg.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/wK6Uo7V8.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/jl2O0Vd6Yg.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/oUZ59J1bI.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/EY78hoZsa0.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Wj6ohMq99Ix.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/OH80CyE7uR.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/hp2QJprO78hz.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/rF1K7vb6OfLLj.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/IE2S0T.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/TL80r3HV.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/byy9iOfK5a.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Qc2leWib7tHLaX3ohMq.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/qU2dBoY9VWVhi5GLY.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Zhu4x3pbE.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Sj6GBzwy2Kc2NiMWg.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/SE5HI7VWVhi6iGjCy.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/yPp5Ro2N0Sdo.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/ii8pbE9Xs9dZEO.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/vu6WlHf7HBo5cU.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/hMv6Ook38xXmhq.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/tL5Uijd5rqw1xgX.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/UZ5O5Yg6kplH.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Lf1KtaDS43CWU.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/RWI879PvrsB.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/lUx7inBE0MdWa3G.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/vS94fY4JatG.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/bfW1mbEd8npky.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/lm52SEu6jfdw.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Sj2GLY4PdQ.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Eq19BwgT0.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Lo3WIAaD2Krrs2pDix.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/wd8YyaBP8mTUVM0.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/bX9HwD92AF.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/AR8XHd0BF6AJH.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/su4KqZP7zlSDj6CNkX.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/HY7xp9o7hz.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Eo4YfX8yG2mrwmb.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Rx8tTlW7OpC2BwOO.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/qB1ws0TtyB7fqZYy.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/UT5bWUL3We9GBhrH.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/SF5eI30vC.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/iY9T0hyP6qd.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Gd2ioN4Kzb7qLBzM.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/SU4LQqh4ny3Ix.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/jRO3YXnB5A5mDKi.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/jT2bI3yxat5tLBOA.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/SL6ej5WVP7nZs.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/lx79pDix5pbE.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/xk9CyE4dcFeE.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/LN8bju0.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/wd1STxex5XQ7EnBVm.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/MC8zT1cYy2suDcq.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/GHA5DkS4PhMi9Lx.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/qa6K55.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/fF3Gbiq6WMNlO3L.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/pb9epgGc7M7G.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/FKL39t5zvJ.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Nh5xp2dAJUS8dJM.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/VW63D.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/GoY5tHLaX8y2oFuln.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/YN55W0.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/bj5pWxqu9.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Gw7YmejJ1rRC6eI.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/EJ4KpkPN7d0P.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/gSO9NPT7QD3O.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/tiR9CWm3gi4.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/mV9Ook9.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Pw66IgdEd9vE.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Jn9UDh8Sdo0.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Ql7D2H2zT.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/oz7EXJo37NPT.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/YI1suxK18NROku.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/NIL9Xs90xR.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/nr9b2U5YysHj.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Wa5iPTJ71oqw.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/VX8CBzhL3dS8MdWa.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/kg3PUEsy6EAct6tsa.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/tQp9Yvhw6iOfK3lFuWA.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/TD4pM2CKMN4wvJ.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/lv63BnCGW6Lpf.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/tj7oGGl9PsUhP6ohMq.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/SvD6UQ4ReoD7Y.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/mS2fY5hQrxS8OgAj.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ONG9qQqw3Mm6.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/nI7CX0J2oqP.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/ZUI6tQp05Akk.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/JB6ogArh5vYhkV3jM.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/yQ5GSfdQ2zmeD7npky.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/oS8U4iGjCy8vuxZ.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/SNe5NX8bZDWZ7UhXdK.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/dq4z4w2.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/GBh9s69.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/sw5NSUp68mGGRm.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/BA7QSaz1alHv5yxsz.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Wx2ReoD1oqP.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/JR9hsJDK48lOZ.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Zo3on9R8.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/yM6hBn8ikR7JByLm.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/uz8pVEYz1EwE4W.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Eh90fnq8PN.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/LQ8pDix3OpC0cmeax.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/xN150Ptam.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/IH5S3rP1L.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/VL930gJL.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Lc9ikR8XZEHJ7PEuF.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/kZ8e24mbEd.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/mRO4NbF2f5As.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/qK8Gj93kBL.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/xz1y9qQI.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/PsU6jbFN9Uijd.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/zW4fLYk6Im2w.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Pv5U1KSVyY6CmwX.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/itU3CBzhL3E1EMvs.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/EA7bXmQ8JEcRg8.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/UB3e1Gbiq7hBn.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/vW2hz4E4jct.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Apv3b9HBo0PUEsy.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/qB3Qc20fU.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Mas9on0WorqI1tUD.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Lp5NDX6Cv7KXJ.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/fU9i8v2i.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Wt4epgGc7L4Z.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/pS5pWxqu6ct6cV.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/SNe24ZV5oeog.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Hr9QrO2LZ2.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/yY2oP8cV4S.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/RG4i8J0A.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/PC6XKBx6fnq.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/vx1R08LZ.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Ee55vuxZ5.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/jM2GLq7AjGP7ws.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/PX8PUEsy69.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/BF1pSZHI7w7HnBMZ.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/jM2QD1ZS4jM.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/fF5sfA8Exs4l.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/YI3tTZW0YfX3EJ.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/oF9CKY7wjGAw5Scb.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Gs8dBoY0tjjJI2.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/jbn1GBzwy5Bxal6mAz.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/MLv29oRCc2bju.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/CK3YIp8qRU2.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/dG51mk.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/En6JdRQ3w5vWPe.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/ce5sbb6XKBx4bKMVG.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/rJ9U3bXmQ1hQrxS.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/vZ7lOZ4l1gZDF.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/WU8DbPtL3As.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ea9mCQd2nQqE6mSaQt.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Li3a6l4X.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/hO2YWUw3C9.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/di6HV1ohMq7NSCk.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/mW4DduKC83.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/qB20YmejJ3qrOH.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ONG1G2HQTp9iPl.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/ta6G6LZ8Dj.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Yl6COCd0vwOfd6ON.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/hp7jDLTN5bUI9oRCc.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/hH2LZ1ahj0Ikp.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Cy2b3C0Gj.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/gl6U83vGF.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/gcn5dBoY6H1lFuWA.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/CT4Vd1o8M.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/vZ9tvrb6hoZsa2reoX.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/UZ7GaVr7XZEHJ3bzE.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/TIA5C5to4LNZ.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/wm8HI4Uo5Q.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/SX7b6Cr5a.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/qO4vf11EI.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Vd4GdRBS0DduKC9cLqy.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/vh5UTE21.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/oe7jbnI0ct8.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/uj2jbnI01f.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Ga6ws1ogilO4rhMF.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Cc59H6i.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Kt5f6QJprO0sGG.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/oC5mGGRm7vxtj7Qb.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/ZS6fqZYy9oGGl7suxK.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/EX9s94vSr.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/CW6Jn88yG.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Eh7mSaQt9Scb9.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/UDh6AF00.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/NOe8cmeax7ny4epgGc.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/gc1vf0sVDPn3.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Pw2SDFV7U1wKMFq.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/FX2fkk9UHdX9ZUI.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Wj2NDX9oP3vuxZ.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/wB1e2.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/eI3e7.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/BO6oC2Dj6INZ.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/oh2to5lFuWA.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/We5Y7mAz4W.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Sv1Qqw3e5.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/yFQ4tvJE6IgdEd6zT.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/AaD9zr76NFutM.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/UxO3Ro1gAhL8SC.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Je29s0aAcKW.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Cc3mGGRm7qQqw5.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/UJ1pbE40b.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ml75ReoD3VEt.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/hK9It2AJH8Ztx.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ok23dcFeE6hW.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/oX6yf9Cr1gAzo.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/nj25kBzf0.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/mT2R8XKBx3.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/UH5viB8aZyEc5JyP.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/mW8bI8Ptam1D.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/oq6v8rIdLP.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/oOe57E0s.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/jo8UVJhk1G5oSU.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Gt84qEo4.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Hr21HI7.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/gc6fU5ZhuM4.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/XT9mDKi0HwD5ogArh.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Er7trTR5ODiFp2ZR.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Uo1Eh4xgX7ZMYAE.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/oq7hpfx9gJL.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Bx6b5BYyw6v.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/BG5fvg4bzE0S.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/rR4N5kNHe.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/sk4tvrb5gcnoA5fPTu.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/WI7DaWbR2p3Ro.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/kU2vxtj0vGX5ny.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/WM2bOIDr5ZUI3HI.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/kI50jl0AHAK.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Cp7Ufz6ClqSz.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/yf2RPI5oL5trTR.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Cg8tTZW1wUl0s.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/kz9z45.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/jE3A9PFi4byQW.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/rh2BwOO9vVJZj8.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Nq9G66WxVb.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/RA1HAJD2FWU8mcR.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/kB3Y5.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/YF1n5xhd4CKY.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/WC6zmeD7cYy0.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/pX1Xjv6mGGRm7rq.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Io4dK9Ujvc.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Te7wUl7SFG4.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/MV4i45.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/fh7Z0CKY5iw.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/eQ3XKBx4M6HrPAf.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/GL6CNkX5CS0ugXx.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/CF8pAy3cLYU4Z.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/LE260zSr.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/ZLk8MCK3bju5WkvgW.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/UYs4H1J3kZt.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Ew6HAJD7Nqwe1.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/pD9Vd5AyyZE3wUl.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/zJ4UDh7Nz3s.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Og9w11xhL.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/rP1Ikp3mbEd7.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/kN1SN39Pq.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/tE3iLv7to9sVlKw.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/qr3ej9Ro1Nym.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/me60s7mrwmb.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/ks6HPeSU4O0qQI.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Gj6Xjv7mTUVM4Eh.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/dK1UQ3cw6UeLS.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/HGc64.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/fF76zlSDj1rFtA.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Kz51Nc5eXK.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/HK3WVP4FWU2At.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/dd4v8NX4c.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/dN1HV5vWPe9O.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Jk1i6leWib5hsJDK.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/rq7aAcKW4hBn3byQW.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/vx3A2XKBx5CX.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Gb4w0Cr6rJpLp.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Li7yxsz3ErZgX8qLBzM.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ky4tiRgR5PvrsB0.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/gs6EnBVm3UQ1U.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/CB4W7ZhuM9hpNsY.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/YO9fw0CBzhL7.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/fv7H72qsa.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/LW71NbF7C.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/PU4bKMVG07ZR.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/oJ2mSaQt5d6E.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/EV6GCnw.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/qi7VEt8ddue3ka.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Lp16inBE1vwOfd.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/aq4oqP3EXJo5td.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/jf4H02Npkec.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/vM1Nz27fvg.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/pW4DkS6qsa.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/vi2Pe7QJprO9e.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/on4s8bOIDr7iGjCy.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ikR6YE0kUOXT1W.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Wu8BwOO0yf1EAct.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/kj1D1gi6dii.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Lo7GFXfk2L0.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/qi4V4ieowf1.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/IMv9i5SXnSa2fCtXF.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Lu1l51UVJhk.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/bvO8a7oFuln5wmHLV.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/YF7ON4J4HI.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/tv8YXnB9gle4CMvYt.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/bR8zJMx4yFQ.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/HQ2YmejJ4e1au.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/kZ4fOHvf2WG6GdRBS.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/bq5UTE0jTVGx.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/JM35pufMC1SUDH.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/FvO8NFutM3oFcgw6CS.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/If4vGX4Cr8.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/iO5ieowf7qUgk2AzQCY.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/rz63KSVyY0ws.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/SO90O9PUEsy.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Lg4mbmX92JyP.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/gAh2wUl6Nz7h.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/UK9Z7Uo.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/NhF8l41x.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/hoZ8Ujvc9t6WC.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/WS4s9kUOXT8j.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/yY6l5dZEO8hMvng.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Ja5U2WMYlo.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/aJ2zr21qRU.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/EM33CT3Fv.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/EJ8iU0FKLE4bmpaY.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/zp4qQqw29HrxvL.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/kB8Uo1m7kNHe.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ha1cU7oqP1H.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/ZV5hW0bZDWZ8GFXfk.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Tk8Ll5rJpLp9d.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/tKu9oqP1EMvs0G.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/fu7Y2ugXx.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/hZ1xHdu5sVlKw3ywmuR.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/oe3HI7T4kBL.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Im8jbFN5O6ACoNJ.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ka8KXJ0iU9aAcKW.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Uy6yuV92tQp.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/ce16rq6TtfY.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/zc42jct.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Fcn8HQTp6BPULq5fOHvf.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/PK5ZUI0EXJo9ON.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/zl36Jk9rP.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/td26W1kQqNG.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/SvD30gcnoA5Z.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Uy7tHdfO66kZt.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/mkp3zmeD9CpNdk6.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Qz4XZEHJ3wvJ1Sdo.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ha5AzQCY6m9KtaDS.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Qb3pAy2w7pVEYz.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/fn1QrO4LEWO6h.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/PN38Yg1sbb.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/vQ29We6p.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/vf3gJL1H5UTE.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Mv1Gj01OpC.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Ak2v5mbEd1ahj.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/CB8WVP1GiG3.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/omA43bOIDr7tjjJI.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Ne8NSUp0bmpaY5.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ml3HQTp6AbiHP2.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/vbc998KpkPN.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/hZ75kUOXT1.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/vu9bE2KA0AjGP.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/UxO6pSZHI5cLYU2qjYtS.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/vW5hpfx8hxx6PhMi.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/jS1VWVhi1fvg3.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/al5bOIDr0VWVhi1U.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/vh18c7.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/HnB6Ikp4pWxqu4Ujvc.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/CX30.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/sw1Nc7T0.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Cm8wNlRn9CpNdk5.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/KA8qrwB3aAKED9eyxg.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/AgW8bvO9EPTad2rP.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/VI2EVa2As5NSUp.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Kt1fqZYy6E2U.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/SU1Kc94YE.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/MK2hBn0t8ZVa.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/qi6zvJ6ej1tsa.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Nu5fnBp1NX3.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/up8XKBx4Nw9.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/iG6on6f4RPq.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/bF8Ro21viB.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/NRO377NPl.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/vP8AF3pDix7CBzhL.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/NI2Ix7tvJE5Y.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/qr1Kzb7vGF4NbF.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Qz8XQ2YvPq9aYszL.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/DL7oP5PEuF0fnq.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/bmp3Url9qrwB.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/aYs6UHdX4up8EnBVm.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/rg15uFRs2Qc.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/oR7AgW3suDcq1.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/uqv2NY3epgGc3jl.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/qQ2UKBi8LEWO8IZ.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/tB2EI1ML6S.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/PK8tHLaX8sfA3mTUVM.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Bh86GyPl7.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/vPq1fvg.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/NP5Url4ODiFp6p.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/oWJ8tc62Jn.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/XK5gdRt7tQp3t.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Ro24om6yYzh.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/dA2tQI5SC4.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Asf6xHdu6dle0h.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/STx1AyyZE4o.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/GZb4mEX0cLYU3mSaQt.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/iF1Xiis9P8C.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Ga8Qc63.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/mK9b97YIp.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/iG2oJSwZ6rqw6.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/NI5dBoY8m.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ep1NuySF97t.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Pe553SwJMI.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/CS7GLq8IDLzm4oOe.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/hs14m2Jj.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/mo7tQp0LQJK.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/zv4trTR8gK6mfcn.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/itU7CT29bqaJj.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/nyn3pVEYz3DkS9Jn.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/UhX5o5tTZW.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/HIM9vxtj60IgdEd.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Ze3mAz2gcFtr.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Zt6JCLL4lmT1jDLTN.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/iw8N8xt8Nw.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/HeW3tHLaX7LNH2iU.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Xj19Ujvc9DLq.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/CS38MdWa6IW.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/SDF43ezc3JedQ.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/aYs3Uo1yeL4.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ka4i7GFXfk5.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/WU9KXJ0ACoNJ.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/pM8O1Xs.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Wk68gle0TkvQH.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/jb74Im7Ro.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/dK66vtfU3J.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/JU9RPq9tiRgR9b.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Hqk4G5DduKC9ven.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/mx1EnBVm2o0EoOV.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/ow92Npkec1tA.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Us8ej9WSr8RHGI.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/qm8As2CMvYt8FlpJ.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Wq6bvg28.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/HhG4Uijd0SC.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/zB1gcnoA78suxK.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/sL9N2kW.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/NA3Y0SvPHp0xkvA.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/cL350Wq.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/LN9MCK1WorqI.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/fw31EAct6.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Xs7HAb8Wu7IQlv.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Nk2SDFV.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/bp5o6BnCGW2WjGgx.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/lnA2Q68tvrb.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/NJ7mbEd4UJpiB5.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/jM5rhMF4uR8.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/giG6iw9DduKC8i.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Jm1CX6GCnw3NX.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/qB7Pe8AJH6.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/YR7wjGAw7v9yeL.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Sd9hz7Npkec1.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/tL2tEu0fLYk9W.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/nHL70tvJE6nrOP.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/zW86UTE0JEcRg.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/ju37Ta6o.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/ws76v3.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/EP6vf2J1gcnoA.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Bw1ws4vVJZj8mJ.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/vo4w3yG2mEX.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/bS3VwPy09.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Bw7YWUw4H6trTR.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/xA8viB9t0HAb.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Fm9vf18.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/rw7m8bju5FWU.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/vA1zvJ5aZyEc8xXUcW.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/Dt5rq4kplH7ioN.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/hMv2O3INZ2RHS.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/rP5vE4Gj3M.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/bv7AHAK9LoN2ikR.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/qj9eXK5ZNkA6pXQvm.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/EG8eplAZ2hai3.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/xa2t8Cv2U.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/zv88V4S.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/cw87WUlt8Sgza.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/kt2kz2w9dJM.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/wB4HwD8HV5gZDF.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/UQ9sGG5O4Pm.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/tm7XT3R.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/Uc3Gj58ogilO.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/EG4eXK1NSCk7iCtOt.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/FvO6hai1bvg.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/HAJ1fPTu5.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/xX2z6Jk3Cr.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/hj4EJ6pWxqu9IgdEd.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Wo6N0ogArh3sfA.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Br2GaVr7Ta0.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/HVx6Nc7rP3.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/xp64rJpLp5.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/iU2tHdfO2Qqw7.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/ie144eaK.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/XB1ddue81ujvI.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/fV1hK7RPI5FNHn.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/fO7baib1kQqNG2.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/DU5e23A.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/MjR1AXsTm93eI.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Qh6Ztf0WkvgW9zNkID.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/Da5Z0Nec3oRCc.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/tQ8A8ZR.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/lX3eplAZ1Im2mGGRm.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/SX3cmeax6Pm7.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/uq9lLv1pbE7a.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/hQ6STxex23.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/rq98AkS8C.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/uI3zT3Dj7dS.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/rq1aKB4NFutM5vGX.html",
				"http://advanced.crawltest.cis5550.net:80/cZl/gAh6wKMFq6G5NtgpO.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/ItU98vE1Z.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/gJL2AgW2UGRY0uIRD.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/mRO2tA4xLYdP0.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/mu9vWPe2NSUp3wNlRn.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/CBh70Wtx0nQqE.html",
				"http://advanced.crawltest.cis5550.net:80/aVzUge/sn90Ztx2.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/fq49KSVyY1aJoR.html",
				"http://advanced.crawltest.cis5550.net:80/c8Rhi6R/LZ5suxK8h9.html"));
		
		return ans;
	}
	
	public static Set<String> getBlackList(String[] url_arg) throws FileNotFoundException, IOException {
		
		KVSClient kvs = new KVSClient("Localhost:8000"); Set<String> blacklist_agg = new HashSet<String>();
		
		// EC2: Blackist w/ wildcards
		if (url_arg.length > 1) {
			String blacklist_tab = url_arg[1];
			Iterator<Row> blacklist_rows = kvs.scan(blacklist_tab);
			
			while (blacklist_rows.hasNext()) {
				Row row = blacklist_rows.next();
				blacklist_agg.add(row.get("pattern"));
			}
		}
		
		return blacklist_agg;
	}
	
	public static boolean checkBlackList(String url, Set<String> blacklisted) {
		
		boolean ans = false;
		
		// check each blacklisted url . . . if any one is a match, result is false
		for (String b_url : blacklisted) {
			
			if (url.equals(b_url)) {return true;}
			
			// url must contain the components in sequence to be blacklisted
			if (b_url.startsWith("*")) {b_url = "~" + b_url;}
			if (b_url.endsWith("*")) {b_url = b_url + "~";}
			
			String[] b_components = b_url.split("\\*");
			
			if (b_components.length > 0) {
			
				boolean violation = true;
				
				// check if components appear in sequence
				if (!b_components[0].equals("~")) {if (!url.startsWith(b_components[0])) {violation = false;}}
				
				int index = 0;
				for (String bc : b_components) {
					
					if (!url.substring(index).contains(bc) & !bc.equals("~")) {violation = false; break;}
					index = url.indexOf(bc) + bc.length();
				}
				
				// if the blacklisted pattern doesn't end on an asterisk and the end of the pattern isn't an exact match for the end of the url, mattern isn't a match (still)
				if (!b_components[b_components.length-1].equals("~") & !url.endsWith(b_components[b_components.length-1])) {violation = false;}
				
				if (violation) {ans = true; break;}
			}
		}
		
		return ans;
	}
	
	public static void printTable(String tabName, boolean compare) throws FileNotFoundException, IOException {
		
		System.out.println("\n== " + tabName + " ==\n");
		
		KVSClient kvs = new KVSClient("localhost:8000");
		
		Iterator<Row> rows = kvs.scan(tabName);
		
		Set<String> ansURLs = ansTable(); List<Row> extras = new LinkedList<Row>();
		
		int i = 1;
		while (rows.hasNext()) {
			Row row = rows.next();
			System.out.print("(" + i + ") " + row.key() + " ");
			for (String col : row.columns()) {
				if (!"page".equals(col)) {System.out.print(row.get(col) + " ");}
			}
			System.out.println("");
			
			if (ansURLs.contains(row.get("url"))) {ansURLs.remove(row.get("url"));}
			else {extras.add(row);};
			
			i++;
		}
		
		if (compare) {
		
			System.out.println("\nExtra entries. . .\n");
			i = 1;
			for (Row row : extras) {
				System.out.print("(" + i + ") " + row.key() + " ");
				for (String col : row.columns()) {
					if (!"page".equals(col)) {System.out.print(row.get(col) + " ");}
				}
				System.out.println("");
				i++;
			}
			
			System.out.println("\nMissing URLs . . .\n");
			i = 1;
			for (String url : ansURLs) {System.out.println("(" + i + ") " + url); i++;}
		}
	}
	
	public static void run(FlameContext ctx, String[] url_arg) throws Exception {
		
		// EC1: get blacklist, will check later
		Set<String> blacklist = getBlackList(url_arg);
		
		// System.out.println("Blacklist patterns:");
		for (String pattern: blacklist) {System.out.println(pattern);}
		
		// initialize table names. NOTE: change to "crawl"
		String tabName = "crawl"; String hostTabName = "hosts";
		
		banned_types.addAll(Arrays.asList(".jpg", ".jpeg", ".gif", ".png", ".txt"));
		
		// get root url as first passed url argument, subtracting the final slash for normalization purposes
		String rootURL_arg = url_arg[0];
		if (rootURL_arg.endsWith("/")) {rootURL_arg = rootURL_arg.substring(0,url_arg[0].length()-1);}
		
		String[] root_components = parseURL(rootURL_arg);
		final String rootURL = normalizeURL(rootURL_arg, root_components);
		
		// check if a seed URL is provided (check if there is an argument in the args array)
		List<String> URLs = Arrays.asList(rootURL_arg);
		if (URLs.size() < 1) {ctx.output("No seed URL provided!"); return;}
		ctx.output("OK");
		
		// add URLs to table
		FlameRDD urlQueue = ctx.parallelize(URLs);
		
		// define ungodly complicated lambda from step 2
		StringToIterable lambda = (url -> {
			
			KVSClient kvs = new KVSClient("Localhost:8000");
			
			// get default values from url
			String[] components = parseURL(url); String host = components[1]; String protocol = components[0];
			url = normalizeURL(url, components);
			
			// create URL object from provided url
			URL newURL = new URL(url);
			
			// create new HttpURLConnection
			HttpURLConnection connect = (HttpURLConnection) newURL.openConnection();
			
			// set request method to "GET" & add header to identify crawler
			connect.setRequestMethod("GET"); connect.setRequestProperty("User-agent", "cis5550-crawler");
			connect.setInstanceFollowRedirects(false);
			
			// aggregate a list of URLs
			Set<String> URL_set = new HashSet<String>();
			
			// check if route is forbidden by host
			byte[] result = kvs.get(hostTabName, host, "Forbidden Routes");
			
			Set<String> forbiddenRoutes = new HashSet<String>();
			
			// if robots.txt has yet to be read, read it!
			if (Objects.isNull(result)) {
				forbiddenRoutes = robots(rootURL);
				for (String route: forbiddenRoutes) {
					kvs.put(hostTabName, host, "Forbidden Routes", route + " ");
				}
			}
			
			// otherwise, grab them from the hosts table
			else {forbiddenRoutes = new HashSet<String>(Arrays.asList(new String(result).split(" ")));}
			
			// if the current route is forbidden by robots.txt, return null and move on
			if (!checkRoute(url, forbiddenRoutes)) {return new HashSet<String>();} // System.out.println("Route: " + url + " forbidden! Returning null . . ."); 
			
			// System.out.println("Route: " + url + " OK");

			// get response code from head req & check if 200 . . . only proceed if so
			List<String> info = head(newURL);
			String contentLength = info.get(0); String contentType = info.get(1); 
			int responseCode = Integer.valueOf(info.get(2)); String location = info.get(3);

			// hash url for url-based key
			String URLkey = Hasher.hash(url);

			// check last accessed time for host
			byte[] time_info = kvs.get(hostTabName, components[1], "time"); long lastAccessedTime = 0;
			if (!Objects.isNull(time_info)) {lastAccessedTime =  Long.parseLong(new String(time_info));}
			
			// intialize buffer array (used for writing body via kvs later on)
			byte[] buf = new byte[10000]; String page = "";

			// if not enough time has elapsed, re-queue the url and try again later
			if ((new java.util.Date().getTime() - lastAccessedTime) < BASE_DELAY) {URL_set.add(url); return URL_set;}

			// otherwise, get page info
			else if (responseCode == 200) {

				// prevent repeated URL visits by tracking last accessed time for hosts
				kvs.put(hostTabName, components[1], "time", ("" + new java.util.Date().getTime()).getBytes());

				// connect (prepare to read output)
				connect.connect();

				// read data from URL via input stream reader
				InputStream is = connect.getInputStream();
				
				// accumulate bytes into page
				int pos = 0; int n;

				while (true) {

					// send input stream to buffer & String agg
					n = is.read(buf, pos, 100); pos = pos + n;
					if (n == -1) {break;}
				}

				buf = Arrays.copyOfRange(buf, 0, pos + 1);

				page = new String(buf);

				// extract all URLs from page
				URL_set.addAll(normalizeURLs(getURLs(page, protocol), components));

				is.close();

			}

			// System.out.println("Response Code: " + responseCode + " URL: " + url);

			Iterator<Row> rows = kvs.scan(tabName);
			
			// prepare to track visited URLs, seen content to avoid repeats. Reverse map is for EC1 (content-seen test)
			Map<String,String> url_content = new HashMap<String,String>(); Map<String,String> content_url = new HashMap<String,String>();

			// get rows from crawl table, add to visited set, remove URLs to_visit from here
			while (rows.hasNext()) {
				Row row = rows.next();
				url_content.put(row.get("url"), row.get("page")); content_url.put(row.get("page"), row.get("url"));
			};

			// remove repeats from URL set
			URL_set.removeAll(url_content.keySet());
			
			// necessary to remove nocrawls on autograder (?) another way of doing it I guess . . . (should be redundant with earlier forbidden route detection)
			Set<String> newSet = new HashSet<String>();
			for (String url1: URL_set) {
				if (checkRoute(url1, forbiddenRoutes) & !checkBlackList(url1, blacklist)) {newSet.add(url1);}
			}
			URL_set = newSet;

			// System.out.println("unvisited URL set length: " + URL_set.size());

			// System.out.println("Visited length: " + url_content.keySet().size());

			// System.out.println("\nTo Visit:\n");
			for (String url_: URL_set) {System.out.println(url_);}
			// System.out.println("");
			
			// update crawl table
			if (!Objects.isNull(url)) {kvs.put(tabName, URLkey, "url", url.getBytes());} // if (url.contains("nocrawl")) {System.out.println("BAD URL: " + url);};}
			
			// EC1: if the page is a duplicate, add a canonical-url column
			if (!Objects.isNull(buf) & kvs.get(tabName, URLkey, "page") == null) {
				
				// if page hasn't been seen, add a page column using the buffer byte[] like normal . . .
				if (!content_url.keySet().contains(page)) {kvs.put(tabName, URLkey, "page", buf);}
				
				// otherwise, add canonicalURL w/ link to original
				else {kvs.put(tabName, URLkey, "canonicalURL", content_url.get(page));}
			}
			
			// other table updates
			if (!Objects.isNull(responseCode)) {kvs.put(tabName, URLkey, "responseCode", ("" + responseCode).getBytes());}
			if (!Objects.isNull(contentType)) {kvs.put(tabName, URLkey, "contentType", contentType.getBytes());}
			if (!Objects.isNull(contentLength)) {kvs.put(tabName, URLkey, "length", contentLength.getBytes());}
			if (!Objects.isNull(location)) {
				URL_set.addAll(normalizeURLs(Arrays.asList(location), components)); 
				// System.out.println("\nAdded URL " + location + " at URL " + url);
			}
			Thread.sleep(0);

			return URL_set;
		
		});
		
		while(urlQueue.count() != 0) {	
			// System.out.println("URL Count: " + urlQueue.count());
			urlQueue = urlQueue.flatMap(lambda);	
		}
		
		// printTable(tabName, true);
	}
}
