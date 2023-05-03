package cis5550.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import cis5550.tools.URLParser;
import cis5550.flame.FlameContext;
import cis5550.flame.FlameContextImpl;
import cis5550.kvs.KVSClient;
import static cis5550.jobs.Crawler.*;

public class NormalizationTest {
	
	public static String compToURL(String[] components) {
		
		String protocol = components[0]; String hostname = components[1]; String port = components[2]; String path = components[3];
		
		if (!Objects.isNull(protocol)) {protocol = protocol + "://";} else {protocol = "";}
		if (Objects.isNull(hostname)) {hostname = "";}
		if (!Objects.isNull(port)) {port =  ":" + port;} else {port = "";}
		if (Objects.isNull(path)) {path = "";}
	
		return protocol + hostname + port + path;
	}
	
	public static String removeDocFrag(String url) {
		if (url.startsWith("#")) {return null;}
		String out = url.split("#")[0]; return out; // isolate secion of the url before pound sign
	}
	
	public static void check(String actual, String expected) {
		if (expected.equals(actual)) {System.out.println("[✓] (actual) " + actual + " == " + expected + " (expected)");}
		else {System.out.println("\n[✗] (actual) " + actual + " !=\n  (expected) " + expected);}
	}
	
	public static void check(boolean actual, boolean expected) {
		if (expected == actual) {System.out.println("[✓] (actual) " + actual + " == " + expected + " (expected)");}
		else {System.out.println("\n[✗] (actual) " + actual + " !=\n  (expected) " + expected);}
	}
	
	public static List<Set<String>> interpret_robots() throws IOException {
		
		List<Set<String>> out = new LinkedList<Set<String>>(); Set<String> allowed = new HashSet<String>(); Set<String> disallowed = new HashSet<String>();
		
		BufferedReader br = new BufferedReader(new FileReader(new File("robots.txt")));
		
		// active: track whether rules apply to our crawler: if user agent is "cis5550-crawler" or "*", then it does
		String line = ""; boolean active = false; List<String> names = Arrays.asList(new String[] {"cis5550-crawler", "*"});
		while (true) {
			
			// get next line of robots.txt
			line = br.readLine();
			
			if (Objects.isNull(line)) {break;}
			
			String[] contents = line.split(":");
			
			if (contents.length >= 2) {
				
				String header = contents[0].strip().toLowerCase(); String body = contents[1].strip().toLowerCase();
				
				// if rules are designated for all users or "cis5550-crawler", we need to report the rules (active = true)
				if (("user-agent".equals(header)) & (names.contains(body))) {active = true;}	
				
				// otherwise, disregard
				else if ("user-agent".equals(header)) {active = false;}
				
				// if we're currently active (listening for rules), add rules to the output
				
				if (active) {
					
					if ("disallow".equals(header)) {disallowed.add(body);}
					
					else if ("allow".equals(header)) {allowed.add(body);}
					
					else if ("crawl-delay".equals(header)) {System.out.println("Crawl Delay: " + body);}
				}
			}
		}
		
		out.add(disallowed); out.add(allowed);
		return out;
	}
	
	public static String normalize(String base_url, String link) {
		/*
		 * Add your normalization code to complete this function, then run tests
		 */
		
		String[] components = parseURL(base_url); String res_url = normalizeURL(link, components);
		return res_url;
	}
	
	public static void main(String[] args) throws Exception {
		
		System.out.println("\n== NORMALIZATION TESTS ==\n");
		
		System.out.println("PUBLIC\n");
		
		// public1 ~ Instructions say: "start by cutting off the part after the #; if the URL is now empty, discard it".
		// That direction conflicts directly with the expected behavior of this test case
		
		System.out.println("Expected behavior in test public1 ↓↓↓ makes no sense based on the instructions.");
		String base_url = "https://foo.com:8000/bar/xyz.html"; String link = "#abc"; String expected = "https://foo.com:8000/bar/xyz.html";
		
		String res_url = normalize(base_url, link);
		check(res_url, expected); System.out.println("");
		
		// public2 ~ passed without modification
		
		base_url = "https://foo.com:8000/bar/xyz.html"; link = "blah.html#test"; expected = "https://foo.com:8000/bar/blah.html";
		res_url = normalize(base_url, link);
		check(res_url, expected);
		
		// public3 ~ passed without modification
		
		base_url = "https://foo.com:8000/bar/xyz.html"; link = "../blubb/123.html"; expected = "https://foo.com:8000/blubb/123.html";
		res_url = normalize(base_url, link);
		check(res_url, expected); 
		
		// public4 ~ passed without modification
		
		base_url = "https://foo.com:8000/bar/xyz.html"; link = "/one/two.html"; expected = "https://foo.com:8000/one/two.html";
		res_url = normalize(base_url, link);
		check(res_url, expected);
		
		// public5 ~ passed without modification
		
		base_url = "https://foo.com:8000/bar/xyz.html"; link = "http://elsewhere.com/some.html"; expected = "http://elsewhere.com:80/some.html";
		res_url = normalize(base_url, link);
		check(res_url, expected);
		
		System.out.println("\nHIDDEN\n");
		
		// normal1 ~ result is the same, but without a port number. The instructions say:
		
		/*
		 * "You should apply a simplified the normalization step to the seed URL as well. 
		 * Obviously, this won’t be a relative link, because there is no page that it can 
		 * be relative to, but you should add the port number if it is missing"
		 */
		
		System.out.println("Based on the instructions I was right to add a port here ↓↓↓ (and for the next six tests).");
		
		base_url = "http://<someHostname>/foo/bar/pageName.html"; link = "same-level.html"; expected = "http://<someHostname>:80/foo/bar/same-level.html";
		res_url = normalize(base_url, link);
		check(res_url, expected);
		
		// normal2
		
		base_url = "http://<someHostname>/foo/bar/pageName.html"; link = "../one-level-up.html"; expected = "http://<someHostname>:80/foo/one-level-up.html";
		res_url = normalize(base_url, link);
		check(res_url, expected);
		
		// normal3
		
		base_url = "http://<someHostname>/foo/bar/pageName.html"; link = "../../two-levels-up.html"; expected = "http://<someHostname>:80/two-levels-up.html";
		res_url = normalize(base_url, link);
		check(res_url, expected);
		
		// normal4
		
		base_url = "http://<someHostname>/foo/bar/pageName.html"; link = "/root-direct.html"; expected = "http://<someHostname>:80/root-direct.html";
		res_url = normalize(base_url, link);
		check(res_url, expected);
		
		// normal5
		
		base_url = "http://<someHostname>/foo/bar/pageName.html"; link = "/blah/root-subdir.html"; expected = "http://<someHostname>:80/blah/root-subdir.html";
		res_url = normalize(base_url, link);
		check(res_url, expected);
		
		// normal6
		
		base_url = "http://<someHostname>/foo/bar/pageName.html"; link = "http://<someHostname>/foo/bar/full-url.html"; expected = "http://<someHostname>:80/foo/bar/full-url.html";
		res_url = normalize(base_url, link);
		check(res_url, expected);
		
		// normal7
		
		base_url = "http://<someHostname>/foo/bar/pageName.html"; link = "http://<someHostname>/with-hash.html#something"; expected = "http://<someHostname>:80/with-hash.html";
		res_url = normalize(base_url, link);
		check(res_url, expected); System.out.println("");
		
		// normal8 ~ passed locally without modification
		
		base_url = "http://<someHostname>/foo/bar/pageName.html"; link = "http://<someHostname>:80/with-port.html"; expected = "http://<someHostname>:80/with-port.html";
		res_url = normalize(base_url, link);
		check(res_url, expected);
		
		// normal9 ~ has attribute (target = "_blank") . . ? This IS NOT mentioned anywhere in the assignment. I cannot explain the expected behavior here
		
		base_url = "http://<someHostname>/foo/bar/pageName.html"; link = "http://<someHostname>/other-attr.html"; expected = "http://<someHostname>:80/other-attr.html";
		res_url = normalize(base_url, link);
		check(res_url, expected);
		
		// normal10 ~ passed locally without modification
		
		base_url = "http://<someHostname>/foo/bar/pageName.html"; link = "http://<someOtherHostname>/different-host.html"; expected = "http://<someOtherHostname>:80/different-host.html";
		res_url = normalize(base_url, link);
		check(res_url, expected);
		
		System.out.println("\nCUSTOM\n");
		
		// custom1
		
		base_url = "http://<someHostname>/a/b/c/d/e/pageName.html"; link = "../../../../b.html"; expected = "http://<someHostname>:80/a/b.html";
		res_url = normalize(base_url, link);
		check(res_url, expected);
			
	}

}
