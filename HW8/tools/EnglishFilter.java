package cis5550.tools;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class EnglishFilter {
	
    static Set<String> nonEnglishUrlCountryCodes = new HashSet<String> (Arrays.asList(
            "ae", "al", "am", "ao", "ar",
            "az", "ba", "bd", "bh", "bj",
            "bn", "bo", "br", "bw", "by",
            "bz", "cd", "cf", "cg", "ci",
            "cl", "cn", "co", "cr", "cu",
            "cy", "dj", "do", "dz", "ec",
            "eg", "er", "et", "ge", "gh",
            "gm", "gn", "gq", "gt", "gw",
            "hk", "hn", "hr", "ht", "id",
            "il", "in", "iq", "ir", "jo",
            "jp", "ke", "kg", "kh", "km",
            "kp", "kr", "kw", "kz", "lb",
            "lk", "lr", "ls", "lt", "lu",
            "lv", "ly", "ma", "md", "mg",
            "mk", "ml", "mm", "mn", "mr",
            "mt", "mu", "mv", "mw", "mx",
            "my", "mz", "na", "ne", "ng",
            "ni", "np", "om", "pa", "pe",
            "pg", "ph", "pk", "pl", "pr",
            "ps", "py", "qa", "ro", "rs",
            "ru", "rw", "sa", "sb", "sd",
            "si", "sk", "sl", "sn", "so",
            "sr", "ss", "sv", "sy", "sz",
            "td", "tg", "th", "tj", "tm",
            "tn", "tr", "tt", "tw", "tz",
            "ua", "ug", "uy", "uz", "ve",
            "vn", "ye", "za", "zm", "zw"
    ));
    
    static Set<String> nonEnglishUrlLanguageCodes = new HashSet<String> (Arrays.asList(
            "af", "am", "ar", "az", "be",
            "bg", "bn", "bs", "ca", "ceb",
            "co", "cs", "cy", "da", "de",
            "el", "en", "eo", "es", "et",
            "eu", "fa", "fi", "fr", "fy",
            "ga", "gd", "gl", "gu", "ha",
            "haw", "he", "hi", "hmn", "hr",
            "ht", "hu", "hy", "id", "ig",
            "is", "it", "iw", "ja", "jw",
            "ka", "kk", "km", "kn", "ko",
            "ku", "ky", "la", "lb", "lo",
            "lt", "lv", "mg", "mi", "mk",
            "ml", "mn", "mr", "ms", "mt",
            "my", "ne", "nl", "no", "ny",
            "or", "pa", "pl", "ps", "pt",
            "ro", "ru", "rw", "sd", "si",
            "sk", "sl", "sm", "sn", "so",
            "sq", "sr", "st", "su", "sv",
            "sw", "ta", "te", "tg", "th",
            "tl", "tr", "uk", "ur", "uz",
            "vi", "xh", "yi", "yo", "zh",
            "zu"
   ));
   
   public static String filter(String url)  {
	   String out = url;
	   
	   for (String component : url.split("\\P{Alnum}+")) {
		   if (nonEnglishUrlCountryCodes.contains(component) | nonEnglishUrlLanguageCodes.contains(component)) {return null;}
	   }
	   
	   return out;
   }
   
   public static List<String> filter(List<String> URLs) {
	   
	   List<String> out = new LinkedList<String>();
	   
	   for (String url: URLs) {
		   out.add(filter(url));
	   }

	   return out;
   }
   
   public static void main(String args[]) {
	   
	   System.out.println(filter("https://sv.wikipedia.org:443/wiki/Belgare_i_Sverige"));
	   
	   System.out.println(filter("https://en-ca.facebook.com:443/pages/create/"));
	   
	   System.out.println(filter("https://github.com/CIS5550/23sp-CIS5550-Team-googel/blob/master/HW8/tests/NormalizationTest.java"));
   }
	
}
