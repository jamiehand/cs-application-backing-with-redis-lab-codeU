package com.flatironschool.javacs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/**
 * Encapsulates a map from search term to frequency (count).
 *
 * @author downey
 *
 */
public class JedisTermCounter {

	// private Map<String, Integer> map;
	private String url;
	private Jedis jedis;

	public JedisTermCounter(String url, Jedis jedis) {
		this.url = url;
		this.jedis = jedis;
		// this.map = new HashMap<String, Integer>();
	}

	public String getUrl() {
		return url;
	}

	// /**
	//  * Returns the total of all counts.
	//  *
	//  * @return
	//  */
	// public int size() {
	// 	int total = 0;
	// 	for (Integer value: map.values()) {
	// 		total += value;
	// 	}
	// 	return total;
	// }

	/**
	 * Takes a collection of Elements and counts their words.
	 *
	 * @param paragraphs
	 */
	public void processElements(Elements paragraphs) {
		Transaction t = jedis.multi();
		System.out.println("hello6a!");
		for (Node node: paragraphs) {
			processTree(node, t);
		}
		t.exec();
		System.out.println("hello6d!");
	}

	/**
	 * Finds TextNodes in a DOM tree and counts their words.
	 *
	 * @param root
	 */
	public void processTree(Node root, Transaction t) {
		// NOTE: we could use select to find the TextNodes, but since
		// we already have a tree iterator, let's use it.
		System.out.println("hello6b!");
		for (Node node: new WikiNodeIterable(root)) {
			if (node instanceof TextNode) {
				processText(((TextNode) node).text(), t);
			}
		}
	}

	/**
	 * Splits `text` into words and counts them.
	 *
	 * @param text  The text to process.
	 */
	public void processText(String text, Transaction t) {
		// replace punctuation with spaces, convert to lower case, and split on whitespace
		String[] array = text.replaceAll("\\pP", " ").toLowerCase().split("\\s+");
		System.out.println("hello6c!");
		for (int i=0; i<array.length; i++) {
			String term = array[i];
			// incrementTermCount(term);
			t.hincrBy("TermCounter:" + url, term, 1);
			// System.out.println("hello7!");
		}
		// System.out.println("hello8!");
	}

	/**
	 * Increments the counter associated with `term`.
	 *
	 * @param term
	 */
	public void incrementTermCount(String term) {
		// System.out.println(term);
		// put(term, get(term) + 1);
	}

	// /**
	//  * Adds a term to the map with a given count.
	//  *
	//  * @param term
	//  * @param count
	//  */
	// public void put(String term, int count) {
	// 	map.put(term, count);
	// }

	// /**
	//  * Returns the count associated with this term, or 0 if it is unseen.
	//  *
	//  * @param term
	//  * @return
	//  */
	// public Integer get(String term) {
	// 	Integer count = map.get(term);
	// 	return count == null ? 0 : count;
	// }

	// /**
	//  * Returns the set of terms that have been counted.
	//  *
	//  * @return
	//  */
	// public Set<String> keySet() {
	// 	// return map.keySet();
	// 	return jedis.hkeys(termCounterKey(url));
	// }

	// /**
	//  * Print the terms and their counts in arbitrary order.
	//  */
	// public void printCounts() {
	// 	for (String key: keySet()) {
	// 		Integer count = get(key);
	// 		System.out.println(key + ", " + count);
	// 	}
	// 	System.out.println("Total of all counts = " + size());
	// }

	// /**
	//  * @param args
	//  * @throws IOException
	//  */
	// public static void main(String[] args) throws IOException {
	// 	String url = "https://en.wikipedia.org/wiki/Java_(programming_language)";
	//
	// 	WikiFetcher wf = new WikiFetcher();
	// 	Elements paragraphs = wf.fetchWikipedia(url);
	//
	// 	JedisTermCounter counter = new JedisTermCounter(url.toString());
	// 	counter.processElements(paragraphs);
	// 	// counter.printCounts();
	// }
}
