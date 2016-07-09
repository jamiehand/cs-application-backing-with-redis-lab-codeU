package com.flatironschool.javacs;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.jsoup.select.Elements;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/**
 * Represents a Redis-backed web search index.
 *
 */
public class JedisIndex {

	private Jedis jedis;

	/**
	 * Constructor.
	 *
	 * @param jedis
	 */
	public JedisIndex(Jedis jedis) {
		this.jedis = jedis;
	}

	/**
	 * Returns the Redis key for a given search term.
	 *
	 * @return Redis key.
	 */
	private String urlSetKey(String term) {
		return "URLSet:" + term;
	}

	/**
	 * Returns the Redis key for a URL's TermCounter.
	 *
	 * @return Redis key.
	 */
	private String termCounterKey(String url) {
		return "TermCounter:" + url;
	}

	/**
	 * Checks whether we have a TermCounter for a given URL.
	 *
	 * @param url
	 * @return
	 */
	public boolean isIndexed(String url) {
		String redisKey = termCounterKey(url);
		return jedis.exists(redisKey);
	}

	/**
	 * Looks up a search term and returns a set of URLs.
	 *
	 * @param term
	 * @return Set of URLs.
	 */
	public Set<String> getURLs(String term) {
        // FILL THIS IN!
		return null;
	}

    /**
	 * Looks up a term and returns a map from URL to count.
	 *
	 * @param term
	 * @return Map from URL to count.
	 */
	public Map<String, Integer> getCounts(String term) {
        // FILL THIS IN!
		return null;
	}

    /**
	 * Returns the number of times the given term appears at the given URL.
	 *
	 * @param url
	 * @param term
	 * @return
	 */
	public Integer getCount(String url, String term) {
        // FILL THIS IN!
		String strCount = jedis.hget(termCounterKey(url), term);
		return Integer.parseInt(strCount);
	}

	/**
	 * Print the terms and their counts in arbitrary order.
	 */
	public void printCounts(String url) {
		for (String term: jedis.hkeys(termCounterKey(url))) {
			Integer count = getCount(url, term);
			System.out.println(term + ", " + count);
		}
	}

	/**
	 * Add a page to the index.
	 *
	 * @param url         URL of the page.
	 * @param paragraphs  Collection of elements that should be indexed.
	 */
	public void indexPage(String url, Elements paragraphs) {
        // FILL THIS IN!
		// just like in index.java
		// make a TermCounter and count the terms in the paragraphs
		JedisTermCounter jtc = new JedisTermCounter(url, jedis);

		/* add term to "TermCounter:url" and increment its count */
		jtc.processElements(paragraphs);

		// TODO: do this as a Transaction to make it more efficient.
		// for each term in the TermCounter, add the TermCounter to the index
		System.out.println("hello6e!");
		Set<String> terms = jedis.hkeys(termCounterKey(url));
		Transaction t = jedis.multi();
		for (String term: terms) {
			add(term, url, t);
		}
		t.exec();
		System.out.println("hello6f!");
	}

	/**
	 * Adds a URL to the redis URLSet associated with `term`.
	 *
	 * @param term
	 * @param url
	 */
	public void add(String term, String url, Transaction t) {
		t.sadd(urlSetKey(term), url);
		// jedis.sadd("URLSet:term", "url");
		// This is done in jtc.processElements:
		//jedis.hincrBy("TermCounter:url", "term", 1);
	}

	/* for testing add(term, url) */
	public Set<String> urlSetMembers(String term) {
		String key = urlSetKey(term);
		Set<String> urls = jedis.smembers(key);
		return urls;
	}

	/**
	 * Prints the contents of the index.
	 *
	 * Should be used for development and testing, not production.
	 */
	public void printIndex() {
		// loop through the search terms
		for (String term: termSet()) {
			System.out.println(term);

			// for each term, print the pages where it appears
			Set<String> urls = getURLs(term);
			for (String url: urls) {
				Integer count = getCount(url, term);
				System.out.println("    " + url + " " + count);
			}
		}
	}

	/**
	 * Returns the set of terms that have been indexed.
	 *
	 * Should be used for development and testing, not production.
	 *
	 * @return
	 */
	public Set<String> termSet() {
		Set<String> keys = urlSetKeys();
		Set<String> terms = new HashSet<String>();
		for (String key: keys) {
			String[] array = key.split(":");
			if (array.length < 2) {
				terms.add("");
			} else {
				terms.add(array[1]);
			}
		}
		return terms;
	}

	/**
	 * Returns URLSet keys for the terms that have been indexed.
	 *
	 * Should be used for development and testing, not production.
	 *
	 * @return
	 */
	public Set<String> urlSetKeys() {
		return jedis.keys("URLSet:*");
	}

	/**
	 * Returns TermCounter keys for the URLS that have been indexed.
	 *
	 * Should be used for development and testing, not production.
	 *
	 * @return
	 */
	public Set<String> termCounterKeys() {
		return jedis.keys("TermCounter:*");
	}

	/**
	 * Deletes all URLSet objects from the database.
	 *
	 * Should be used for development and testing, not production.
	 *
	 * @return
	 */
	public void deleteURLSets() {
		Set<String> keys = urlSetKeys();
		Transaction t = jedis.multi();
		for (String key: keys) {
			t.del(key);
		}
		t.exec();
	}

	/**
	 * Deletes all URLSet objects from the database.
	 *
	 * Should be used for development and testing, not production.
	 *
	 * @return
	 */
	public void deleteTermCounters() {
		Set<String> keys = termCounterKeys();
		Transaction t = jedis.multi();
		for (String key: keys) {
			t.del(key);
		}
		t.exec();
	}

	/**
	 * Deletes all keys from the database.
	 *
	 * Should be used for development and testing, not production.
	 *
	 * @return
	 */
	public void deleteAllKeys() {
		Set<String> keys = jedis.keys("*");
		// transaction saves up a batch of operations to execute at once (faster
		// than sending each operation one at a time)
		Transaction t = jedis.multi();
		for (String key: keys) {
			t.del(key);
		}
		t.exec();
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Jedis jedis = JedisMaker.make();
		JedisIndex index = new JedisIndex(jedis);
		System.out.println("hello0!");

		index.deleteTermCounters();
		index.deleteURLSets();
		index.deleteAllKeys();
		System.out.println("hello1!");
		loadIndex(index);
		System.out.println("hello2!");

		// Map<String, Integer> map = index.getCounts("the");
		// for (Entry<String, Integer> entry: map.entrySet()) {
		// 	System.out.println(entry);
		// }
	}

	/**
	 * Stores two pages in the index for testing purposes.
	 *
	 * @return
	 * @throws IOException
	 */
	private static void loadIndex(JedisIndex index) throws IOException {
		WikiFetcher wf = new WikiFetcher();
		System.out.println("hello3!");

		String url = "https://en.wikipedia.org/wiki/Java_(programming_language)";
		Elements paragraphs = wf.readWikipedia(url);
		System.out.println("hello4!");

		index.indexPage(url, paragraphs);
		System.out.println("hello5!");

		url = "https://en.wikipedia.org/wiki/Programming_language";
		paragraphs = wf.readWikipedia(url);
		index.indexPage(url, paragraphs);

		// int theCount = index.getCount(url, "the");
		// System.out.println("theCount is: " + theCount + "!!!");
		// index.printCounts(url);
	}
}
