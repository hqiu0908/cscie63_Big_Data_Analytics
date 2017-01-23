import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.BufferedReader;

public class WordCounter {

	HashSet<String> stopwords = new HashSet<String>(Arrays.asList("the", "a", "an", "and", "or", "of", "to",
			"about", "above", "after", "all", "are", "be", "but", "he", "she", "by", "can't", "for", "do", "i",
			"has", "don't", "her", "is", "in", "our", "his", "with", "that", "you", "it", "was", "on", "him"));

	String delimiters = "[.,;:\\*\\-\\?!\\\"\\[\\]\\(\\)\\{\\}_]+";

	private List<HashMap<String, Integer>> parseFile(String filename) throws IOException {
		File file = new File(filename);

		String line = null;
		List<HashMap<String, Integer>> wordCount = new LinkedList<HashMap<String, Integer>>();

		// Read the input file.
		BufferedReader br = new BufferedReader(new FileReader(file));

		// Divide each line into (word, count) pairs.
		while ((line = br.readLine()) != null) {				
			String[] pairs = line.split("\t");
			String word = pairs[0];
			word = processWord(word);
			if (word == null) {
				continue;
			}
			int num = Integer.parseInt(pairs[1]);

			HashMap<String, Integer> pair = new HashMap<String, Integer>();
			pair.put(word, num);

			if (! stopwords.contains(word)) {
				wordCount.add(pair);
			}
		}

		br.close();

		return wordCount;
	}

	private String processWord(String str) {
		String[] words = str.split(delimiters);

		String word = null;

		for (int i = 0; i < words.length; i++) {
			if (!words[i].trim().isEmpty()) {
				word = words[i];
				break;
			}
		}

		return (word == null? null : word.toLowerCase());
	}

	private List<Map.Entry<String, Integer>> reducer(List<HashMap<String, Integer>> wordCount) {
		HashMap<String, Integer> reducedPairs = new HashMap<String, Integer>();

		for (int i = 0; i < wordCount.size(); i++) {
			HashMap<String, Integer> pair = wordCount.get(i);
			// Each HashMap only have one entry here. Get the key value pair.
			Entry<String, Integer> entry = pair.entrySet().iterator().next();

			String word = entry.getKey();
			int val = entry.getValue();

			// Aggregate to (word, count) pairs.
			if (reducedPairs.containsKey(word)) {
				reducedPairs.put(word, reducedPairs.get(word) + val);				
			} else {
				reducedPairs.put(word, val);
			}
		}

		// Sort all the entries based on its occurrence. If the words have the same occurrence,
		// sort them alphabetically.
		List<Map.Entry<String, Integer>> sortedEntries = sortHashMap(reducedPairs);

		return sortedEntries;
	}

	// Sort the name value pairs.
	private List<Map.Entry<String, Integer>> sortHashMap(HashMap<String, Integer> hashmap) {
		List<Map.Entry<String, Integer>> sortedEntries = new LinkedList<Map.Entry<String, Integer>>(hashmap.entrySet());

		Collections.sort(sortedEntries, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> left, Map.Entry<String, Integer> right) {
				if (left.getValue() != right.getValue()) {
					return right.getValue() - left.getValue();
				}

				return left.getKey().compareTo(right.getKey());
			}
		});

		return sortedEntries;
	}

	private void printHashEntryList(List<Map.Entry<String, Integer>> list) throws IOException {
		FileWriter fw = new FileWriter("output_processed.txt");

		for (int i = 0; i < 200; i++) {
			Map.Entry<String, Integer> entry = list.get(i);
			String line = entry.getKey() + "\t" + entry.getValue() + "\n";
			fw.write(line);
		}

		fw.close();
	}

	public static void main (String[] args) throws IOException {
		String filename = "/Users/hqiu/Documents/Virtual Machines.localized/VM_shared/part-r-00000";

		WordCounter wordCounter = new WordCounter();

		List<HashMap<String, Integer>> wordCount = wordCounter.parseFile(filename);

		List<Map.Entry<String, Integer>> sortedEntries = wordCounter.reducer(wordCount);
		wordCounter.printHashEntryList(sortedEntries);
	}
}

