package hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import static utils.Utils.readFileStopword;

public class MyReducer extends Reducer<Text, Text, Text, Text> {

	/**
	 * This variable (allData) contains the words and the counter of words for each language 
	 * 
	 * key:language
	 * value: {
	 * 			key: word,
	 * 			value: counter of the word
	 * 		  }
	 */
	private HashMap<String, HashMap<String, Integer>> allData = new HashMap<>();

	/**
	 * contains stopword lists for each language
	 * 
	 * key:language
	 * value: stopwords of the language
	 */
	private HashMap<String, HashSet<String>> stopwordList = new HashMap<>();
	private final int MAX_TOP = 10;

	/**
	 * key: language_word values: counter of word
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String[] language_key = key.toString().split("_");
		String language = language_key[0]; //language
		String str = language_key[1]; //word

		// load stopwords
		if (!stopwordList.containsKey(language)) {

			HashSet<String> stopwords = readFileStopword(language + ".txt");

			if (stopwords == null) {// file not found, do nothing
				return;
			}

			//add the stopwords list of the associated language to the general list
			stopwordList.put(language, stopwords);
		}

		// if the word is stopword
		if (stopwordList.get(language).contains(str)) {
			return;
		}

		//calculate the counter of word
		int sum = 0;

		for (Text val : values) {
			sum += Integer.parseInt(val.toString());
		}

		//add word and word counters to the list according to the corresponding language
		if (allData.containsKey(language)) {

			allData.get(language).put(str, sum);
		} else {

			HashMap<String, Integer> hmap = new HashMap<>();

			hmap.put(str, sum);
			allData.put(language, hmap);
		}
	}

	// cleanup: Called once at the end of the task.
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		// Create a comparator to compare and sort
		Comparator<Entry<String, Integer>> comparator = new Comparator<Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		};

		/**
		 * sort and print EX: ------------------------------------ dutch
		 * Top 10 words=[den=1520, zoo=512, marten=330, baas=303, zien=230, jan=224, vrouw=201, oogen=194, riep=185, goed=178]
		 *
		 */
		for (Entry<String, HashMap<String, Integer>> entry : allData.entrySet()) {

			// create a list of word and counter
			List<Entry<String, Integer>> values = new ArrayList<>(entry.getValue().entrySet());

			// sort descending by counter
			Collections.sort(values, comparator);

			StringBuilder sb = new StringBuilder("------------------------------------\n");

			sb.append(entry.getKey());// print name of language
			sb.append("\nTop " + MAX_TOP + " words=");
			sb.append(values.subList(0, values.size() > MAX_TOP ? MAX_TOP : values.size()));

			context.write(new Text(sb.toString()), new Text("\n"));
		}
	}
}