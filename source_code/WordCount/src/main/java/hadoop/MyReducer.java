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

public class MyReducer extends Reducer<Text, Text, Text, Text> {

	//language + counter or stopwords
	private HashMap<String, HashMap<String, Integer>> allData = new HashMap<>();
	private HashMap<String, HashSet<String>> stopwordList = new HashMap<>();
	private final int MAX_TOP = 10;

	//key = language_word, counter
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String[] language_key = key.toString().split("_");
		
		if(language_key.length < 2) {
			return;
		}
		
		String language = language_key[0];
		String str = language_key[1];
        
		//load stopwords
        if (!stopwordList.containsKey(language)) {
            stopwordList.put(language, IOUtils.readFileStopword(language + ".txt"));
        } 
        
		//if not a stopword put in output map
        if (stopwordList.get(language).contains(str)) {
        	return;
        }
        
		int sum = 0;

		for (Text val : values) {
			sum += Integer.parseInt(val.toString());
		}
		
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

		Comparator<Entry<String, Integer>> comparator = new Comparator<Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		};

		/**
		 * print
		 * z.B.: 
		 *  ------------------------------------
		 * dutch
		 * top 10 words=[den=1520, t=699, gij=627, zich=518, zoo=501, eene=488, u=418, der=375, marten=330, baas=303]
		 * run time in ms=349
		 *
		 */
		for (Entry<String, HashMap<String, Integer>> entry : allData.entrySet()) {

			StringBuilder sb = new StringBuilder("------------------------------------\n");
			sb.append(entry.getKey());//print name of language
			sb.append("\nTop 10 words=");
			List<Entry<String, Integer>> values = new ArrayList<>(entry.getValue().entrySet());
			Collections.sort(values, comparator);
			sb.append(values.subList(0, values.size() > MAX_TOP ? MAX_TOP : values.size()));
			context.write(new Text(sb.toString()), new Text("\n"));
		}

	}
}