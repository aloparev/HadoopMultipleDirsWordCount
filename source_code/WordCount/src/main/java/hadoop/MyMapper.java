package hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import utils.Utils;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final static Text one = new Text("1");
    private Text word = new Text();
    /**
     * contains stopword lists for each language
     * key:language value: stopwords of the language
     */
    private HashMap<String, HashSet<String>> stopwordList = new HashMap<>();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();
        List<String> words = new ArrayList<>();
        // splits value and remove the special character at the same time
        Collections.addAll(words, line.split("\\P{L}+"));
        // get language folder
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
        String language = new Path(filePathString).getParent().getName().toLowerCase();
        // load stopwords
        if (!stopwordList.containsKey(language)) {
            HashSet<String> stopwords = Utils.readFileStopword(language + ".txt");
            if (stopwords == null) {// file not found, do nothing
                return;
            }
            // add the stopwords list of the associated language to the general list
            stopwordList.put(language, stopwords);
        }
        // loop >> glue language and words
        for (String w : words) {
            // delete words less than or equal to 2
            if (w.length() <= 2) {
                continue;// ignore
            }
            // if the word is stopword
            if (stopwordList.get(language).contains(w)) {
                continue;// ignore
            }
            word.set(language + "_" + w); // language_word, Ex: english_water
            context.write(word, one);
        }
    }
}
