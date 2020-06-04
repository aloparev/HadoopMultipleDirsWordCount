package hadoop;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		private final static Text one = new Text("1");
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString().toLowerCase();
			List<String> words = new ArrayList<>();
			
			//splits value and remove the special character at the same time 
			Collections.addAll(words, line.split("\\P{L}+"));
			
			//delete words less than or equal to 2
			words.removeIf(str -> (str.length()) <= 2);
			
			//get language folder
			String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
			String language = new Path(filePathString).getParent().getName().toLowerCase();
			
			//loop >> glue language and words
			for (String w : words) {

				word.set(language + "_" + w); //language_word, Ex: english_water
				context.write(word, one);
			}
		}
	}
