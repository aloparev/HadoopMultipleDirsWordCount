import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import java.net.URI;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	public static final String[] languages = new String[] { "dutch", "english", "french", "german", "italian",
			"russian", "spanish", "ukrainian" };

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private final static Text one = new Text("1");
		private Text word = new Text();
		private ArrayList<String> stopwordList = new ArrayList<>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			String str = "";

			String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
			String folderName = new Path(filePathString).getParent().getName().toLowerCase();// English
			stopwordList = IOUtil.readFile(String.format("./%s.txt", folderName));

			while (tokenizer.hasMoreTokens()) {

				str = tokenizer.nextToken().toLowerCase();

				if (stopwordList.contains(str)) {
					continue;
				}

				word.set(String.format("%s_%s", folderName, str));// "english_youngest"

				context.write(word, one);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private HashMap<String, TreeMap<Text, IntWritable>> allData = new HashMap<>();
		private final int MAX_TOP = 10;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			allData = new HashMap<>();
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int sum = 0;

			for (Text val : values) {
				sum += Integer.parseInt(val.toString());
			}

			String[] language_key = new Text(key).toString().split("_");
			String language = language_key[0];// english

			key.set(language_key[1]);// youngest

			if (allData.containsKey(language)) {

				allData.get(language).put(new Text(key), new IntWritable(sum));
			} else {
				TreeMap<Text, IntWritable> val = new TreeMap<>();

				val.put(new Text(key), new IntWritable(sum));
				allData.put(language, val);
			}

		}

		// cleanup: Called once at the end of the task.
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			Comparator<Entry<Text, IntWritable>> comparator = new Comparator<Entry<Text, IntWritable>>() {

				@Override
				public int compare(Entry<Text, IntWritable> o1, Entry<Text, IntWritable> o2) {
					return o2.getValue().compareTo(o1.getValue());
				}
			};

			for (Entry<String, TreeMap<Text, IntWritable>> entry : allData.entrySet()) {

				// context.write(new Text(entry.getKey()), new IntWritable(0));
				context.write(new Text(""), new Text(""));
				context.write(new Text("============  ".concat(entry.getKey())), new Text("============"));

				List<Entry<Text, IntWritable>> values = new ArrayList<>(entry.getValue().entrySet());

				Collections.sort(values, comparator);

				int length = values.size() > MAX_TOP ? MAX_TOP : values.size();

				for (int i = 0; i < length; i++) {
					// context.write(values.get(i).getKey(), values.get(i).getValue());
					context.write(values.get(i).getKey(), new Text(values.get(i).getValue().toString()));
				}
			}

		}
	}

	// args = [<language>, <input path>, <output path>]
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		// check 2 params

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "WordCount");

		job.setJarByClass(WordCount.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// add files contain stoppwords
		URI[] files = new URI[languages.length];

		for (int i = 0; i < languages.length; i++) {
			files[i] = new URI(String.format("stopwords/%s.txt", languages[i]));
		}

		job.setCacheFiles(files);
		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}