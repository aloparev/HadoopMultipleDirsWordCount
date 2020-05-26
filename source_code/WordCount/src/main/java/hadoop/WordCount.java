package hadoop;
import java.net.URI;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class WordCount {
	public static final String[] languages = new String[] { "dutch", "english", "french", "german", "italian",
			"russian", "spanish", "ukrainian" };

	public static void main(String[] args) throws Exception {

		log.info("hadoop word count up and running");

		// check 2 params
        if(args.length != 2) {
            log.info("two args are expected: input and output folders!");
            return;
        }

		Configuration conf = new Configuration();


		log.info("run args: in=" + args[0] + " out=" + args[1]);

		//Start: check if input-output-stopwords exist
		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);
		Path stopwordsPath = new Path("/user/"+System.getProperty("user.name")+"/stopwords");

		FileSystem fs = FileSystem.get(conf);

		if (!fs.exists(stopwordsPath)) {
			log.info("Stopwords don't exist: " + stopwordsPath);
			return;
		}

		if (!fs.exists(inPath)) {
			log.info("Input folder not exists");
			return;
		}

		if (fs.exists(outPath)) {
			log.info("Output folder already exists... Overwriting");
			fs.delete(outPath, true);
		}
		//End: check if input-output-stopwords exist

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "WordCount");

		//set input/output paths:
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		//set main class for the jar
		job.setJarByClass(WordCount.class);

		//set the mapper and reducer classes
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		//input und output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//output types (key, value)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//read files recursively
		FileInputFormat.setInputDirRecursive(job, true);

		//add stoppword-files to Cache
		URI[] stopwordFiles = new URI[languages.length];

		for (int i = 0; i < languages.length; i++) {
			stopwordFiles[i] = new URI(String.format("stopwords/%s.txt", languages[i]));
		}

		job.setCacheFiles(stopwordFiles);

		//run the job
		try {
			job.waitForCompletion(true);
		} catch(InterruptedException e){
			e.printStackTrace();
		} catch(ClassNotFoundException e){
			e.printStackTrace();
		}

		System.out.println("Done!");
	}
}