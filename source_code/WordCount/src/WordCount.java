import java.io.IOException;
import java.util.*;
     
import org.apache.hadoop.fs.Path;
import java.net.URI;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
     
public class WordCount {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private ArrayList<String> stopwordList = new ArrayList<String>();
	    
	    private void loadstopwords(Context context) {
	    	
	    	String filePath = "./".concat(context.getConfiguration().get("language")).concat(".txt");
	    	
	    	stopwordList = IOUtil.readFile(filePath);
	    }
	     
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
		    StringTokenizer tokenizer = new StringTokenizer(line);
		    String str = "";
		    
		    loadstopwords(context);
	
		    while (tokenizer.hasMoreTokens()) {
		    	
		    	str = tokenizer.nextToken();
		    	
		    	if(stopwordList.contains(str)) {
		    		continue;
		    	}
		    	
		        word.set(str);
		        context.write(word, one);
		    }
	    }
	 }
	     
	 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	 
	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	      throws IOException, InterruptedException {
	        
	    	int sum = 0;
		    
	    	//đọc từng dự liệu từ map
	        for (IntWritable val : values) {
		        sum += val.get();
		    }
		    
		    context.write(key, new IntWritable(sum));
	    }
	 }
	     
	 public static void main(String[] args) throws Exception {
	    
		Configuration conf = new Configuration();
		String language = args[0];
		conf.set("language", language);
		
	    @SuppressWarnings("deprecation")
		Job job = new Job(conf, "WordCount");
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	     
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	     
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setJarByClass(WordCount.class);
	    			
	    URI[] files = new URI[] {
	    		new URI("stopwords/".concat(language).concat(".txt"))
	    };
	    
	    job.setCacheFiles(files);
	    
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	     
	    job.waitForCompletion(true);
	 }
     
}