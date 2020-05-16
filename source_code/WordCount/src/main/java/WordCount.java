import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;

import java.net.URI;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

@Slf4j
public class WordCount {
    public static final String[] languages = new String[]{"dutch", "english", "french", "german", "italian",
            "russian", "spanish", "ukrainian"};

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final static Text one = new Text("1");
        private Text word = new Text();

//        runs for every folder
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            log.info("map start");
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String str = "";
            String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
            String folderName = new Path(filePathString).getParent().getName().toLowerCase();// English
            log.info("folderName=" + folderName);

//			loop1 >> glue language and words
            while (tokenizer.hasMoreTokens()) {
                str = tokenizer.nextToken().toLowerCase();
                word.set(String.format("%s_%s", folderName, str));// "english_youngest"
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        //language + counter or stopwords
        private HashMap<String, TreeMap<Text, IntWritable>> allData = new HashMap<>();
        private HashMap<String, ArrayList<String>> stopwordList = new HashMap<>();
        private final int MAX_TOP = 10;

        //		language_word, conter
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//language,word
            log.info("reducer start");
            String[] language_key = new Text(key).toString().split("_");
            if (language_key.length < 2) {
                return;
            }
            String language = language_key[0];// english
            String str = language_key[1];
//			load stopwords
            if (!stopwordList.containsKey(language)) {
                stopwordList.put(language, Utils.readFile(String.format("./%s.txt", language)));
            }
//			if not a stopword put in output map
            //if <val> is in group stopwords?
            if (stopwordList.get(language).contains(str)) {
                return;
            }
            int sum = 0;

//            loop1
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }
            key.set(str);// youngest
            if (allData.containsKey(language)) {
                allData.get(language).put(new Text(key), new IntWritable(sum));
            } else {
                TreeMap<Text, IntWritable> treeMap = new TreeMap<>();
                treeMap.put(new Text(key), new IntWritable(sum));
                allData.put(language, treeMap);
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

//            loop1
            for (Entry<String, TreeMap<Text, IntWritable>> entry : allData.entrySet()) {
                // context.write(new Text(entry.getKey()), new IntWritable(0));
                context.write(new Text(""), new Text(""));
                context.write(new Text("============  ".concat(entry.getKey())), new Text("============"));
                List<Entry<Text, IntWritable>> values = new ArrayList<>(entry.getValue().entrySet());
                Collections.sort(values, comparator);
                int length = values.size() > MAX_TOP ? MAX_TOP : values.size();

//                loop2
                for (int i = 0; i < length; i++) {
                    // context.write(values.get(i).getKey(), values.get(i).getValue());
                    context.write(values.get(i).getKey(), new Text(values.get(i).getValue().toString()));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        log.info("hadoop word count up and running");
        long javaRunTimeTotalMs = 0;

        if(args.length != 2) {
            log.info("check the number of args!");
        } else {
            URI[] stopwordFiles;
            String in = args[0];
            String out = args[1];
            Configuration conf = new Configuration();
            log.info("\trun args: in=" + args[0] + " out=" + args[1]);

//            @SuppressWarnings("deprecation")
//            Job job = new Job(conf, "WordCountTop10");
//            job.setJarByClass(WordCount.class);
//            job.setMapperClass(Map.class);
//            job.setReducerClass(Reduce.class);
//            job.setInputFormatClass(TextInputFormat.class);
//            job.setOutputFormatClass(TextOutputFormat.class);
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(Text.class);

            // add stopwords globally to hadoop and run java counter at the same time
            stopwordFiles = new URI[languages.length];
            for (int i = 0; i < languages.length; i++) {
                String language = languages[i];
                stopwordFiles[i] = new URI(String.format("stopwords/%s_stopwords.txt", language));
//                log.info(String.format("stopwords/%s_stopwords.txt", language));

                System.out.println("java parallel: start processing " + language);
                WordCountJavaResult javaResult = WordCountJavaParallel.getWordCountResultWithTime(language, Utils.readAllFilesFromDir(language), Utils.readStopwords(language));
                System.out.println("java parallel: top10words=" + javaResult.top10Words);
                System.out.println("java parallel: run time in ms=" + javaResult.runTimeMs);
                System.out.println("------------------------------------");
                javaRunTimeTotalMs += javaResult.runTimeMs;
            }
            System.out.println("\njava parallel: total run time in seconds: " + javaRunTimeTotalMs/1000);

//            job.setCacheFiles(stopwordFiles);
//            FileInputFormat.setInputDirRecursive(job, true);
//            FileInputFormat.addInputPath(job, new Path(in));
//            FileOutputFormat.setOutputPath(job, new Path(out));
//            job.waitForCompletion(true);
        }
    }
}