package java_pararell;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WordCountJavaParallel {
    //private static final String splitRegex1 = "[\\s\\d\\p{P}]+(?<!-)";
    private static final String splitRegex2 = "\\P{L}+";
   // private static final String splitRegex3 = "\\W+";

    public static WordCountJavaResult getWordCountResultWithTime(String language, List<String> text, List<String> stopwords) {
        long startTime = System.nanoTime();
        Supplier<Stream<String>> words = () -> splitAndCleanLines(text, stopwords);
        List<Map.Entry<String, Long>> wordsFreqSorted = countWords(words);
        long endTime = System.nanoTime();
        long totalTime = TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS);
        return new WordCountJavaResult(language, wordsFreqSorted.subList(0, 10), totalTime);
    }

    private static Stream<String> splitAndCleanLines(List<String> text, List<String> stopwords) {
        return text
                .stream()
                .parallel()
                .map(line -> line.split(" "))
                .flatMap(Stream::of)
                .map(String::toLowerCase)
                .map(word -> word.replaceAll(splitRegex2, ""))
                .filter(word -> !stopwords.contains(word));
    }

    private static List<Map.Entry<String, Long>> countWords(Supplier<Stream<String>> words) {
        return words.get()
                .parallel()
                .collect(Collectors.groupingBy(String::toString, Collectors.counting()))
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toList());
    }
}


