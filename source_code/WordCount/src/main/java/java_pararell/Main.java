package java_pararell;

import utils.Utils;

public class Main {
    public static void main(String[] args) throws Exception {
        long javaRunTimeTotalMs = 0;
        String[] languages = new String[]{"dutch", "english", "french", "german", "italian", "russian", "spanish",
                "ukrainian"};

        for (String language : languages) {
            System.out.println("java parallel: start processing " + language);
            WordCountJavaResult javaResult = WordCountJavaParallel.getWordCountResultWithTime(language,
                    Utils.readAllFilesFromDir(language), Utils.readStopwords(language));

            System.out.println("java parallel: top10words=" + javaResult.top10Words);
            System.out.println("java parallel: run time in ms=" + javaResult.runTimeMs);
            System.out.println("------------------------------------");
            javaRunTimeTotalMs += javaResult.runTimeMs;
        }
        System.out.println("\njava parallel: total run time in seconds: " + javaRunTimeTotalMs / 1000);
    }
}