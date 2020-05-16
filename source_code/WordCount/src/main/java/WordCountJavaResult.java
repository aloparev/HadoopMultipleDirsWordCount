import java.util.List;
import java.util.Map;

public class WordCountJavaResult {
    String language;
    List<Map.Entry<String, Long>> top10Words;
    Long runTimeMs;

    public WordCountJavaResult(String language, List<Map.Entry<String, Long>> top10Words, Long runTimeMs) {
        this.language = language;
        this.top10Words = top10Words;
        this.runTimeMs = runTimeMs;
    }
}
