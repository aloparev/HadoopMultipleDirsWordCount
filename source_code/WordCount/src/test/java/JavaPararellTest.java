import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import java_pararell.Utils;
import java_pararell.WordCountJavaParallel;
import java_pararell.WordCountJavaResult;

public class JavaPararellTest {

	
	WordCountJavaParallel WCParallel;
	String[] languages;
	
	@Before
	public void setUp() throws Exception {
		WCParallel = new WordCountJavaParallel();
		languages = new String[] { "dutch", "english", "french", "german", "italian", "russian", "spanish", "ukrainian" };
	}

	@Test
	public void test() {
		String language = "english";
		WordCountJavaResult javaResult = WordCountJavaParallel.getWordCountResultWithTime(language, Utils.readAllFilesFromDir(language), Utils.readStopwords(language));
	}

}
