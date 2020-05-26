import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.nio.file.Paths;

import org.junit.Test;

import utils.Utils;

public class HadoopTest {

	@Test
	public void readStopwords() {

		try {
			String uri = Paths.get(Thread.currentThread().getContextClassLoader().getResource("stopwords/english.txt").toURI()).toString();
			assertTrue(Utils.readFileStopword(uri).size() > 0);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

}