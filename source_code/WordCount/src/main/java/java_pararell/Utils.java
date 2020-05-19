package java_pararell;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

import java.nio.file.Path;

@Slf4j
public class Utils {
    public static ArrayList<String> readFile(String fileName) {
 
        BufferedReader bf = null;
        ArrayList<String> rs = new ArrayList<String>();
        try {
        	//read UTF-8 encoded data from a file
        	bf = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "UTF-8"));
        	String line = "";
        	
            while ((line = bf.readLine()) != null) {
                rs.add(line);
            }
            
            bf.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return rs;
    }

	public static List<String> readStopwords(String fileName) {
		List<String> stopwords = emptyList();
		try (Stream<String> lines = Files.lines(Paths.get(Thread.currentThread().getContextClassLoader().getResource("stopwords/"+fileName + ".txt").toURI()))) {
			stopwords = lines.collect(Collectors.toList());
			stopwords.add("");
		} catch (IOException | URISyntaxException e) {
			System.out.println("Could not read stopwords for fileName=" + fileName);
		}
		return stopwords;
	}

	public static List<String> readAllFilesFromDir(String dirName) {
		List<String> text;
		try {
			Path path = getResourcePath(dirName);
//			log.info(path.toString());
			text = readAllFilesFromPath(dirName, path);
		} catch (IOException | URISyntaxException e) {
			System.out.println("Could not read files for dirName=" + dirName);
			text = emptyList();
		}
		return text;
	}

	private static Path getResourcePath(String dirName) throws URISyntaxException {
		return Paths.get(Thread.currentThread().getContextClassLoader().getResource(dirName).toURI());
	}

	private static List<String> readAllFilesFromPath(String directoryName, Path pathName) throws IOException, URISyntaxException {
		return Files.walk(pathName)
				.filter(s -> s.toString().endsWith(".txt"))
				.map(Utils::readFromFile)
				.flatMap(Collection::stream)
				.collect(Collectors.toList());
	}

	private static List<String> readFromFile(Path filePath) {
		try (Stream<String> lines = Files.lines(filePath)) {
			return lines.collect(Collectors.toList());
		} catch (IOException e) {
			System.out.println("Could not read from file: " + filePath.toString());
			return emptyList();
		}
	}
}
