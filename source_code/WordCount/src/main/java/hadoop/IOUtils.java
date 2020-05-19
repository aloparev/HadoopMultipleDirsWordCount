package hadoop;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashSet;

public class IOUtils {
    public static HashSet<String> readFileStopword(String fileName) {
   
    	HashSet<String> rs = new HashSet<String>();
        
        try {
        	//read UTF-8 encoded data from a file
        	BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "UTF-8"));
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
}
