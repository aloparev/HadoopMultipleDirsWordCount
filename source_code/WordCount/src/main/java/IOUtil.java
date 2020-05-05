import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class IOUtil {

	public static ArrayList<String> readFile(String filename) {
		
		BufferedReader bf = null;
		ArrayList<String> rs = new ArrayList<String>();
		
		try {
			FileInputStream fis = new FileInputStream(filename);
			bf = new BufferedReader(new InputStreamReader(fis));
			
			String val;
			
			while((val = bf.readLine()) != null) {
				rs.add(val);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				bf.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return rs;
	}

}
