import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

//converts ARTICLE LEMMA INDEX to CSV by removing the commas in the middle of each tag
public class ConvertToCSV {

	public static void main(String[] args) {
		String output = "wikicsv.txt";
		String input = "newSmall.txt";
		
		BufferedReader br = null;
		BufferedWriter bw = null;
		int current;
		int prev = ' '; //initialize to a random value
		int count = 0;
		try {
			// Load ARTICLE LEMMA INDEX
			br = new BufferedReader(new FileReader(new File(input)));
			bw = new BufferedWriter(new FileWriter(new File(output)));
			
			//read until end of file is reached
			while ((current = br.read()) != -1) {
				
				if(current == 44) { //char code for ','
					count++;
					if(prev == '>') {
						bw.write(44);
					} else {
						bw.write(" ");
					}
				} else {
					bw.write(current);
				}
				prev = current;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (bw != null) {
				try {
					bw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
