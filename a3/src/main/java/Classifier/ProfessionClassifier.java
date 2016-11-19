package Classifier;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import Classifier.StringIntegerList.StringInteger;

public class ProfessionClassifier {
	
	// A map <Lemma, LemmaInfo> where LemmaInfo contains the index and number of appearances.
	// Index can be listed as follows: {0, 1, 2, ...}
	private static HashMap<String, LemmaInfo> dictionary = new HashMap<String, LemmaInfo>();
	// A map <Person's name, Professions>
	private static HashMap<String, String[]> professionals = new HashMap<String, String[]>();
	private static int THRESHOLD = 10;
	
	public static void main (String[] args) throws IOException {
		Configuration conf = new Configuration();
		System.out.println("BLAAHH");
		loadProfessions(conf, args[0]);
		writeSeqFile(conf, args[1], args[2]);
		System.out.println("");
		System.out.println(dictionary.keySet().size());
		System.out.println("BLAAHH");
	}
	
	// Load the list of people and their professions for the training set.
	public static void loadProfessions(Configuration conf, String professionsFile) throws IOException {
//        Path professionsPath = new Path (professionsFile);
//        
//        // Load file contents into HashMap for use by the mapper.
//        FileSystem fs = FileSystem.get(conf);
        BufferedReader reader = null;
        
        int count = 0;
		try {
			reader =  new BufferedReader(new FileReader(professionsFile));

			String line;
			
			while ((line = reader.readLine()) != null) {
				count++;
				String[] tokens = line.split(" : ");
				String professional = tokens[0];
				String[] professions = tokens[1].split(", ");
				professionals.put(professional, professions);
			}
			
		} finally {
			System.out.println("Profession line count: " + count);
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {}
		}
	}
	
	public static void writeSeqFile(Configuration conf, String indexFile, String seqFile) throws IOException {

		createDictionary(indexFile);
		
		FileSystem fs = FileSystem.getLocal(conf);
		Path seqFilePath = new Path(seqFile);
		fs.delete(seqFilePath,false);
		
		SequenceFile.Writer writer = null;

		try {
			Option optPath = SequenceFile.Writer.file(seqFilePath);
		    Option optKey = SequenceFile.Writer.keyClass(Text.class);
		    Option optVal = SequenceFile.Writer.valueClass(VectorWritable.class);
		    writer = SequenceFile.createWriter(conf, optPath, optKey, optVal);
		    
			List<MahoutVector> mahout_vectors = vectorize(indexFile);
			System.out.println(" Mahout vector size: " + mahout_vectors.size());
			for (MahoutVector mahout_vector : mahout_vectors) {
				VectorWritable vectorWritable = new VectorWritable();
				vectorWritable.set(mahout_vector.getVector());
				writer.append(new Text("/" + mahout_vector.getClassifier() + "/"), vectorWritable);
			}
			
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (IOException e) {}
		}
	}
	
	// Parse through ARTICLE_LEMMA_INDEX and create dictionary of all lemmas appearing in corpus.
	private static void createDictionary(String indexFile) throws IOException {
		
		BufferedReader reader = null;
		int lines = 0;
		try {
			String line;
			reader =  new BufferedReader(new FileReader(indexFile));
			// Line consists of: Person name<\t>StringIntegerList
			while ((line = reader.readLine()) != null) {
				lines++;
				String[] entry = line.split("\t");
				String person = entry[0];
				
				if (professionals.containsKey(person) && entry.length > 1) {
					StringIntegerList lemma_freqs = new StringIntegerList();
					lemma_freqs.readFromString(line.split("\t")[1]);
				
					for (StringInteger lemma_freq : lemma_freqs.getIndices()) {
						String lemma = lemma_freq.getString();
						int count = lemma_freq.getValue();
						if (!dictionary.containsKey(lemma)) {
							LemmaInfo lemma_info = new LemmaInfo();
							lemma_info.setIndex(dictionary.keySet().size());
							lemma_info.addCount(count);
							dictionary.put(lemma, lemma_info);
						} else {
							// Only update the number of appearances of the lemma.
							dictionary.get(lemma).addCount(count);
						}
							
					}
				}

			}
		} finally {
			System.out.println("Index file line count: " + lines);
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {}
		}
	}
	
	private static List<MahoutVector> vectorize(String indexFile) throws IOException {
		
		// List of vectors of a profession and lemmas associated with the profession.
		List<MahoutVector> mahout_vectors = new ArrayList<MahoutVector>();
		BufferedReader reader = null;
//		FSDataInputStream stream;

		int lines = 0;
		try {
			// Line consists of: Person name<\t>StringIntegerList
			Configuration conf = new Configuration();
			Path path = new Path(indexFile);
			FileSystem fs = FileSystem.get(conf);
//			stream = fs.open(path);
            reader=new BufferedReader(new InputStreamReader(fs.open(path)));

			String[] entry;
			String person;
			String line = "";
			while (line != null) {
				try {
					line = reader.readLine();
					
					lines++;
					entry = line.split("\t");
					person = entry[0];
					if (lines % 1000 == 0) {
						System.out.println("Line number: " + lines);
					}
					if (professionals.containsKey(person) && entry.length > 1) {
						Vector vector = new RandomAccessSparseVector(dictionary.keySet().size() -1, line.length() - 1);
						
						StringIntegerList lemma_freqs = new StringIntegerList();
						lemma_freqs.readFromString(entry[1]);
						
						// StringInteger <lemma, count>
						for (StringInteger lemma_freq: lemma_freqs.getIndices()) {
							LemmaInfo lemma_info = dictionary.get(lemma_freq.getString());
							if (lemma_info.getCount() > THRESHOLD) {
								// Set element at index specified in dictionary paired with TFIDF
								vector.set(lemma_info.getIndex(), lemma_info.getCount());
							}
						}
										
						// Create a Mahout vector for each profession assigned to person.
							MahoutVector mahout_vector = new MahoutVector();
							mahout_vector.setClassifier(professionals.get(person)[0]);
							mahout_vector.setVector(vector);
							mahout_vectors.add(mahout_vector);
						
				
					}
				} catch (OutOfMemoryError e) {
					line = "";
					System.out.println("Out of memory at " + lines);
				}
			}		
		} finally {
			System.out.println("Index file line count for vectorize: " + lines);
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {}
		}
		
		return mahout_vectors;
		
	}
	
}
