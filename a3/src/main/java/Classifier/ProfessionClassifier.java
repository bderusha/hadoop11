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

	// Arguments should be {Professions file, ARTICLE_LEMMA_INDEX file, Output file}
	public static void main (String[] args) throws IOException {
		Configuration conf = new Configuration();
		loadProfessions(conf, args[0]);
		writeSeqFile(conf, args[1], args[2]);
	}
	
	// Load the list of people and their professions for the training set.
	public static void loadProfessions(Configuration conf, String professionsFile) throws IOException {
	        Path professionsPath = new Path (professionsFile);
        
        	// Load file contents into HashMap for use by the mapper.
        	FileSystem fs = FileSystem.get(conf);
        	BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(professionsPath)));
        
		try {
			String line;
			
			while ((line = reader.readLine()) != null) {
				String[] tokens = line.split(" : ");
				String professional = tokens[0];
				String[] professions = tokens[1].split(", ");
				professionals.put(professional, professions);
			}
			
		} finally {
			reader.close();
		}
	}
	
	public static void writeSeqFile(Configuration conf, String indexFile, String seqFile) throws IOException {

		createDictionary(indexFile);
		
		FileSystem fs = FileSystem.getLocal(conf);
		Path seqFilePath = new Path(seqFile);
		
		fs.delete(seqFilePath,false);
		
		// Deprecated?
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, seqFilePath, Text.class, VectorWritable.class);

		try {
			List<MahoutVector> mahout_vectors = vectorize(indexFile);
			
			for (MahoutVector mahout_vector : mahout_vectors) {
				VectorWritable vectorWritable = new VectorWritable();
				vectorWritable.set(mahout_vector.getVector());
				writer.append(new Text("/" + mahout_vector.getClassifier() + "/"), vectorWritable);
			}
			
		} finally {
			writer.close();
		}
	}
	
	// Parse through ARTICLE_LEMMA_INDEX and create dictionary of all lemmas appearing in corpus.
	private static void createDictionary(String indexFile) throws IOException {
		
		BufferedReader reader = new BufferedReader(new FileReader(indexFile));
		String line;
		
		try {
			// Line consists of: Person name<\t>StringIntegerList
			while ((line = reader.readLine()) != null) {
				
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
		} finally {
			reader.close();
		}
	}
	
	private static List<MahoutVector> vectorize(String indexFile) throws IOException {
		
		// List of vectors of a profession and lemmas associated with the profession.
		List<MahoutVector> mahout_vectors = new ArrayList<MahoutVector>();
		BufferedReader reader = new BufferedReader(new FileReader(indexFile));
		String line;
		
		try {
			// Line consists of: Person name<\t>StringIntegerList
			while ((line = reader.readLine()) != null) {
		
				String[] entry = line.split("\t");
				String person = entry[0];
				
				if (professionals.containsKey(person)) {
					Vector vector = new RandomAccessSparseVector(line.length() - 1, line.length() - 1);
					
					StringIntegerList lemma_freqs = new StringIntegerList();
					lemma_freqs.readFromString(entry[1]);
					
					// StringInteger <lemma, count>
					for (StringInteger lemma_freq: lemma_freqs.getIndices()) {
						LemmaInfo lemma_info = dictionary.get(lemma_freq.getString());
						// Set element at index specified in dictionary paired with TFIDF
						vector.set(lemma_info.getIndex(), calculateTFIDF(lemma_freq.getString()));
					}
									
					// Create a Mahout vector for each profession assigned to person.
					for (int i = 0; i < professionals.get(person).length; i++) {
						MahoutVector mahout_vector = new MahoutVector();
						mahout_vector.setClassifier(professionals.get(person)[i]);
						mahout_vector.setVector(vector);
						mahout_vectors.add(mahout_vector);
					}
			
				}
			}		
		} finally {
			reader.close();
			return mahout_vectors;
		}
		
	}
	
	// Naive TFIDF.
	private static double calculateTFIDF(String lemma) {
		return dictionary.get(lemma).getCount();
	}
}
