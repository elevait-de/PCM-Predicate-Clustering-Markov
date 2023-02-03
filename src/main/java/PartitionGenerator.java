

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

/**
 * allocate some obtained clusters of triples to partitions
 */
public class PartitionGenerator {

	private static List<Integer> partitionFileSizes = new ArrayList<Integer>(); 
	private static List<String> predicateEncodings = new ArrayList<String>(); 
	private static Map<String, String> predicateFileNames = new HashMap<String, String>();
 
	
	public static void main(String[] args) throws IOException {

		WeightGeneratorFromTestQueries.generateWeights();

		getPredicateEncodings();
		generatePartitionFiles();

		markovDistribution();

//		greedyDistribution(cluster);


		System.out.println("Terminated Successfully");

	}

	static void markovDistribution() throws IOException {
		MarkovClustering.main(new String[]{});

		List<String> orderedPredicateList = getPredicatesInCluster();
		Map<String, Integer> predicateToPartitionNr = assignPredicatesToPartitions(orderedPredicateList, PathConstants.TOTAL_PARTITIONS);

		try (BufferedReader reader = new BufferedReader(new FileReader(PathConstants.DATASET_PATH))) {

			List<BufferedWriter> partitionWriters = new ArrayList<>();
			for (int i = 0; i < PathConstants.TOTAL_PARTITIONS; i++) {
				partitionWriters.add(new BufferedWriter(new FileWriter(PathConstants.PARTITION_OUTPUT_FOLDER + "/Partition" + i)));
			}

			String line;
			int lineCounter = 0;
			while ((line = reader.readLine()) != null) {
				String[] triple = line.trim().split(" ");
				if (triple.length < 3 || !triple[1].startsWith("<")) {
					System.out.printf("Error splitting line: %s\n", line.trim());
					continue;
				}
				String predicate = triple[1].trim();
				int targetChunk;
				targetChunk = predicateToPartitionNr.getOrDefault(predicate, PathConstants.TOTAL_PARTITIONS - 1);

				partitionWriters.get(targetChunk).write(line.trim());
				partitionWriters.get(targetChunk).newLine();
				lineCounter++;
			}

			// clean up
			partitionWriters.forEach(bufferedWriter -> {
				try {
					bufferedWriter.flush();
					bufferedWriter.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});

			System.out.printf("Number of triples : %d\n", lineCounter);
			System.out.printf("Input data set : %s\n", PathConstants.DATASET_PATH);
			System.out.printf("Input query-log file : %s\n", PathConstants.QUERIES_PATH);
		}
	}

	private static Map<String, Integer> assignPredicatesToPartitions(List<String> orderedPredicateList, int numberOfGraphChunks) {
		Map<String, Integer> predicatesToPartitions = new HashMap<>();
		// one chunk is always left for the predicates not in the cluster
		int numberOfChunksLeftUnassigned = numberOfGraphChunks - 1;
		int currentPartitionNumber = 0;
		while (currentPartitionNumber < numberOfGraphChunks - 1 && !orderedPredicateList.isEmpty()) {
			int nrOfPredicatesToPartition = (int) Math.ceil(orderedPredicateList.size() / (float)(numberOfChunksLeftUnassigned));
			for (int i = 0; i < nrOfPredicatesToPartition; i++) {
				predicatesToPartitions.put(orderedPredicateList.get(i), currentPartitionNumber);
			}
			orderedPredicateList = orderedPredicateList.subList(nrOfPredicatesToPartition, orderedPredicateList.size());
			currentPartitionNumber++;
			numberOfChunksLeftUnassigned--;
		}
		return predicatesToPartitions;
	}

	private static List<String> getPredicatesInCluster() {
		HashMap<String, String> numberToPredicate = new HashMap<>();
		try (BufferedReader br = new BufferedReader(new FileReader(PathConstants.PREDICATE_FILE))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] lineWords = line.trim().split(" ");
				if (lineWords.length == 3) {
					numberToPredicate.put(lineWords[0], lineWords[2]);
				}
			}
		} catch (IOException e) {
			System.out.printf("Error reading the predicates list: %s%n\n", e.getMessage());
			e.printStackTrace();
		}

		LinkedList<String> orderedPredicateList = new LinkedList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(PathConstants.CLUSTER_FILE))) {
			String line = br.readLine();
			line = StringUtils.substringAfter(line, "[");
			line = StringUtils.substringBeforeLast(line, "]");
			String[] predicateNumbers = line.split(", ");
			for (String number :  predicateNumbers) {
				String predicate = numberToPredicate.get(number);
				if (predicate == null) {
					throw new RuntimeException("Failed to resolve the predicate for number \"" + number + "\"");
				}
				orderedPredicateList.add(predicate);
			}
		} catch (IOException e) {
			System.out.printf("Error reading the cluster list: %s", e.getMessage());
			e.printStackTrace();
		}
		return orderedPredicateList;
	}

	private static void greedyDistribution() throws IOException {
		Set<String> cluster = getClusters(PathConstants.CLUSTER_FILE);
		PredicateFileGenerator.generatePredicateFiles(PathConstants.DATASET_PATH, PathConstants.PREDICATE_FILES);
		getPredicateFileNames();

		if(cluster.size() == 1) {
			splitClusterIntoPartitions(cluster, PathConstants.PREDICATE_FILE, PathConstants.PARTITION_OUTPUT_FOLDER, PathConstants.TOTAL_PARTITIONS);
		}
		else if(cluster.size() > 1) {
			distributeClustersIntoPartitions(cluster, PathConstants.PREDICATE_FILE, PathConstants.PARTITION_OUTPUT_FOLDER, PathConstants.TOTAL_PARTITIONS);
		}

		distributeRemainingTriples();
	}

	private static void distributeClustersIntoPartitions(Set<String> clusters, String predicateFile, String outputFolder, int totalPartitions) throws IOException {
		for(String c : clusters) {
			Set<String>cluster = new HashSet<String>();
			cluster.add(c);
			splitClusterIntoPartitions(cluster,predicateFile, outputFolder, totalPartitions);

		}
	}

	private static void splitClusterIntoPartitions(Set<String> cluster, String predicateFile, String outputFolder, int totalPartitions) throws IOException {
        String predicates[] = cluster.iterator().next().split(",");
        int currentPartition = 0;
        int currentPartitionSize = 0;
        for(int i=0; i<predicates.length; i++) {
        	predicates[i] = predicates[i].replace("[", "").replace("]", "").trim();
        	String predicate = predicateEncodings.get(Integer.parseInt(predicates[i]));
        	Set<String> tripleSet = getTriplesFromDataset(predicate);
        	predicateFileNames.remove(predicate);
        	
        	if(currentPartition == totalPartitions-1)
        		currentPartition = 0;
        	
        	if(currentPartitionSize < PathConstants.PARTITION_SIZE) {
        		currentPartitionSize = currentPartitionSize + tripleSet.size();
        		String fileName = outputFolder.concat("Partition") +currentPartition;
        		appendPartition(fileName, tripleSet);
        	}
        	else if(currentPartitionSize >= PathConstants.PARTITION_SIZE) {
        		currentPartition = currentPartition + 1;
        		currentPartitionSize = tripleSet.size();
        		String fileName = outputFolder.concat("Partition") +currentPartition;
        		appendPartition(fileName, tripleSet);        	}
        }
	}

	private static void distributeRemainingTriples() throws IOException {
		for(String predicate : predicateFileNames.keySet()) {
        	Set<String> tripleSet = getTriplesFromDataset(predicate);
        	String smallestPartition = getSmallestFile();
    		appendPartition(smallestPartition, tripleSet);


		}
	}
	
	private static String getSmallestFile() {
		File file = new File(PathConstants.PARTITION_OUTPUT_FOLDER);
		File[] files = file.listFiles();
		String fMaxName = "";
		String fMinName = "";
		long fMaxLength = 0;
		long fMinLength = Long.MAX_VALUE;
		
		
		for (File f : files) {
			if (f.isFile()) {
				if (f.length() > fMaxLength) {
					fMaxLength = f.length();
					fMaxName = f.getAbsolutePath();
				}
				if (f.length() < fMinLength) {
					fMinLength = f.length();
					fMinName = f.getAbsolutePath();
				}
			} 
		}
		return fMinName;
	}

	
	private static void appendPartition(String fileName, Set<String> tripleSet) throws IOException {
		FileWriter fw = new FileWriter(fileName, true);
	    BufferedWriter writer = new BufferedWriter(fw); 
	    for(String triple : tripleSet) {
		    writer.write(triple);
		    writer.newLine();  
	    }
	    writer.close();
	}




	private static void getPredicateFileNames() {
		File folder = new File(PathConstants.PREDICATE_FILES);
		File[] files = folder.listFiles(); 

		for (File file:files) {
			String predicate = "";
			try {
				BufferedReader br = new BufferedReader(new FileReader(file));			
				String triple = "";
				while ((triple = br.readLine()) != null){
					if(triple.length() > 0) {
						predicate = "<"+getPredicate(triple)+">";
						break;
					}
				}
				br.close();
			}catch (IOException e){
				e.printStackTrace();
			}	
			predicateFileNames.put(predicate, file.toString());
		}
	}

	static String getPredicate(String triple) {
		String predicate = "";
		try {
	    	final Model model = ModelFactory.createDefaultModel();
	    	RDFDataMgr.read(model, new ByteArrayInputStream(triple.getBytes(StandardCharsets.UTF_8)), Lang.NTRIPLES);
	    	final StmtIterator listStatements = model.listStatements();
	    	final Statement statement = listStatements.next();
	    	predicate = statement.getPredicate().toString();
	    } catch(Exception e) {  }
		return predicate;
	}

	
	
	private static Set<String> getTriplesFromDataset(String predicate) {
		Set<String> triples = new HashSet<String>();
		try{
			BufferedReader br = new BufferedReader(new FileReader(predicateFileNames.get(predicate)));			
			String s;
			while ((s = br.readLine()) != null){
				if(s.length() > 0) {
					triples.add(s);
				}
			}
			br.close();
		}catch (Exception e){
		}
		return triples;
	}

	
	
	
	static void generatePartitionFiles() {
		try {
			if (Files.isDirectory(Path.of(PathConstants.PARTITION_OUTPUT_FOLDER))) {
				Files.list(Path.of(PathConstants.PARTITION_OUTPUT_FOLDER)).forEach(path -> {
					try {
						if (path.getFileName().toString().startsWith("Partition")) {
							Files.delete(path);
						}
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				});
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}


		for(int i = 0; i< PathConstants.TOTAL_PARTITIONS; i++) {
		  try {
		         File file = new File(PathConstants.PARTITION_OUTPUT_FOLDER +"/Partition"+i);
		         file.createNewFile();
		         partitionFileSizes.add(i, 0);
	      } catch(Exception e) {
		         e.printStackTrace();
	      }
		}
	}
	

	
	static void getPredicateEncodings() {
		try (BufferedReader br = new BufferedReader(new FileReader(PathConstants.PREDICATE_FILE));) {
			String s;
			while ((s = br.readLine()) != null){
				if(s.length() > 0) {
					String line[] = s.split("=");
					int index = Integer.parseInt(line[0].trim());
					String predicate = line[1].trim();
					predicateEncodings.add(index, predicate);
				}
			}
		}catch (IOException e){
			e.printStackTrace();
		}
	}

	private static Set<String> getClusters(String clusterFile) {
		Set<String> cluster = new HashSet<String>();
		try{
			BufferedReader br = new BufferedReader(new FileReader(clusterFile));			
			String s;
			while ((s = br.readLine()) != null){
				if(s.length() > 0) 
					cluster.add(s);
			}
			br.close();
		}catch (IOException e){
			e.printStackTrace();
		}
		return cluster;
	}

}
