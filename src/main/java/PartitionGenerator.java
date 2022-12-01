

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

public class PartitionGenerator {
	
	private static String path = "src/main/resources/";
	static String clusterFile = path+"swdf-clusters.txt";
	private static String predicateFile = path+"predicateEncoding.txt";
	private static  String predicatePartitions = path+"predicate-based/";
	private static String outputFolder = path+"final-partitions/";
	private static int totalPartitions = 10;
	private static int partitionSize = 304583/totalPartitions;  // This is total number of triples in the dataset
	//dbpedia dataset has total 232536510 triples
	//swdf dataset has total 304583 triples
	private static List<Integer> partitionFileSizes = new ArrayList<Integer>(); 
	private static List<String> predicateEncodings = new ArrayList<String>(); 
	private static Map<String, String> predicateFileNames = new HashMap<String, String>();
 
	
	public static void main(String[] args) throws IOException {


		Set<String> cluster = getClusters(clusterFile);
		getPredicateEncodings();
		generatePartitionFiles();
		getpredicateFileNames();
		
		if(cluster.size() == 1) {
			splitClusterIntoParatitions(cluster,predicateFile, outputFolder, totalPartitions);
		}
		else if(cluster.size() > 1) {
			distributeClustersIntoParatitions(cluster, predicateFile, outputFolder, totalPartitions);
		}
		
		distributeRemainingTriples();
		System.out.println("Terminated Successfully");

	}
	

	


	private static void distributeClustersIntoParatitions(Set<String> clusters, String predicateFile,  String outputFolder, int totalPartitions) throws IOException {
		for(String c : clusters) {
			Set<String>cluster = new HashSet<String>();
			cluster.add(c);
			splitClusterIntoParatitions(cluster,predicateFile, outputFolder, totalPartitions);

		}
	}

	private static void splitClusterIntoParatitions(Set<String> cluster, String predicateFile, String outputFolder, int totalPartitions) throws IOException {
        String predicates[] = cluster.iterator().next().split(",");
        int currentPartition = 0;
        int currentPartitionSize = 0;
        for(int i=0; i<predicates.length; i++) {
        	predicates[i] = predicates[i].replace("[", "").replace("]", "").trim();
        	String predicate = predicateEncodings.get(Integer.parseInt(predicates[i]));
        	Set<String> tripleSet = getTriplesFromDataset(predicate);
//        	System.out.println(predicateFileNames.keySet().contains(predicate));
        	predicateFileNames.remove(predicate);
        	
        	if(currentPartition == totalPartitions-1)
        		currentPartition = 0;
        	
        	if(currentPartitionSize < partitionSize) {
        		currentPartitionSize = currentPartitionSize + tripleSet.size();
        		String fileName = outputFolder.concat("Partition") +currentPartition;
        		appendPartition(fileName, tripleSet);
        	}
        	else if(currentPartitionSize >= partitionSize) {
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
		File file = new File(outputFolder);
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




	private static void getpredicateFileNames() {
		File folder = new File(predicatePartitions);
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

	private static String getPredicate(String triple) {
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

	
	
	
	private static void generatePartitionFiles() {
		for(int i=0; i<totalPartitions; i++) {
		  try {
		         File file = new File(outputFolder+"/Partition"+i);
		         file.createNewFile();
		         partitionFileSizes.add(i, 0);
	      } catch(Exception e) {
		         e.printStackTrace();
	      }
		}
	}
	

	
	private static void getPredicateEncodings() {
		try{
			BufferedReader br = new BufferedReader(new FileReader(predicateFile));			
			String s;
			while ((s = br.readLine()) != null){
				if(s.length() > 0) {
					String line[] = s.split("=");
					int index = Integer.parseInt(line[0].trim());
					String predicate = line[1].trim();
					predicateEncodings.add(index, predicate);
				}
			}
			br.close();
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
