package org.example.clustering;

import java.io.*;
import java.util.*;

import com.opencsv.CSVReader;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;

/**
 * WeightGeneratorFromTestQueries extract a list of predicate co-occurrences
 * from a querying workload and model them as a weighted graph.
 * It takes a query workload 𝑄 = {𝑞 1, . . . , 𝑞𝑛 } of SPARQL queries as an input.
 * and creates a predicates co-occurrence list 𝐿 = {𝑒 1, . . . , 𝑒𝑚 }
 * where each entry 𝑒 =< 𝑝 1, 𝑝 2, 𝑐 >, with 𝑝 1 , 𝑝 2 are two different predicates
 * used in the triple patterns of SPARQL queries in the given workload,
 * and 𝑐 is the co-occurrence count, i.e. the number of queries in which
 * both 𝑝 1 and 𝑝 2 are co-occurred.
 * Finally, it models the list 𝐿 as a weighted graph, such that for a
 * given list entry 𝑒 =< 𝑝 1, 𝑝 2, 𝑐 >, it creates two nodes for 𝑝 1 and 𝑝 2
 * that are connected by a link with weight equalling 𝑐.
 * The corresponding weighted graph and Encoding of predicates will be written in two text files.
 */
public class WeightGeneratorFromTestQueries {

	public static void main(String[] args) throws IOException {
		generateWeights(PathConstants.QUERIES_PATH, PathConstants.GRAPH_WEIGHT_FILE, PathConstants.PREDICATE_FILE);
	}

	public static void generateWeights(String queryFile, String graphWeightFile, String predicateFile) throws IOException {
		long start = System.currentTimeMillis();
		WeightGeneratorFromTestQueries wgftq = new WeightGeneratorFromTestQueries();
		Set<String> queries = wgftq.readQueryFile(queryFile);
		System.out.println("Total Input queries are: " +queries.size());

		Map<Integer, Set<String>> patterns = wgftq.getQueryPatterns(queries);
		System.out.println("Total queries with patterns : " +patterns.keySet().size());

		Map<String, Integer> graphWeight = wgftq.getWeight(patterns);

		wgftq.generateOutputFile(graphWeight, graphWeightFile, predicateFile);

		System.out.printf("Weight generation time (ms): %d \n", System.currentTimeMillis() - start);
	}

	private void generateOutputFile(Map<String, Integer> graphWeight, String graphWeightFile, String predicateFile) throws IOException {
		List<String> takenPredicates = new ArrayList<>();

		try (FileWriter weightFile = new FileWriter(graphWeightFile);
			 FileWriter predicateEncodingFile = new FileWriter(predicateFile);) {

			for (String key : graphWeight.keySet()) {
				String pred1 = key.split(" ")[0].trim();
				String pred2 = key.split(" ")[1].trim();
				int ser1 = 0;
				int ser2 = 0;
				if (!takenPredicates.contains(pred1)) {
					takenPredicates.add(pred1);
					ser1 = takenPredicates.indexOf(pred1);
					predicateEncodingFile.write(ser1 + " = " + pred1 + "\n");
					predicateEncodingFile.flush();
				} else
					ser1 = takenPredicates.indexOf(pred1);

				if (!takenPredicates.contains(pred2)) {
					takenPredicates.add(pred2);
					ser2 = takenPredicates.indexOf(pred2);
					predicateEncodingFile.write(ser2 + " = " + pred2 + "\n");
					predicateEncodingFile.flush();
				} else
					ser2 = takenPredicates.indexOf(pred2);

				weightFile.write(ser1 + " " + ser2 + " " + graphWeight.get(key) + "\n");
				weightFile.flush();
			}
		}
	}


	private Map<String, Integer> getWeight(Map<Integer, Set<String>> patterns) {
		Map<String, Integer> graphWeight = new HashMap<>();
		for(int i=0; i<patterns.keySet().size(); i++) {
			getPredicateWeightInPattern(graphWeight, patterns.get(i));
		}
		
		
		return graphWeight;
	}


	private Set<String> getPredicateWeightInPattern(Map<String, Integer> graphWeight, Set<String> pattern) {
		List<String> predicates = new ArrayList<>();
		Set<String> predicateWeight = new HashSet<>();
		for(String pat : pattern) {
			if (pat.contains(" ")) {
				String predicate = pat.split(" ")[1].trim();
				if (!predicates.contains(predicate)) {
					predicates.add(predicate);
				}
			}
		}
		
		for(int i=0; i<predicates.size()-1; i++) {
			for(int j=i+1; j<predicates.size(); j++) {
				String val1 = predicates.get(i).concat(" ").concat(predicates.get(j));
				String val2 = predicates.get(j).concat(" ").concat(predicates.get(i));
				if(!val1.contains("?p") && !val1.equals(val2)) {
					if(!graphWeight.keySet().contains(val1) && !graphWeight.keySet().contains(val2))
						graphWeight.put(val1, 1);
					else if(graphWeight.keySet().contains(val1)) {
						int value = graphWeight.get(val1)+1;
						graphWeight.remove(val1);
						graphWeight.put(val1, value);
						predicateWeight.add(val1);
					}
					else if(graphWeight.keySet().contains(val2)) {
						int value = graphWeight.get(val2)+1;
						graphWeight.remove(val2);
						graphWeight.put(val2, value);
						predicateWeight.add(val2);
					}
				}
			}
		}
		return predicateWeight;
	}



	private boolean isQuery(String line) {
		boolean result = false;
		try{
			Query q = QueryFactory.create(line);
			result = true;
		}catch(Exception e) {
			result = false;
		}
		return result;
	}


	private Map<Integer, Set<String>> getQueryPatterns(Set<String> queries) {
		Map<Integer, Set<String>> patterns = new HashMap<>();
		int index = 0;
		for(String query : queries) {
			try {
				Set<String> qryPatterns = PatternsFinder.getTriplePatterns(query);
				patterns.put(index, qryPatterns);
				index = index+1;
			}catch(Exception e) {
				System.err.println("Error getting query pattern, with message: " + e.getMessage());
			}
		}
		return patterns;
	}

	private String stringArrayToSingleString(String[] input) {
		StringBuilder sb = new StringBuilder();
		for (Object obj : input)
			sb.append(obj.toString()).append("\n");
		return sb.substring(0, sb.length() - 1);
	}


	private Set<String> readQueryFile(String inputFile) {
		Set<String> fileData = new HashSet<>();

		try (CSVReader reader = new CSVReader(new BufferedReader(new FileReader(inputFile)))) {
			String[] queryLines;
			while ((queryLines = reader.readNext()) != null) {
				String queryText = stringArrayToSingleString(queryLines);
				if (isQuery(queryText)) {
					fileData.add(queryText);
				}
			}
		} catch (Exception e) {
			System.out.println("file not found");
		}
		return fileData;
	}

}
