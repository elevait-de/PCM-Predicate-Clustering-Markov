import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;

public class WeightGeneratorFromTestQueries {

	public static void main(String[] args) throws IOException {
//		String inputFile =  "/home/asal/IdeaProjects/PCG3/src/main/resources/test-line-query.ttl";
		String inputFile =  "/home/asal/Documents/3DFed/Queries/swdf-300-bgp-queries.txt";
		Set<String> queries = readQueryFile(inputFile);
 		System.out.println("Total Input queries are: " +queries.size());
		
		Map<Integer, Set<String>> patterns = getQueryPatterns(queries);
		System.out.println("Total queries with patterns are: " +patterns.keySet().size());
		
		Map<String, Integer> graphWeight = getWeight(patterns);

//		for(String key : graphWeight.keySet())
//			System.out.println(key +"  ===>  " +graphWeight.get(key));

		generateOutputFile(graphWeight, inputFile);
				
		System.out.println("Terminated Successfully");
	}

		
	
	private static void generateOutputFile(Map<String, Integer> graphWeight, String inputFile) throws IOException {
		File path = new File(inputFile);
		List<String> takenPredicates = new ArrayList<String>();
		String folder = path.getParent();
		System.out.println("Output files are generated at: " +folder);
		FileWriter weightFile = new FileWriter(folder.concat("/graphweight.txt")); 
		FileWriter predicateEncodingFile = new FileWriter(folder.concat("/predicateEncoding.txt")); 
		for(String key : graphWeight.keySet()) {
			String pred1 = key.split(" ")[0].trim();
			String pred2 = key.split(" ")[1].trim();
			int ser1 = 0;
			int ser2 = 0;
			if(!takenPredicates.contains(pred1)) {
				takenPredicates.add(pred1);
				ser1 = takenPredicates.indexOf(pred1);
				predicateEncodingFile.write(ser1 +" = " +pred1 +"\n");
				predicateEncodingFile.flush();
			}  else
					ser1 = takenPredicates.indexOf(pred1);

			if(!takenPredicates.contains(pred2)) {
				takenPredicates.add(pred2);
				ser2 = takenPredicates.indexOf(pred2);
				predicateEncodingFile.write(ser2 +" = " +pred2 +"\n");
				predicateEncodingFile.flush();
			}	else
					ser2 = takenPredicates.indexOf(pred2);

			weightFile.write(ser1 +" " +ser2 +" " +graphWeight.get(key) +"\n"); 
			weightFile.flush();
		}

		weightFile.close();
		predicateEncodingFile.close();
	}


	private static Map<String, Integer> getWeight(Map<Integer, Set<String>> patterns) {
		Map<String, Integer> graphWeight = new HashMap<String, Integer>();

		System.err.println(patterns.get(0));
		for(int i=0; i<patterns.keySet().size(); i++) {
			getPredicateWeightInPattern(graphWeight, patterns.get(i));
		}
		
		
		return graphWeight;
	}


	private static Set<String> getPredicateWeightInPattern(Map<String, Integer> graphWeight, Set<String> pattern) {
		List<String> predicates = new ArrayList<String>();
		Set<String> predicateWeight = new HashSet<String>();
		for(String pat : pattern) {
			if (pat.contains(" ")) {
				String predicate = pat.split(" ")[1].trim();
				if (!predicates.contains(predicate)) {
					predicates.add(predicate);
				}
			}
		}
		
		for(int i=0; i<predicates.size()-1; i++) {
			for(int j=1; j<predicates.size(); j++) {
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



	private static boolean isQuery(String line) {
		boolean result = false;
		try{
			Query q = QueryFactory.create(line);
			result = true;
		}catch(Exception e) {
			result = false;
		}
		return result;
	}


	private static Map<Integer, Set<String>> getQueryPatterns(Set<String> queries) {
		Map<Integer, Set<String>> patterns = new HashMap<Integer, Set<String>>();
		int index = 0;
		for(String query : queries) {
			try {
				Set<String> qryPatterns = PatternsFinder.getTriplePatterns(query);
				patterns.put(index, qryPatterns);
				index = index+1;
			}catch(Exception e) {
			}
		}
		return patterns;
	}


	private static Set<String> readQueryFile(String inputFile) {
		Set<String> fileData = new HashSet<String>();
		try{

			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			String line = br.readLine();
			if (line.startsWith("#-")) {
				// case multiline
				String multiline = "";
				while ((line = br.readLine()) != null){
					if (!line.startsWith("#-")) {
						multiline = multiline.concat(" ").concat(line);
					} else {
						if(isQuery(multiline)) {
							fileData.add(multiline);
						}
						multiline = "";
					}
				}
				if(isQuery(multiline)) {
					fileData.add(multiline);
				}
			} else {
				// case single line
				do {
					if(isQuery(line)) {
						fileData.add(line);
					}
				}
				while ((line = br.readLine()) != null);
			}
			br.close();
		}catch(Exception e) {
			System.out.println("file not found");
		}
		return fileData;
	}

}
