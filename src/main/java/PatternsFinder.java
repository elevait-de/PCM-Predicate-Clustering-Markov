import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;

/**
 * Each pattern is a string which can be used for filtering triples in a sparql query.
 * like <http://en.wikipedia.org/wiki/Cloud_computing> ?p ?o
 * it should have three parts so that its predicate can be recognized.
 */
public class PatternsFinder {

	public static void main(String[] args) {
		System.out.println("Finding Pattterns ...");
		
		String query = "PREFIX  owl:  <http://www.w3.org/2002/07/owl#> SELECT  * WHERE { ?s owl:sameAs ?o .}";

		Set<String> patterns = getTriplePatterns(query);
		for(String pattern : patterns)
			System.out.println(pattern);
		
		System.out.println("Terminated Successfully");
		
	}

	public static Set<String> getTriplePatterns(String query) {

		Query q = QueryFactory.create(query);
		
		String patt = q.getQueryPattern().toString();
		
		String[] firstPatterns = patt.split("\\{");
		
		String [] patterns = refinePattern(firstPatterns);
		 
		Set<String> finalPatterns = new HashSet<String>();


		for(String pattern : patterns) {
			String temp = pattern.trim();
			
			if(temp.length() > 1) {	
				if(pattern.contains("\n")) {
					String[] nestedPatterns = pattern.split("\n");
					for(int i=0; i<nestedPatterns.length; i++) {
						nestedPatterns[i] = nestedPatterns[i].trim();
						
						
						if(nestedPatterns[i].length()>1 && ! nestedPatterns[i].contains(";")) {
							finalPatterns.add(nestedPatterns[i]);							
//							System.out.println(nestedPatterns[i]);
						}
						else if(nestedPatterns[i].length()>1 && nestedPatterns[i].contains(";")) {
							String patternSubject;
							if (nestedPatterns[0].trim().contains(" ")) {
								patternSubject = nestedPatterns[0].trim().split(" ")[0];
							} else {
								patternSubject = nestedPatterns[0];
							}
							nestedPatterns[i] = nestedPatterns[i].replace(";", "");
							finalPatterns.add(nestedPatterns[i]);
							nestedPatterns[i+1] = patternSubject.concat(" " +nestedPatterns[i+1].trim());
//							System.out.println(nestedPatterns[i]);
						}

					}
				}
				else
					finalPatterns.add(temp);

			}
		}
		
		
		
		

		String globalSubject = getGlobalSubject(finalPatterns);
		
		Set<String> result = makeFinalPatterns(globalSubject, finalPatterns);
		
		return result;
	}
	
	
	
	
	private static Set<String> makeFinalPatterns(String globalSubject, Set<String> finalPatterns) {
		List<String> result = new ArrayList<String>();

		for(String pattern : finalPatterns) {
			String[] nodes = pattern.split(" ");
			int size = 0;
			
			for(int i=0; i<nodes.length; i++) {
				if(nodes[i].trim().length()>0)
					size = size+1;
			}
			
			if(size == 2) {
				pattern = globalSubject.concat(" " +pattern);
				result.add(pattern);
			}
			if(size == 3) {
				result.add(pattern);
			}
		}
		
		Set<String> finalResult = new HashSet<String>();

		for(int i=0; i<result.size(); i++) {
//			System.out.println(result.get(i));
			String patt = replaceVariables(result.get(i));
			finalResult.add(patt);
		}
		
			
		return finalResult;
	}



	private static String replaceVariables(String input) {
		input = input.trim();
		input = input.replaceAll(" +", " ");
		String[] resultSet = input.split(" ");
		List<String> pattern = new ArrayList<String>();

		String finalPattern = "";
		
		if(resultSet.length == 3) {
			
			for(int i=0; i<resultSet.length; i++) {
				resultSet[i] = resultSet[i].trim();
				
				if(resultSet[i].length()>0) {
					pattern.add(resultSet[i]);
				}
			}
			
			
			//Changing Subject
			
			if(pattern.get(0).contains("?")) {
				pattern.remove(0);
				pattern.add(0, "?s");

			}
			
			else {
				String s = pattern.get(0).trim();
				pattern.remove(0);
				pattern.add(0, s);			
			}

			//Changing Predicate

			if(pattern.get(1).contains("?")) {
				pattern.remove(1);
				pattern.add(1, "?p");

			}
			
			else if(pattern.get(1).equals("a")) {
				String p = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
				pattern.remove(1);
				pattern.add(1, p);			
			}
			else {
				String p = pattern.get(1).trim();
				pattern.remove(1);
				pattern.add(1, p);			
			}
			

			//Changing Object

			if(pattern.get(2).contains("?")) {
				pattern.remove(2);
				pattern.add(2, "?o");

			}
			
			else {
				String o = pattern.get(2).trim();
				pattern.remove(2);
				pattern.add(2, o);			
			}
			
			finalPattern = pattern.get(0).concat(" "+pattern.get(1)).concat(" "+pattern.get(2));
		}

		

		
		return finalPattern;
	}

	private static String getGlobalSubject(Set<String> finalPatterns) {
		String globalSubject = "";
		for(String pattern : finalPatterns) {
			String[] nodes = pattern.split(" ");
			int size = 0;
			
			for(int i=0; i<nodes.length; i++) {
				if(nodes[i].trim().length()>0)
					size = size+1;
			}
			
			if(size == 1)
				globalSubject = pattern;
		}
		return globalSubject;
	}
	
	
	
	private static String[] refinePattern(String[] firstPatterns) {
		String [] patterns = new String[firstPatterns.length];
		for(int i = 0; i<patterns.length; i++) {
			patterns[i] = firstPatterns[i].replaceAll("}", "");
			String tempPattern = patterns[i];
			
			for(int j=0; j<10; j++)
				tempPattern = seperateKeywords(tempPattern);
			
			patterns[i] = tempPattern; 
		}
		return patterns;
	}

	private static String seperateKeywords(String input) {
		String pattern = input;
		if(input.contains("UNION")) {
			pattern = input.split("UNION")[0];
		}
		else if(input.contains("DISTINCT")) {
			pattern = input.split("DISTINCT")[0];
		}
		else if(input.contains("ORDER BY")) {
			pattern = input.split("ORDER BY")[0];	
		}
		else if(input.contains("REGEX")) {
			pattern = input.split("REGEX")[0];
		}
		else if(input.contains("LIMIT")) {
			pattern = input.split("LIMIT")[0];
		}
		else if(input.contains("OFFSET")) {
			pattern = input.split("OFFSET")[0];
		}
		else if(input.contains("OPTIONAL")) {
			pattern = input.split("OPTIONAL")[0];
		}
		else if(input.contains("FILTER")) {
			pattern = input.split("FILTER")[0];
		}
		else if(input.contains("GROUP BY")) {
			pattern = input.split("GROUP BY")[0];
		}
		
		return pattern;
	}
	
	
}
