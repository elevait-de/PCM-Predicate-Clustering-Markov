import java.util.ArrayList;
import java.util.List;

public class FindPredicates {

	public static void main(String[] args) {
		System.out.println("Making Query from Pattern ...");
		
		String p1 = "?resource  <http://xmlns.com/foaf/0.1/page>  <http://en.wikipedia.org/wiki/Microsoft>";
		String p2 = "?resource  <http://xmlns.com/foaf/0.1/depiction>  ?image";
		String p3 = "<http://en.wikipedia.org/wiki/Microsoft>  <http://xmlns.com/foaf/0.1/page>  <http://en.wikipedia.org/wiki/Insight_(E-mail_client)>";
		
		String predicate = getPredicate(p2);
		System.out.println(predicate);
		System.out.println("Terminated Successfully");
	}

	public static String getPredicate(String input) {

		String[] nodes = input.split(" ");
		List<String> pattern = new ArrayList<String>();

		for(int i = 0; i<nodes.length; i++) {
			if(nodes[i].length() > 1) {
				pattern.add(nodes[i]);
			}
		}
		
		String predicate = pattern.get(1);
		return predicate;
	}

}
