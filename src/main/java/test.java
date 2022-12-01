import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.graph.Triple;

import java.util.Set;

public class test {
    public static void main(String[] args) {
        System.out.println("Finding test Pattterns ...");
////        String qs = "<http://lsq.aksw.org/res/q-adf4039d> <http://lsq.aksw.org/vocab#text> "PREFIX  semw: <http://data.semanticweb.org/> PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX  swrc: <http://swrc.ontoware.org/ontology#> PREFIX  foaf: <http://xmlns.com/foaf/0.1/> PREFIX  dc:   <http://purl.org/dc/elements/1.1/>  SELECT DISTINCT  ?abstract ?keyword ?author_name WHERE   {   { <http://data.semanticweb.org/conference/eswc/2014/paper/phd-symposium/13>                   swrc:author  ?author       }     UNION       { <http://data.semanticweb.org/conference/eswc/2014/paper/phd-symposium/13>                   foaf:maker  ?author       }     ?author  foaf:name  ?author_name     OPTIONAL       { <http://data.semanticweb.org/conference/eswc/2014/paper/phd-symposium/13>                   swrc:abstract  ?abstract       }     OPTIONAL       {   { <http://data.semanticweb.org/conference/eswc/2014/paper/phd-symposium/13>                       swrc:keywords  ?keyword           }         UNION           { <http://data.semanticweb.org/conference/eswc/2014/paper/phd-symposium/13>                       dc:subject  ?keyword           }         UNION           { <http://data.semanticweb.org/conference/eswc/2014/paper/phd-symposium/13>                       <http://data.semanticweb.org/ns/swc/ontology#hasTopic>  ?topic .             ?topic    rdfs:label            ?keyword           }       }   } " .";
        String qs = "<http://lsq.aksw.org/res/q-63d296dc> <http://lsq.aksw.org/vocab#text> \"SELECT  (( 1 + 2 ) AS ?bar) WHERE   { ?s  ?p  ?o } LIMIT   1 \" .";

//        String qs = "SELECT * { ?s ?p <_:ABC>}";
        Query query = QueryFactory.create(qs);
        Element qp = query.getQueryPattern();
        Element el = ((ElementGroup)qp).get(0);
        ElementPathBlock epb = (ElementPathBlock)el;
        TriplePath tp = epb.getPattern().get(0);
        Triple t = tp.asTriple();
//        assertEquals("ABC", t.getObject().getBlankNodeLabel());


        System.out.println("Terminated Successfully");

    }

}
