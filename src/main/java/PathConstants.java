public class PathConstants {

    static final String PATH = "src/main/resources/";
    public static final String QUERIES_PATH = "src/main/resources/input/queries/swdf-300-bgp-queries.txt";
    public static final String DATASET_PATH = "src/main/resources/input/datasets/SWDF.nt";
    static final String CLUSTER_FILE = PATH +"swdf-clusters.txt";
    static final String PREDICATE_FILE = PATH +"predicateEncoding.txt";
    static final String PREDICATE_FILES = PATH +"predicate-based/";
    static final String PARTITION_OUTPUT_FOLDER = PATH +"final-partitions/";
    static final String GRAPH_WEIGHT_FILE = PATH +"/graphweight.txt";
    static final int TOTAL_PARTITIONS = 10;

    //dbpedia dataset has total 232536510 triples
    //swdf dataset has total 304583 triples
    static final int PARTITION_SIZE = 304583/ TOTAL_PARTITIONS;  // This is total number of triples in the dataset
}
