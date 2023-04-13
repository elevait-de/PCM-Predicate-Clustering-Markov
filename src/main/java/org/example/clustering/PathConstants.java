package org.example.clustering;

public class PathConstants {

    private PathConstants() {}

    static final String PATH = "/home/asal/Documents/3DFed/Elevait-Experimental-Result-3Aril23/";
    public static final String MEASUREMENT_PATH = PATH + "pcm/pcm-partition-files/fully-featured-queries/pcm-measurements.csv";
    public static final String QUERIES_PATH = PATH + "queryLogs/SWDF/train-querylog-csv-files/fully-featured-queries/sparql_2023-04-06_09-15-50Z-9.csv"; //swdf-300-bgp-queries";
    public static final String DATASET_PATH = PATH + "datasets/SWDF.nt";
    static final String CLUSTER_FILE = PATH +"pcm/clusters.txt";
    static final String PREDICATE_FILE = PATH +"pcm/predicateEncoding.txt";
    static final String PREDICATE_FILES = PATH +"pcm/predicate-based/";
    static final String PARTITION_OUTPUT_FOLDER = PATH +"pcm/pcm-partition-files/fully-featured-queries/sparql_2023-04-06_09-15-50Z-9"; //swdf-300-bgp-queries";
    static final String GRAPH_WEIGHT_FILE = PATH + "pcm/graphweight.txt";
    static final int TOTAL_PARTITIONS = 10;

    //dbpedia dataset has total 232536510 triples
    //swdf dataset has total 304583 triples
    static final long PARTITION_SIZE = 304583/ TOTAL_PARTITIONS;  // This is total number of triples in the dataset
}
