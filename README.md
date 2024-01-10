# General Info
This is a project based on [Cugar](https://github.com/renespeck/Cugar) used in [workload-aware-rdf-partitioning](https://github.com/dice-group/workload-aware-rdf-partitioning).

It extends the Cugar to cluster a triple dataset based on its predicates using Markov-Clustering algorithm (PCM).

# Usage
## Configuration
`src/main/java/org/example/clustering/PathConstants.java` contains all paths which have to be configured to run the clustering.
## Running
Run `main()` function in the `PCM` class to perform clustering.
## Output Structure
The output consists of several partition files (the number is configurable) of triples and a measurements.csv file containing benchmark results.
