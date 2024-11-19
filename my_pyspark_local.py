import my_pyspark_local
from pyspark.sql import SparkSession

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("HetIONet RDD Analysis").getOrCreate()
    sc = spark.sparkContext

    # Load nodes.tsv and edges.tsv as RDDs
    nodes_rdd = spark.sparkContext.textFile("nodes.tsv")
    edges_rdd = spark.sparkContext.textFile("edges.tsv")

    # Query 1
    edges_rdd = edges_rdd.map(lambda line: tuple(line.split('\t')))
    metaedges_of_interest = {"CbG", "CdG", "CpD", "CtD", "CuG"}
    filtered_rdd = edges_rdd.filter(lambda x: x[1] in metaedges_of_interest)  # filter out all edges that don't have drug-gene or drug-disease relationships
    drug_gene_pairs_rdd = filtered_rdd.filter(lambda x: x[1] in {"CbG", "CdG", "CuG"}).map(lambda x: (x[0], x[2])) # (drug, gene) pair
    drug_disease_pairs_rdd = filtered_rdd.filter(lambda x: x[1] in {"CpD", "CtD"}).map(lambda x: (x[0], x[2])) # (drug, disease) pair
    drug_gene_count_rdd = drug_gene_pairs_rdd.distinct().map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b) # (drug, gene_count) pair
    drug_disease_count_rdd = drug_disease_pairs_rdd.distinct().map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b) # (drug, disease_count) pair
    joined_count_rdd = drug_gene_count_rdd.fullOuterJoin(drug_disease_count_rdd).map(lambda x: (x[0], (x[1][0] or 0, x[1][1] or 0))) # (drug, (gene_count, disease_count)) pair
    final_count_rdd = joined_count_rdd.map(lambda x: (x[0], x[1][0], x[1][1])) # (drug, gene_count, disease_count) tuple
    top_5_drugs_by_genes = final_count_rdd.takeOrdered(5, key=lambda x: -x[1]) # top 5 (drug, gene_count, disease_count) pairs sorted by gene_count
    print(top_5_drugs_by_genes)

    # Query 2
    metaedges_of_interest = {"CpD", "CtD"} 
    drug_disease_rdd = edges_rdd.filter(lambda x: x[1] in metaedges_of_interest) # filter out all edges that don't have drug-disease relationships
    disease_drug_pairs_rdd = drug_disease_rdd.map(lambda x: (x[2], x[0])) # (disease, drug) pair
    disease_drug_count_rdd = disease_drug_pairs_rdd.distinct().map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b) # (disease, drug association_count) pair
    drug_association_count_rdd = disease_drug_count_rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b) # (drug_association_count, number of diseases associated with drug association count) pair
    top_5_drug_association_count = drug_association_count_rdd.takeOrdered(5, key=lambda x: -x[1]) # top 5 (drug_association_count, number of diseases associated with drug association count) pairs sorted by number of diseases associated with drug association count
    print(top_5_drug_association_count)

    # Query 3
    nodes_rdd = nodes_rdd.map(lambda line: tuple(line.split('\t')))
    drug_nodes_rdd = nodes_rdd.filter(lambda x: x[2] == 'Compound') # filter out all nodes that aren't compounds
    drug_name_rdd = drug_nodes_rdd.map(lambda x: (x[0], x[1])) # (id, name) pair
    top_5_drugs_by_genes_rdd = sc.parallelize(top_5_drugs_by_genes) # result from Query 1
    top_5_drug_names_rdd = top_5_drugs_by_genes_rdd.join(drug_name_rdd) # (id, (gene_count, name)) pair
    top_5_drug_names = top_5_drug_names_rdd.map(lambda x: (x[1][1], x[1][0])).takeOrdered(5, key=lambda x: -x[1]) # top 5 (name, gene_count) pairs sorted by gene_count
    print(top_5_drug_names)

if __name__ == '__main__':
    main()
