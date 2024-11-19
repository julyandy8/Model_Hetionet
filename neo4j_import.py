from neo4j import GraphDatabase
import time

# Neo4j connection setup
class Neo4jLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def load_data(self):
        with self.driver.session() as session:
            # Load nodes using the new Cypher query
            print("Loading nodes from 'nodes.tsv'...")
            start_time = time.time()
            session.run(self.load_nodes_query())  # Run the load_nodes query directly
            elapsed_time = time.time() - start_time
            print(f"Finished loading nodes in {elapsed_time:.2f} seconds.")

            # Load relationships using the new Cypher query
            print("Loading relationships from 'edges.tsv'...")
            start_time = time.time()
            session.run(self.load_relationships_query())  # Run the load_relationships query directly
            elapsed_time = time.time() - start_time
            print(f"Finished loading relationships in {elapsed_time:.2f} seconds.")

    def load_nodes_query(self):  # Add 'self' as the first parameter
        return """
        LOAD CSV WITH HEADERS FROM 'file:///nodes.tsv' AS csvLine FIELDTERMINATOR '\t'
        CALL apoc.create.node([csvLine.kind], {id: csvLine.id, name: csvLine.name}) YIELD node
        RETURN node
        """
    
    def load_relationships_query(self):  # Add 'self' as the first parameter
        return """
        LOAD CSV WITH HEADERS FROM 'file:///edges.tsv' AS csvLine FIELDTERMINATOR '\t'
        CALL {
            WITH csvLine
            MATCH (source {id: csvLine.source}), (target {id: csvLine.target})
            CALL apoc.create.relationship(source, csvLine.metaedge, {metaedge: csvLine.metaedge}, target)
            YIELD rel
            RETURN rel
        } IN TRANSACTIONS OF 500 ROWS
        RETURN count(rel) AS relationshipsCreated
        """

def main():
    neo4j_loader = Neo4jLoader("neo4j://localhost:7687", "neo4j", "HetioNet")
    neo4j_loader.load_data()
    neo4j_loader.close()  # Don't forget to close the connection

    print("Database created successfully!")

if __name__ == "__main__":
    main()

