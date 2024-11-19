import csv
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['hetionet']

def import_nodes():
    with open('nodes.tsv', 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        nodes = list(reader)
        if nodes:
            db.nodes.insert_many(nodes)
            print(f"Imported {len(nodes)} nodes")

def import_edges():
    with open('edges.tsv', 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        edges = list(reader)
        if edges:
            db.edges.insert_many(edges)
            print(f"Imported {len(edges)} edges")

def create_indexes():
    db.nodes.create_index('Id')
    db.edges.create_index([('source', 1), ('target', 1)])
    print("Created indexes")

if __name__ == "__main__":
    import_nodes()
    import_edges()
    create_indexes()
    print("Data import completed")