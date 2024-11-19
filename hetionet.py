import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from neo4j import GraphDatabase
from pymongo import MongoClient

class Neo4jLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def query_new_disease(self, disease_id):
        query = """
        MATCH (c:Compound)-[r1]->(g:Gene)
        MATCH (d:Disease {id: $diseaseID})-[:DlA]->(a:Anatomy)-[r2]->(g)
        WHERE NOT (c)-[:CtD]->(d)
        AND (
            (r1.metaedge = 'CuG' AND r2.metaedge = 'AdG')
            OR 
            (r1.metaedge = 'CdG' AND r2.metaedge = 'AuG')
        )
        RETURN DISTINCT c.name AS compound
        """
        with self.driver.session() as session:
            result = session.run(query, diseaseID=disease_id)
            compounds = [record["compound"] for record in result]
        return compounds

class MongoLoader:
    def __init__(self, uri):
        self.client = MongoClient(uri)
        self.db = self.client['hetionet']

    def close(self):
        self.client.close()

    def query_disease(self, disease_id):
        pipeline = [
            {"$match": {"id": disease_id, "kind": "Disease"}},
            {"$lookup": {
                "from": "edges",
                "let": {"disease_id": "$id"},
                "pipeline": [
                    {"$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$target", "$$disease_id"]},
                                {"$in": ["$metaedge", ["CtD", "CpD"]]}
                            ]
                        }
                    }},
                    {"$lookup": {
                        "from": "nodes",
                        "localField": "source",
                        "foreignField": "id",
                        "as": "drug"
                    }},
                    {"$unwind": "$drug"},
                    {"$group": {
                        "_id": "$metaedge",
                        "drugs": {"$addToSet": "$drug.name"}
                    }}
                ],
                "as": "drugs"
            }},
            {"$lookup": {
                "from": "edges",
                "let": {"disease_id": "$id"},
                "pipeline": [
                    {"$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$source", "$$disease_id"]},
                                {"$eq": ["$metaedge", "DaG"]}
                            ]
                        }
                    }},
                    {"$lookup": {
                        "from": "nodes",
                        "localField": "target",
                        "foreignField": "id",
                        "as": "gene"
                    }},
                    {"$unwind": "$gene"},
                    {"$group": {
                        "_id": None,
                        "genes": {"$addToSet": "$gene.name"}
                    }}
                ],
                "as": "genes"
            }},
            {"$lookup": {
                "from": "edges",
                "let": {"disease_id": "$id"},
                "pipeline": [
                    {"$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$source", "$$disease_id"]},
                                {"$eq": ["$metaedge", "DlA"]}
                            ]
                        }
                    }},
                    {"$lookup": {
                        "from": "nodes",
                        "localField": "target",
                        "foreignField": "id",
                        "as": "anatomy"
                    }},
                    {"$unwind": "$anatomy"},
                    {"$group": {
                        "_id": None,
                        "locations": {"$addToSet": "$anatomy.name"}
                    }}
                ],
                "as": "locations"
            }},
            {"$project": {
                "_id": 0,
                "disease_name": "$name",
                "treating_drugs": {
                    "$filter": {
                        "input": "$drugs",
                        "as": "drug",
                        "cond": {"$eq": ["$$drug._id", "CtD"]}
                    }
                },
                "palliative_drugs": {
                    "$filter": {
                        "input": "$drugs",
                        "as": "drug",
                        "cond": {"$eq": ["$$drug._id", "CpD"]}
                    }
                },
                "causing_genes": {"$arrayElemAt": ["$genes.genes", 0]},
                "disease_locations": {"$arrayElemAt": ["$locations.locations", 0]}
            }}
        ]
        result = list(self.db.nodes.aggregate(pipeline))
        return result[0] if result else None

class HetioNetGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("HetioNet Query System")
        self.root.geometry("800x600")

        self.neo4j_loader = Neo4jLoader("bolt://localhost:7687", "neo4j", "password")
        self.mongo_loader = MongoLoader("mongodb://localhost:27017/")

        self.create_widgets()

    def create_widgets(self):
        # Create a notebook (tabbed interface)
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Create tabs
        self.query_tab = ttk.Frame(self.notebook)
        self.new_disease_tab = ttk.Frame(self.notebook)

        self.notebook.add(self.query_tab, text="Query Disease")
        self.notebook.add(self.new_disease_tab, text="Query New Disease")

        # Query Disease Tab
        self.create_query_disease_widgets(self.query_tab)

        # Query New Disease Tab
        self.create_new_disease_widgets(self.new_disease_tab)

    def create_query_disease_widgets(self, parent):
        # Disease ID Entry
        ttk.Label(parent, text="Enter Disease ID:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.disease_id_entry = ttk.Entry(parent, width=30)
        self.disease_id_entry.grid(row=0, column=1, padx=5, pady=5)

        # Query Button
        ttk.Button(parent, text="Query Disease", command=self.query_disease).grid(row=1, column=0, columnspan=2, padx=5, pady=10)

        # Results Text Area
        self.result_text = scrolledtext.ScrolledText(parent, wrap=tk.WORD, width=70, height=20)
        self.result_text.grid(row=2, column=0, columnspan=2, padx=5, pady=5)

    def create_new_disease_widgets(self, parent):
        # Disease ID Entry
        ttk.Label(parent, text="Enter Disease ID:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.new_disease_id_entry = ttk.Entry(parent, width=30)
        self.new_disease_id_entry.grid(row=0, column=1, padx=5, pady=5)

        # Query Button
        ttk.Button(parent, text="Query New Disease", command=self.query_new_disease).grid(row=1, column=0, columnspan=2, padx=5, pady=10)

        # Results Text Area
        self.new_disease_result_text = scrolledtext.ScrolledText(parent, wrap=tk.WORD, width=70, height=20)
        self.new_disease_result_text.grid(row=2, column=0, columnspan=2, padx=5, pady=5)

    def query_disease(self):
        disease_id = self.disease_id_entry.get()
        if not disease_id:
            messagebox.showerror("Error", "Please enter a Disease ID")
            return

        result = self.mongo_loader.query_disease(disease_id)
        self.display_result(result)

    def query_new_disease(self):
        disease_id = self.new_disease_id_entry.get()
        if not disease_id:
            messagebox.showerror("Error", "Please enter a Disease ID")
            return

        compounds = self.neo4j_loader.query_new_disease(disease_id)
        self.display_compounds(compounds)

    def display_result(self, result):
        self.result_text.delete(1.0, tk.END)
        if result:
            disease_name = result.get('disease_name', 'Unknown')
            treating_drugs = result.get('treating_drugs', [])
            palliative_drugs = result.get('palliative_drugs', [])
            causing_genes = result.get('causing_genes', [])
            disease_locations = result.get('disease_locations', [])

            self.result_text.insert(tk.END, f"Disease Name: {disease_name}\n\n")
            
            self.result_text.insert(tk.END, "Treating Drugs:\n")
            for drug_group in treating_drugs:
                self.result_text.insert(tk.END, ", ".join(drug_group.get('drugs', [])) + "\n")
            self.result_text.insert(tk.END, "\n")

            self.result_text.insert(tk.END, "Palliative Drugs:\n")
            for drug_group in palliative_drugs:
                self.result_text.insert(tk.END, ", ".join(drug_group.get('drugs', [])) + "\n")
            self.result_text.insert(tk.END, "\n")

            self.result_text.insert(tk.END, "Causing Genes:\n")
            self.result_text.insert(tk.END, ", ".join(causing_genes) + "\n\n")

            self.result_text.insert(tk.END, "Disease Locations:\n")
            self.result_text.insert(tk.END, ", ".join(disease_locations) + "\n")
        else:
            self.result_text.insert(tk.END, "No results found for this disease ID.\n")

    def display_compounds(self, compounds):
        self.new_disease_result_text.delete(1.0, tk.END)
        if compounds:
            self.new_disease_result_text.insert(tk.END, "Compounds that can treat the new disease:\n\n")
            self.new_disease_result_text.insert(tk.END, ", ".join(compounds))
        else:
            self.new_disease_result_text.insert(tk.END, "No compounds found for this disease.\n")

    def close(self):
        self.neo4j_loader.close()
        self.mongo_loader.close()
        self.root.quit()

if __name__ == "__main__":
    root = tk.Tk()
    app = HetioNetGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.close)
    root.mainloop()