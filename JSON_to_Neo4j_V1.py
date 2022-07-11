#!/usr/bin/env python
# coding: utf-8

import json
import pdb

from tqdm import tqdm

# import numpy

from neo4j import GraphDatabase

# Here substitute with your uri, user and pwd, if necessary

URI = "bolt://localhost:7687"

USER = "neo4j"

PASSWORD = "1234"

# Here change filename if necessary

import glob

FILENAMES = ["PanGenome/Acinetobacter_nosocomialis/pangenomeGraph.json","PanGenome/Acinetobacter_nosocomialis/pangenomeGraph.json"]


def execute(driver, query):
    """Execute a query."""

    with driver.session() as session:
        if len(query) > 0:
            result = session.run(query)

            return result


def load_data():
    """Load the dataset from json."""

    driver = GraphDatabase.driver(

        URI, auth=(USER, PASSWORD))

    # clean_query = "MATCH (n) DETACH DELETE n"
    #
    # execute(driver, clean_query)
    json_data = {"graph": {"edges": [], "nodes": [], "node_types": {}}}
    for path in FILENAMES:
        print(path)
        with open(path, "r+") as f:
            json_data_local = json.load(f)
            json_data["graph"]["edges"] += json_data_local["graph"]["edges"]
            json_data["graph"]["nodes"] += json_data_local["graph"]["nodes"]
            json_data["graph"]["node_types"].update(json_data_local["graph"]["node_types"])

    nodes_to_remove = []

    # If the dataset is too large and everything is slow, uncomment the following code,

    # it will remove N (by default 700) random nodes from the data)

    # (do not forget to uncomment 'import numpy' on the top)

    # N = 700 # number of nodes to remove

    # nodes_to_remove = numpy.random.choice(

    #     [n["id"] for n in json_data["graph"]["nodes"]], N, replace=False)
    import pdb
    with open("panfam_output_DATE2022-05-21_HOUR08.56.36_PID17689/PanFAM5080.tsv",'r') as panfam_file:
        for line in panfam_file:
            elements = line.split()
            json_data["graph"]["edges"].append({"from": "F_" + elements[1].strip('"'), "to": "F_" + elements[0].strip('"'), "type": ["SIMILAR_TO"], "attr": {}})

    # Create nodes
    for n in tqdm(json_data["graph"]["nodes"], unit="node"):
        print(json_data["graph"]["node_types"]["s_Acinetobacter_pittii"]) if str(n["id"]) == "s_Acinetobacter_pittii" else None
        if n["id"] not in nodes_to_remove:
            if any([x in ["GeneFamily", "Module", "Taxa"] for x in json_data["graph"]["node_types"][str(n["id"])]]):
                query = (

                    "CREATE (n:{}) \n".format(":".join(json_data["graph"]["node_types"][str(n["id"])])) +

                    f'SET n.id = "{n["id"]}"' + "\n"

                )
                print(json_data["graph"]["node_types"]["s_Acinetobacter_pittii"]) if str(n["id"]) == "s_Acinetobacter_pittii" else None

                for k, v in n["attr"].items():

                    if k == "nb_genes" or k == "nb_genomes" or k == "nb_fam":

                        query += "SET n.{} = {}\n".format(k, v)

                    else:

                        query += "SET n.{} = \"{}\"\n".format(k, v)

                execute(driver, query)

    # Create relationships

    for e in tqdm(json_data["graph"]["edges"], unit="edges"):
        if any([x in ["IN_MODULE", "NEIGHBOR_OF", "IN_TAXA", "SIMILAR_TO"] for x in e["type"]]):
            query = (

                    'MATCH (s:{} {{id: "{}"}}), (t:{} {{id: "{}"}}) \n'.format(

                        ":".join(json_data["graph"]["node_types"][str(e["from"])]), e["from"],

                        ":".join(json_data["graph"]["node_types"][str(e["to"])]), e["to"]) +

                    'MERGE (s)-[r:{}]->(t)\n'.format(e["type"][0])

            )
            for k, v in e["attr"].items():

                if k == "weight":

                    query += "SET r.{} = {}\n".format(k, v)

                else:

                    query += "SET r.{} = \"{}\"\n".format(k, v)

            execute(driver, query)


if __name__ == '__main__':
    load_data()
