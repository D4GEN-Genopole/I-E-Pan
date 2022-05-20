#!/usr/bin/env python
# coding: utf-8

import json
from tqdm import tqdm

# import numpy

from neo4j import GraphDatabase

# Here substitute with your uri, user and pwd, if necessary

URI = "bolt://localhost:7687"

USER = "neo4j"

PASSWORD = "1234"

# Here change filename if necessary

FILENAME = "PanGenome/Acinetobacter_baumannii/pangenomeGraph.json"


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

    clean_query = "MATCH (n) DETACH DELETE n"

    execute(driver, clean_query)

    with open(FILENAME, "r+") as f:

        json_data = json.load(f)

    nodes_to_remove = []

    # If the dataset is too large and everything is slow, uncomment the following code,

    # it will remove N (by default 700) random nodes from the data)

    # (do not forget to uncomment 'import numpy' on the top)

    # N = 700 # number of nodes to remove

    # nodes_to_remove = numpy.random.choice(

    #     [n["id"] for n in json_data["graph"]["nodes"]], N, replace=False)

    # Create nodes

    for n in tqdm(json_data["graph"]["nodes"], unit="node"):

        if n["id"] not in nodes_to_remove:
            if json_data["graph"]["node_types"][str(n["id"])] in ["GeneFamilies", "module"]:
                query = (

                    "CREATE (n:{}) \n".format(":".join(json_data["graph"]["node_types"][str(n["id"])])) +

                    f'SET n.id = "{n["id"]}"' + "\n"

                )

                for k, v in n["attr"].items():

                    if k == "nb_genes" or k == "nb_genomes" or k == "nb_fam":

                        query += "SET n.{} = {}\n".format(k, v)

                    else:

                        query += "SET n.{} = \"{}\"\n".format(k, v)

                execute(driver, query)

    # Create relationships

    for e in tqdm(json_data["graph"]["edges"], unit="edges"):
        if e["type"] in ["IN_MODULE", "NEIGHBOR_OF"]:
            query = (

                    'MATCH (s:{} {{id: "{}"}}), (t:{} {{id: "{}"}}) \n'.format(

                        json_data["graph"]["node_types"][str(e["from"])], e["from"],

                        json_data["graph"]["node_types"][str(e["to"])], e["to"]) +

                    'MERGE (s)-[r:{}]->(t)\n'.format(e["type"])

            )
            for k, v in e["attr"].items():

                if k == "weight":

                    query += "SET r.{} = {}\n".format(k, v)

                else:

                    query += "SET r.{} = \"{}\"\n".format(k, v)

            execute(driver, query)


if __name__ == '__main__':
    load_data()