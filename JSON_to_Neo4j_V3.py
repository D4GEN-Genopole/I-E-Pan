

#!/usr/bin/env python

# coding: utf-8



import json
import re
from tqdm import tqdm


# import numpy

from neo4j import GraphDatabase

import argparse

parser=argparse.ArgumentParser()
parser.add_argument('URI')
parser.add_argument('USER')
parser.add_argument('PASSWORD')
parser.add_argument('PATH')
args=parser.parse_args()

# Here substitute with your uri, user and pwd, if necessary

#URI = "bolt://localhost:7687"
URI=args.URI
#USER = "neo4j"
USER=args.USER

#PASSWORD = "neoforj"
PASSWORD=args.PASSWORD



# Here change filename if necessary

#FILENAME = "test_V2.json"
FILENAME=args.PATH





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

    entier = re.compile("^[1-9][0-9]*$")
    chaine = re.compile("^.*$")

    Attributes = {}
    Attributes["name"] = chaine
    Attributes["nb_genes"] = entier
    Attributes["nb_genomes"] = entier
    Attributes["nb_fams"] = entier
    Attributes["partition"] = re.compile("[pP]ersistent|[sS]hell|[cC]loud|[uU]ndefined")
    Attributes["subpartition"] = chaine
    Attributes["From"] = entier
    Attributes["To"] = entier
    Attributes["Id"] = entier
    Attributes["weight"] = entier
    Attributes["type"] = re.compile("GeneFamily|Module|Gene|Genome")

    # Create nodes

    for n in tqdm(json_data["graph"]["nodes"], unit="node"):

        if n["id"] not in nodes_to_remove:

            if any([x in ["GeneFamilies", "module"] for x in json_data["graph"]["node_types"][str(n["id"])]]):

                query = "CREATE (n:{})\n".format(":".join(json_data["graph"]["node_types"][str(n["id"])])) + "SET n.id = {} \n".format(n["id"])

                for k, v in n["attr"].items():

                    if k not in Attributes: raise Exception("Unknown attribute : " + str(k))

                    if not(bool(Attributes[k].match(str(v)))): raise Exception("Wrong format for attribute " + str(k) + " : " + str(v))

                    if k == "nb_genes" or k == "nb_genomes" or k == "nb_fam":

                        query += "SET n.{} = {}\n".format(k, v)

                    else:

                        query += "SET n.{} = \"{}\"\n".format(k, v)

                execute(driver, query)



    # Create relationships

    for e in tqdm(json_data["graph"]["edges"], unit="edges"):

        if any([x in ["IN_MODULE", "NEIGHBOR_OF"] for x in e["type"]]):

            query = "MATCH (s:{} {{id: {}}}), (t:{} {{id: {}}}) \n".format(":".join(json_data["graph"]["node_types"][str(e["from"])]), e["from"], ":".join(json_data["graph"]["node_types"][str(e["to"])]), e["to"]) + "MERGE (s)-[r:{}]->(t)\n".format(e["type"][0])

            for k, v in e["attr"].items():

                if k not in Attributes: raise Exception("Unknown attribute : " + str(k))

                if not(bool(Attributes[k].match(str(v)))): raise Exception("Wrong format for attribute " + str(k) + " : " + str(v))

                if k == "weight":

                    query += "SET r.{} = {}\n".format(k, v)

                else:

                    query += "SET r.{} = \"{}\"\n".format(k, v)

            execute(driver, query)


if __name__ == '__main__':

    load_data()

