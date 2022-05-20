#!/usr/bin/env python3
# coding:utf-8

# default libraries
import argparse
from multiprocessing import get_context
from collections import Counter, defaultdict
import logging
import pkg_resources
from statistics import median, mean, stdev
import os
import json
import pdb

# local libraries
from ppanggolin.pangenome import Pangenome
from ppanggolin.utils import write_compressed_or_not, mkOutdir, restricted_float
from ppanggolin.formats import checkPangenomeInfo

# global variable to store the pangenome
pan = None  # TODO change to pan:Pangenome = Pangenome=() ?
needAnnotations = False
needFamilies = False
needGraph = False
needPartitions = False
needSpots = False
needRegions = False
needModules = False
ignore_err = False


def writeJSON_old(output, compress):
    logging.getLogger().info("Writing the json file for the pangenome graph...")
    outname = output + "/pangenomeGraph.json"
    with write_compressed_or_not(outname, compress) as json:
        json.write('{"graph":')
        json.write('{"edges":[')
        edgeids = 0
        index = pan.getIndex()

        for edge in pan.edges:
            json.write('{"from":' + str(edge.source.ID) + ',' +
                       '"to":' + str(edge.target.ID) + ',' +
                       '"attr":{"weight":' + str(len(edge.organisms)) + '}},')
        json.write(',"nodes": [')
        node_types = {}
        famList = list(pan.geneFamilies)
        firstFam = famList[0]
        writeJSONGeneFam(firstFam, json)
        node_types[str(firstFam.ID)] = "GeneFamilies"
        for geneFam in famList[1:]:
            json.write(', ')
            writeJSONGeneFam(geneFam, json)
            node_types[str(geneFam.ID)] = "GeneFamilies"
        json.write(']')
        # if other things are to be written such as the parameters, write them here
        json.write('},')
        for mod in pan.modules:
            json.write('{"id":' + str(mod.ID) + ',' +
                       '"attr:{"nb_fam"' + str(len(mod.families)) + '}},')
            for family in mod.families:
                partition_counter[family.namedPartition] += 1
                for gene in family.genes:
                    org_dict[gene.organism].add(gene)
            fout.write(
                f"module_{mod.ID}\t{len(mod.families)}\t{len(org_dict)}\t{partition_counter.most_common(1)[0][0]}\t"
                f"{round((sum([len(genes) for genes in org_dict.values()]) / len(org_dict)) / len(mod.families), 3)}\n")
    logging.getLogger().info(f"Done writing the json file : '{outname}'")


def writeJSON(output, compress):
    logging.getLogger().info("Writing the json file for the pangenome graph...")
    outname = output + "/pangenomeGraph.json"
    out_dict = {"graph": {"edges": [], "nodes": [], "node_types": {}}}
    for org in pan.organisms:
        out_dict["graph"]["nodes"].append({"id": str(org.name), "attr": {}})
        out_dict["graph"]["node_types"][str(org.name)] = ["genome"]
    for edge in pan.edges:
        part_source, part_target = (pan.getGeneFamily(edge.source.name).namedPartition,
                                    pan.getGeneFamily(edge.target.name).namedPartition)
        out_dict["graph"]["edges"].append(
            {"from": int(edge.source.ID), "to": int(edge.target.ID),
             "type": ["NEIGHBOR_OF", f"{part_source}_{part_target}"],
             "attr": {"weight": len(edge.organisms)}})
    for geneFam in list(pan.geneFamilies):
        out_dict["graph"]["nodes"].append({"id": int(geneFam.ID), "attr": {"nb_genomes": len(edge.organisms),
                                                                           "partition": geneFam.namedPartition,
                                                                           "subpartition": geneFam.partition,
                                                                           "nb_genes": len(geneFam.genes)}})
        out_dict["graph"]["node_types"][str(geneFam.ID)] = ["GeneFamily", geneFam.namedPartition]
        for gene in geneFam.genes:
            out_dict["graph"]["nodes"].append(
                {"id": gene.ID, "attr": {"genomic_type": "CDS", "is_fragment": int(gene.is_fragment)}})
            out_dict["graph"]["edges"].append({"from": gene.ID, "to": geneFam.ID, "type": "IN_FAMILY", "attr": {}})
            out_dict["graph"]["node_types"][str(gene.ID)] = ["gene"]
            out_dict["graph"]["edges"].append(
                {"from": gene.ID, "to": str(gene.organism.name), "type": "IN_ORG", "attr": {}})
    for mod in pan.modules:
        out_dict["graph"]["nodes"].append({"id": int(mod.ID), "attr": {"nb_fams": str(len(mod.families))}})
        out_dict["graph"]["node_types"][str(mod.ID)] = ["Module"]
        for family in mod.families:
            out_dict["graph"]["edges"].append(
                {"from": int(family.ID), "to": int(mod.ID), "type": ["IN_MODULE"], "attr": {}})
    with write_compressed_or_not(outname, compress) as json_file:
        json.dump(out_dict, json_file)


def writeJSONGeneFam(geneFam, json):
    json.write('{' + f'"id": {geneFam.ID}, "attr":' + '{' +
               f'"name": "{geneFam.name}", ' +
               f'"nb_genes": {len(geneFam.genes)}, ' +
               f'"partition": "{geneFam.namedPartition}", "subpartition": "{geneFam.partition}"' + '}')


def writeFlatFiles(pangenome, output, cpu=1, json=False, compress=False,
                   disable_bar=False):
    if not any(x for x in [json]):
        raise Exception("You did not indicate what file you wanted to write.")

    global pan
    pan = pangenome
    processes = []
    global needAnnotations
    global needFamilies
    global needGraph
    global needPartitions
    global needSpots
    global needRegions
    global needModules
    global ignore_err

    if json:
        needAnnotations = True
        needFamilies = True
        needPartitions = True
        needGraph = True
        needModules = True if pangenome.status["modules"] == "inFile" else False

    checkPangenomeInfo(pan, needAnnotations=needAnnotations, needFamilies=needFamilies, needGraph=needGraph,
                       needPartitions=needPartitions, needRGP=needRegions, needSpots=needSpots, needModules=needModules,
                       disable_bar=disable_bar)

    pan.getIndex()  # make the index because it will be used most likely
    if json:
        writeJSON(output, compress)


def launchFlat(args):
    mkOutdir(args.output, args.force)
    pangenome = Pangenome()
    pangenome.addFile(args.pangenome)
    writeFlatFiles(pangenome, args.output, cpu=args.cpu, json=args.json, compress=args.compress,
                   disable_bar=args.disable_prog_bar)


def writeFlatSubparser(subparser):
    parser = subparser.add_parser("write", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    required = parser.add_argument_group(title="Required arguments",
                                         description="One of the following arguments is required :")
    required.add_argument('-p', '--pangenome', required=True, type=str, help="The pangenome .h5 file")
    required.add_argument('-o', '--output', required=True, type=str,
                          help="Output directory where the file(s) will be written")
    optional = parser.add_argument_group(title="Optional arguments")
    optional.add_argument("--json", required=False, action="store_true", help="Writes the graph in a json file format")
    optional.add_argument("--compress", required=False, action="store_true", help="Compress the files in .gz")
    return parser
