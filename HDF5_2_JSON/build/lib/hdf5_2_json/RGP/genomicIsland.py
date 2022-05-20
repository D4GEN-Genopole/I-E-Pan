#!/usr/bin/env python3
# coding:utf-8

# default libraries
import logging
import argparse

# installed libraries
from tqdm import tqdm

# local libraries
from ppanggolin.pangenome import Pangenome
from ppanggolin.region import Region
from ppanggolin.formats import checkPangenomeInfo, writePangenome, ErasePangenome
from ppanggolin.utils import restricted_float


class MatriceNode:
    def __init__(self, state, score, prev, gene):
        self.state = state  # state of the node. 1 for RGP and 0 for not RGP.
        self.score = score if score > 0 else 0  # current score of the node
        self.prev = prev  # previous matriceNode
        self.gene = gene  # gene this node corresponds to

    def changes(self, score):
        # state of the node. 1 for RGP and 0 for not RGP.
        self.state = 1 if score >= 0 else 0
        # current score of the node. If the given score is negative, set to 0.
        self.score = score if score >= 0 else 0


def extractRGP(contig, node, ID, naming):
    """
        Extract the region from the given starting node
    """
    new_region = None
    if naming == "contig":
        new_region = Region(contig.name + "_RGP_" + str(ID))
    elif naming == "organism":
        new_region = Region(node.gene.organism.name + "_" + contig.name + "_RGP_" + str(ID))
    while node.state:
        new_region.append(node.gene)
        node.state = 0
        node.score = 0
        node = node.prev
        if node is None:  # it's the end of the contig and the end of the region.
            break
    return new_region


def rewriteMatrix(contig, matrix, index, persistent, continuity, multi):
    """
        ReWrite the matrice from the given index of the node that started a region.
    """
    prev = matrix[index]
    index += 1
    if index > len(matrix) and contig.is_circular:
        index = 0
    # else the node was the last one of the contig, and there is nothing to do
    if index < len(matrix):
        nextNode = matrix[index]
        nbPerc = 0
        while nextNode.state:  # while the old state is not 0, recompute the scores.
            if nextNode.gene.family.namedPartition == "persistent" and nextNode.gene.family not in multi:
                modif = -pow(persistent, nbPerc)
                nbPerc += 1
            else:
                modif = continuity
                nbPerc = 0

            curr_score = modif + prev.score
            # scores can't be negative. If they are, they'll be set to 0.
            matrix[index].changes(curr_score)
            index += 1
            if index >= len(matrix):
                if contig.is_circular:
                    index = 0
                else:
                    # else we're at the end of the contig, so there are no more computations. Get out of the loop
                    break

            prev = nextNode
            nextNode = matrix[index]


def initMatrices(contig, persistent_penalty, variable_gain, multi):
    """initialize the vector of score/state nodes"""
    mat = []
    prev = None
    nbPerc = 0
    zeroInd = None
    curr_state = None
    for gene in contig.genes:
        if gene.family.namedPartition == "persistent" and gene.family not in multi:
            modif = -pow(persistent_penalty, nbPerc)
            nbPerc += 1
        else:
            modif = variable_gain
            nbPerc = 0

        curr_score = modif + prev.score if prev is not None else modif
        if curr_score >= 0:
            curr_state = 1
        else:
            curr_state = 0
            zeroInd = True
        prev = MatriceNode(curr_state, curr_score, prev, gene)
        if prev.state == 0:
            zeroInd = prev
        mat.append(prev)

    # if the contig is circular, and we're in a rgp state,
    # we need to continue from the "starting" gene until we leave rgp state.
    if contig.is_circular and curr_state and zeroInd is not None:
        # the previous node of the first processed gene is the last node.
        mat[0].prev = prev
        c = 0
        nbPerc = 0
        while curr_state:  # while state is rgp.
            matNode = mat[c]
            if matNode == zeroInd:
                # then we've parsed the entire contig twice.
                # The whole sequence is a rgp, so we're stopping the iteration now, otherwise we'll loop indefinitely
                break

            if matNode.gene.family.namedPartition == "persistent" and matNode.gene.family not in multi:
                modif = -pow(persistent_penalty, nbPerc)
                nbPerc += 1
            else:
                modif = variable_gain
                nbPerc = 0

            curr_score = modif + prev.score
            curr_state = 1 if curr_score >= 0 else 0
            matNode.changes(curr_score)
            c += 1
    return mat


def mkRegions(contig, matrix, min_length, min_score, persistent, continuity, multi, naming="contig"):
    # processing matrix and 'emptying' it to get the regions.
    def maxIndexNode(lst):
        """gets the last node with the highest score from a list of matriceNode"""
        if isinstance(lst, list):
            # init with the first element of the list
            maxScore = lst[0].score
            maxIndex = 0
            for index, node in enumerate(lst):
                if node.score >= maxScore:
                    maxScore = node.score
                    maxIndex = index
            return maxScore, maxIndex
        else:
            raise TypeError(f"List of matriceNode is expected. The detected type was {type(lst)}")

    contigRegions = set()
    val, index = maxIndexNode(matrix)
    while val >= min_score:
        new_region = extractRGP(contig, matrix[index], len(contigRegions), naming)
        new_region.score = val
        if (new_region[0].stop - new_region[-1].start) > min_length:
            contigRegions.add(new_region)
        rewriteMatrix(contig, matrix, index, persistent, continuity, multi)
        val, index = maxIndexNode(matrix)
    return contigRegions


def compute_org_rgp(organism, persistent_penalty, variable_gain, min_length, min_score, multigenics, naming="contig"):
    orgRegions = set()
    for contig in organism.contigs:
        if len(contig.genes) != 0:  # some contigs have no coding genes...
            # can definitely multiprocess this part, as not THAT much information is needed...
            matrix = initMatrices(contig, persistent_penalty, variable_gain, multigenics)
            orgRegions |= mkRegions(contig, matrix, min_length, min_score, persistent_penalty, variable_gain,
                                    multigenics, naming=naming)
    return orgRegions


def testNamingScheme(pangenome):
    contigsids = set()
    for org in pangenome.organisms:
        for contig in org.contigs:
            oldlen = len(contigsids)
            contigsids.add(contig.name)
            if oldlen == len(contigsids):
                logging.getLogger().warning("You have contigs with identical identifiers in your assemblies. "
                                            "identifiers will be supplemented with your provided organism names.")
                return "organism"
    return "contig"


def checkPangenomeFormerRGP(pangenome, force):
    """ checks pangenome status and .h5 files for former rgp, delete them if allowed or raise an error """
    if pangenome.status["predictedRGP"] == "inFile" and not force:
        raise Exception("You are trying to predict RGPs in a pangenome that already have them predicted. "
                        "If you REALLY want to do that, use --force "
                        "(it will erase RGPs and every feature computed from them).")
    elif pangenome.status["predictedRGP"] == "inFile" and force:
        ErasePangenome(pangenome, rgp=True)


def predictRGP(pangenome, force=False, persistent_penalty=3, variable_gain=1, min_length=3000, min_score=4,
               dup_margin=0.05, disable_bar=False):
    # check statuses and load info
    checkPangenomeFormerRGP(pangenome, force)
    checkPangenomeInfo(pangenome, needAnnotations=True, needFamilies=True, needGraph=False, needPartitions=True,
                       disable_bar=disable_bar)

    logging.getLogger().info("Detecting multigenic families...")
    multigenics = pangenome.get_multigenics(dup_margin)
    logging.getLogger().info("Compute Regions of Genomic Plasticity ...")
    namingScheme = testNamingScheme(pangenome)
    bar = tqdm(pangenome.organisms, unit="genomes", disable=disable_bar)
    for org in bar:
        pangenome.addRegions(compute_org_rgp(org, persistent_penalty, variable_gain, min_length, min_score, multigenics,
                                             naming=namingScheme))
    logging.getLogger().info(f"Predicted {len(pangenome.regions)} RGP")

    # save parameters and save status
    pangenome.parameters["RGP"] = {}
    pangenome.parameters["RGP"]["persistent_penalty"] = persistent_penalty
    pangenome.parameters["RGP"]["variable_gain"] = variable_gain
    pangenome.parameters["RGP"]["min_length"] = min_length
    pangenome.parameters["RGP"]["min_score"] = min_score
    pangenome.parameters["RGP"]["dup_margin"] = dup_margin
    pangenome.status['predictedRGP'] = "Computed"


def launch(args):
    pangenome = Pangenome()
    pangenome.addFile(args.pangenome)
    predictRGP(pangenome, force=args.force, persistent_penalty=args.persistent_penalty,
               variable_gain=args.variable_gain, min_length=args.min_length, min_score=args.min_score,
               dup_margin=args.dup_margin, disable_bar=args.disable_prog_bar)
    writePangenome(pangenome, pangenome.file, args.force, disable_bar=args.disable_prog_bar)


def rgpSubparser(subparser):
    parser = subparser.add_parser("rgp", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    required = parser.add_argument_group(title="Required arguments",
                                         description="One of the following arguments is required :")
    required.add_argument('-p', '--pangenome', required=True, type=str, help="The pangenome .h5 file")

    optional = parser.add_argument_group(title="Optional arguments")
    optional.add_argument('--persistent_penalty', required=False, type=int, default=3,
                          help="Penalty score to apply to persistent genes")
    optional.add_argument('--variable_gain', required=False, type=int, default=1,
                          help="Gain score to apply to variable genes")
    optional.add_argument('--min_score', required=False, type=int, default=4,
                          help="Minimal score wanted for considering a region as being a RGP")
    optional.add_argument('--min_length', required=False, type=int, default=3000,
                          help="Minimum length (bp) of a region to be considered a RGP")
    optional.add_argument("--dup_margin", required=False, type=restricted_float, default=0.05,
                          help="Minimum ratio of organisms where the family is present in which the family must "
                               "have multiple genes for it to be considered 'duplicated'")
    return parser
