#!/usr/bin/env python3
# coding:utf-8

# default libraries
import argparse
from collections import Counter
import logging
import random
import tempfile
import time
from multiprocessing import get_context
import os
import warnings

# installed libraries
from tqdm import tqdm
import gmpy2
import numpy
from pandas import Series, read_csv
import plotly.offline as out_plotly
import plotly.graph_objs as go
import scipy.optimize as optimization

# local libraries
from ppanggolin.pangenome import Pangenome
from ppanggolin.utils import mkOutdir
from ppanggolin.formats import checkPangenomeInfo
import ppanggolin.nem.partition as ppp

# import this way to use the global variable pan defined in ppanggolin.nem.partition

samples = []


def raref_nem(index, tmpdir, beta, sm_degree, free_dispersion, chunk_size, K, krange, seed):
    samp = samples[index]
    currtmpdir = tmpdir + "/" + str(index) + "/"
    if K < 3:
        K = ppp.evaluate_nb_partitions(samp, sm_degree, free_dispersion, chunk_size, krange, 0.05, False, 1,
                                       tmpdir + "/" + str(index) + "_eval", seed, None)

    if len(samp) <= chunk_size:  # all good, just write stuff.
        edges_weight, nb_fam = ppp.write_nem_input_files(tmpdir=currtmpdir, organisms=set(samp), sm_degree=sm_degree)
        cpt_partition = ppp.run_partitioning(currtmpdir, len(samp), beta * (nb_fam / edges_weight), free_dispersion,
                                             K=K, seed=seed, init="param_file")[0]
    else:  # going to need multiple partitioning for this sample...
        families = set()
        cpt_partition = {}
        validated = set()
        cpt = 0

        def validate_family(result):
            for node, nem_class in result[0].items():
                cpt_partition[node][nem_class[0]] += 1
                sum_partitioning = sum(cpt_partition[node].values())
                if (sum_partitioning > len(samp) / chunk_size and max(
                        cpt_partition[node].values()) >= sum_partitioning * 0.5) or (sum_partitioning > len(samp)):
                    if node not in validated:
                        if max(cpt_partition[node].values()) < sum_partitioning * 0.5:
                            cpt_partition[node]["U"] = len(samp)
                        validated.add(node)

        for fam in ppp.pan.geneFamilies:
            if not samp.isdisjoint(fam.organisms):  # otherwise, useless to keep track of
                families.add(fam)
                cpt_partition[fam.name] = {"P": 0, "S": 0, "C": 0, "U": 0}

        org_nb_sample = Counter()
        for org in samp:
            org_nb_sample[org] = 0
        condition = len(samp) / chunk_size

        while len(validated) < len(families):
            org_samples = []

            while not all(val >= condition for val in org_nb_sample.values()):
                # each family must be tested at least len(select_organisms)/chunk_size times.
                shuffled_orgs = list(samp)  # copy select_organisms
                random.shuffle(shuffled_orgs)  # shuffle the copied list
                while len(shuffled_orgs) > chunk_size:
                    org_samples.append(set(shuffled_orgs[:chunk_size]))
                    for org in org_samples[-1]:
                        org_nb_sample[org] += 1
                    shuffled_orgs = shuffled_orgs[chunk_size:]
            # making arguments for all samples:
            for samp in org_samples:
                edges_weight, nb_fam = ppp.write_nem_input_files(currtmpdir + "/" + str(cpt) + "/", samp,
                                                                 sm_degree=sm_degree)
                validate_family(
                    ppp.run_partitioning(currtmpdir + "/" + str(cpt) + "/", len(samp), beta * (nb_fam / edges_weight),
                                         free_dispersion, K=K, seed=seed, init="param_file"))
                cpt += 1
    if len(cpt_partition) == 0:
        counts = {"persistent": "NA", "shell": "NA", "cloud": "NA", "undefined": "NA", "K": K}
    else:
        counts = {"persistent": 0, "shell": 0, "cloud": 0, "undefined": 0, "K": K}

        for val in cpt_partition.values():
            if isinstance(val, str):
                part = val
            else:
                part = max(val, key=val.get)
            if part.startswith("P"):
                counts["persistent"] += 1
            elif part.startswith("C"):
                counts["cloud"] += 1
            elif part.startswith("S"):
                counts["shell"] += 1
            else:
                counts["undefined"] += 1
    return counts, index


def launch_raref_nem(args):
    return raref_nem(*args)


def drawCurve(output, maxSampling, data):
    logging.getLogger().info("Drawing the rarefaction curve ...")
    rarefName = output + "/rarefaction.csv"
    raref = open(rarefName, "w")
    raref.write(",".join(
        ["nb_org", "persistent", "shell", "cloud", "undefined", "exact_core", "exact_accessory", "soft_core",
         "soft_accessory", "pangenome", "K"]) + "\n")
    for part in data:
        raref.write(",".join(map(str,
                                 [part["nborgs"], part["persistent"], part["shell"], part["cloud"], part["undefined"],
                                  part["exact_core"], part["exact_accessory"], part["soft_core"],
                                  part["soft_accessory"], part["exact_core"] + part["exact_accessory"],
                                  part["K"]])) + "\n")
    raref.close()

    def heap_law(N, kappa, gamma):
        return kappa * N ** (gamma)

    def PolyArea(x, y):
        return 0.5 * numpy.abs(numpy.dot(x, numpy.roll(y, 1)) - numpy.dot(y, numpy.roll(x, 1)))

    annotations = []
    traces = []
    data_raref = read_csv(rarefName, index_col=False)
    params_file = open(output + "/rarefaction_parameters" + ".csv", "w")
    params_file.write("partition,kappa,gamma,kappa_std_error,gamma_std_error,IQR_area\n")
    for partition in ["persistent", "shell", "cloud", "undefined", "exact_core", "exact_accessory", "soft_core",
                      "soft_accessory", "pangenome"]:
        percentiles_75 = Series({i: numpy.nanpercentile(data_raref[data_raref["nb_org"] == i][partition], 75) for i in
                                 range(1, maxSampling + 1)}).dropna()
        percentiles_25 = Series({i: numpy.nanpercentile(data_raref[data_raref["nb_org"] == i][partition], 25) for i in
                                 range(1, maxSampling + 1)}).dropna()
        mins = Series({i: numpy.min(data_raref[data_raref["nb_org"] == i][partition]) for i in
                       range(1, maxSampling + 1)}).dropna()
        maxs = Series({i: numpy.max(data_raref[data_raref["nb_org"] == i][partition]) for i in
                       range(1, maxSampling + 1)}).dropna()
        medians = Series({i: numpy.median(data_raref[data_raref["nb_org"] == i][partition]) for i in
                          range(1, maxSampling + 1)}).dropna()
        means = Series({i: numpy.mean(data_raref[data_raref["nb_org"] == i][partition]) for i in
                        range(1, maxSampling + 1)}).dropna()
        initial_kappa_gamma = numpy.array([0.0, 0.0])
        x = percentiles_25.index.tolist()
        x += list(reversed(percentiles_25.index.tolist()))
        area_IQR = PolyArea(x, percentiles_25.tolist() + percentiles_75.tolist())
        nb_org_min_fitting = 15
        COLORS = {"pangenome": "black", "exact_accessory": "#EB37ED", "exact_core": "#FF2828", "soft_core": "#c7c938",
                  "soft_accessory": "#996633", "shell": "#00D860", "persistent": "#F7A507", "cloud": "#79DEFF",
                  "undefined": "#828282"}
        try:
            all_values = data_raref[data_raref["nb_org"] > nb_org_min_fitting][partition].dropna()
            res = optimization.curve_fit(heap_law, data_raref.loc[all_values.index]["nb_org"], all_values,
                                         initial_kappa_gamma)
            kappa, gamma = res[0]
            error_k, error_g = numpy.sqrt(numpy.diag(res[1]))  # to calculate the fitting error.
            # The variance of parameters are the diagonal elements of the variance-co variance matrix,
            # and the standard error is the square root of it. source :
            # https://stackoverflow.com/questions/25234996/getting-standard-error-associated-with-parameter-estimates-from-scipy-optimize-c
            if numpy.isinf(error_k) and numpy.isinf(error_g):
                params_file.write(",".join([partition, "NA", "NA", "NA", "NA", str(area_IQR)]) + "\n")
            else:
                params_file.write(
                    ",".join([partition, str(kappa), str(gamma), str(error_k), str(error_g), str(area_IQR)]) + "\n")
                regression = numpy.apply_along_axis(heap_law, 0, range(nb_org_min_fitting + 1, maxSampling + 1), kappa,
                                                    gamma)
                regression_sd_top = numpy.apply_along_axis(heap_law, 0, range(nb_org_min_fitting + 1, maxSampling + 1),
                                                           kappa - error_k, gamma + error_g)
                regression_sd_bottom = numpy.apply_along_axis(heap_law, 0,
                                                              range(nb_org_min_fitting + 1, maxSampling + 1),
                                                              kappa + error_k, gamma - error_g)
                traces.append(go.Scatter(x=list(range(nb_org_min_fitting + 1, maxSampling + 1)),
                                         y=regression,
                                         name=partition + ": Heaps' law",
                                         line=dict(color=COLORS[partition],
                                                   width=4,
                                                   dash='dash'),
                                         visible="legendonly" if partition == "undefined" else True))
                traces.append(go.Scatter(x=list(range(nb_org_min_fitting + 1, maxSampling + 1)),
                                         y=regression_sd_top,
                                         name=partition + ": Heaps' law error +",
                                         line=dict(color=COLORS[partition],
                                                   width=1,
                                                   dash='dash'),
                                         visible="legendonly" if partition == "undefined" else True))
                traces.append(go.Scatter(x=list(range(nb_org_min_fitting + 1, maxSampling + 1)),
                                         y=regression_sd_bottom,
                                         name=partition + ": Heaps' law error -",
                                         line=dict(color=COLORS[partition],
                                                   width=1,
                                                   dash='dash'),
                                         visible="legendonly" if partition == "undefined" else True))
                annotations.append(dict(x=maxSampling,
                                        y=heap_law(maxSampling, kappa, gamma),
                                        ay=0,
                                        ax=50,
                                        text="F=" + str(round(kappa, 0)) + "N" + "<sup>" + str(
                                            round(gamma, 5)) + "</sup><br>IQRarea=" + str(round(area_IQR, 2)),
                                        showarrow=True,
                                        arrowhead=7,
                                        font=dict(size=10, color='white'),
                                        align='center',
                                        arrowcolor=COLORS[partition],
                                        bordercolor='#c7c7c7',
                                        borderwidth=2,
                                        borderpad=4,
                                        bgcolor=COLORS[partition],
                                        opacity=0.8))
        except (TypeError, RuntimeError, ValueError):  # if fitting doesn't work
            params_file.write(",".join([partition, "NA", "NA", "NA", "NA", str(area_IQR)]) + "\n")

        traces.append(go.Scatter(x=medians.index,
                                 y=medians,
                                 name=partition + " : medians",
                                 mode="lines+markers",
                                 error_y=dict(type='data',
                                              symmetric=False,
                                              array=maxs.subtract(medians),
                                              arrayminus=medians.subtract(mins),
                                              visible=True,
                                              color=COLORS[partition],
                                              thickness=0.5),
                                 line=dict(color=COLORS[partition],
                                           width=1),
                                 marker=dict(color=COLORS[partition], symbol=3, size=8, opacity=0.5),
                                 visible="legendonly" if partition == "undefined" else True))
        traces.append(go.Scatter(x=means.index,
                                 y=means,
                                 name=partition + " : means",
                                 mode="markers",
                                 marker=dict(color=COLORS[partition], symbol=4, size=8, opacity=0.5),
                                 visible="legendonly" if partition == "undefined" else True))
        # up = percentiles_75
        # down = percentiles_25
        # IQR_area = up.append(down[::-1])
        # traces.append(go.Scatter(x=IQR_area.index,
        #                          y=IQR_area,
        #                          name = "IQR",
        #                          fill='toself',
        #                          mode="lines",
        #                          hoveron="points",
        #                          #hovertext=[str(round(e)) for e in half_stds.multiply(2)],
        #                          line=dict(color=COLORS[partition]),
        #                          marker=dict(color = COLORS[partition]),
        #                          visible = "legendonly" if partition == "undefined" else True))
        traces.append(go.Scatter(x=percentiles_75.index,
                                 y=percentiles_75,
                                 name=partition + " : 3rd quartile",
                                 mode="lines",
                                 hoveron="points",
                                 # hovertext=[str(round(e)) for e in half_stds.multiply(2)],
                                 line=dict(color=COLORS[partition]),
                                 marker=dict(color=COLORS[partition]),
                                 visible="legendonly" if partition == "undefined" else True))
        traces.append(go.Scatter(x=percentiles_25.index,
                                 y=percentiles_25,
                                 name=partition + " : 1st quartile",
                                 fill='tonexty',
                                 mode="lines",
                                 hoveron="points",
                                 # hovertext=[str(round(e)) for e in half_stds.multiply(2)],
                                 line=dict(color=COLORS[partition]),
                                 marker=dict(color=COLORS[partition]),
                                 visible="legendonly" if partition == "undefined" else True))
    layout = go.Layout(title="Rarefaction curve ",
                       titlefont=dict(size=20),
                       xaxis=dict(title='size of genome subsets (N)'),
                       yaxis=dict(title='# of gene families (F)'),
                       annotations=annotations,
                       plot_bgcolor='#ffffff')
    fig = go.Figure(data=traces, layout=layout)
    out_plotly.plot(fig, filename=output + "/rarefaction_curve.html", auto_open=False)
    params_file.close()


def makeRarefactionCurve(pangenome, output, tmpdir, beta=2.5, depth=30, minSampling=1, maxSampling=100, sm_degree=10,
                         free_dispersion=False, chunk_size=500, K=-1, cpu=1, seed=42, kestimate=False, krange=[3, -1],
                         soft_core=0.95, disable_bar=False):
    ppp.pan = pangenome  # use the global from partition to store the pangenome, so that it is usable

    try:
        krange[0] = ppp.pan.parameters["partition"]["K"] if krange[0] < 0 else krange[0]
        krange[1] = ppp.pan.parameters["partition"]["K"] if krange[1] < 0 else krange[1]
    except KeyError:
        krange = [3, 20]
    checkPangenomeInfo(pangenome, needAnnotations=True, needFamilies=True, needGraph=True, disable_bar=disable_bar)

    tmpdirObj = tempfile.TemporaryDirectory(dir=tmpdir)
    tmpdir = tmpdirObj.name

    if float(len(pangenome.organisms)) < maxSampling:
        maxSampling = len(pangenome.organisms)
    else:
        maxSampling = int(maxSampling)

    if K < 3 and kestimate is False:  # estimate K once and for all.
        try:
            K = ppp.pan.parameters["partition"]["K"]
            logging.getLogger().info(f"Reuse the number of partitions {K}")
        except KeyError:
            logging.getLogger().info("Estimating the number of partitions...")
            K = ppp.evaluate_nb_partitions(pangenome.organisms, sm_degree, free_dispersion, chunk_size, krange, 0.05,
                                           False, cpu, tmpdir, seed, None)
            logging.getLogger().info(f"The number of partitions has been evaluated at {K}")

    logging.getLogger().info("Extracting samples ...")
    AllSamples = []
    for i in range(minSampling, maxSampling):  # each point
        for _ in range(depth):  # number of samples per points
            AllSamples.append(set(random.sample(set(pangenome.organisms), i + 1)))
    logging.getLogger().info(f"Done sampling organisms in the pangenome, there are {len(AllSamples)} samples")
    SampNbPerPart = []

    logging.getLogger().info("Computing bitarrays for each family...")
    index_org = pangenome.computeFamilyBitarrays()
    logging.getLogger().info(
        f"Done computing bitarrays. Comparing them to get exact and soft core stats for {len(AllSamples)} samples...")
    bar = tqdm(range(len(AllSamples) * len(pangenome.geneFamilies)), unit="gene family", disable=disable_bar)
    for samp in AllSamples:
        # make the sample's organism bitarray.
        sampBitarray = gmpy2.xmpz(0)  # pylint: disable=no-member
        for org in samp:
            sampBitarray[index_org[org]] = 1

        part = Counter()
        part["soft_core"] = 0
        part["exact_core"] = 0
        part["exact_accessory"] = 0
        part["soft_accessory"] = 0
        for fam in pangenome.geneFamilies:
            nbCommonOrg = gmpy2.popcount(fam.bitarray & sampBitarray)  # pylint: disable=no-member
            part["nborgs"] = len(samp)
            if nbCommonOrg != 0:  # in that case the node 'does not exist'
                if nbCommonOrg == len(samp):
                    part["exact_core"] += 1
                else:
                    part["exact_accessory"] += 1

                if float(nbCommonOrg) >= len(samp) * soft_core:
                    part["soft_core"] += 1
                else:
                    part["soft_accessory"] += 1
            bar.update()
        SampNbPerPart.append(part)
    bar.close()
    # done with frequency of each family for each sample.

    global samples
    samples = AllSamples

    args = []
    for index, samp in enumerate(samples):
        args.append((index, tmpdir, beta, sm_degree, free_dispersion, chunk_size, K, krange, seed))

    with get_context('fork').Pool(processes=cpu) as p:
        # launch partitioning
        logging.getLogger().info(" Partitioning all samples...")
        bar = tqdm(range(len(args)), unit="samples partitionned", disable=disable_bar)
        random.shuffle(args)  # shuffling the processing so that the progress bar is closer to reality.
        for result in p.imap_unordered(launch_raref_nem, args):
            SampNbPerPart[result[1]] = {**result[0], **SampNbPerPart[result[1]]}
            bar.update()
    bar.close()

    logging.getLogger().info("Done  partitioning everything")
    warnings.filterwarnings("ignore")
    drawCurve(output, maxSampling, SampNbPerPart)
    warnings.resetwarnings()
    tmpdirObj.cleanup()
    logging.getLogger().info("Done making the rarefaction curves")


def launch(args):
    """
        main code when launch partition from the command line.
    """
    mkOutdir(args.output, args.force)
    pangenome = Pangenome()
    pangenome.addFile(args.pangenome)
    makeRarefactionCurve(pangenome=pangenome, output=args.output, tmpdir=args.tmpdir, beta=args.beta, depth=args.depth,
                         minSampling=args.min, maxSampling=args.max, sm_degree=args.max_degree_smoothing, cpu=args.cpu,
                         free_dispersion=args.free_dispersion, chunk_size=args.chunk_size, K=args.nb_of_partitions,
                         seed=args.seed, kestimate=args.reestimate_K, krange=args.krange, soft_core=args.soft_core,
                         disable_bar=args.disable_prog_bar)


def rarefactionSubparser(subparser):
    parser = subparser.add_parser("rarefaction", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    required = parser.add_argument_group(title="Required arguments",
                                         description="One of the following arguments is required :")
    required.add_argument('-p', '--pangenome', required=True, type=str, help="The pangenome .h5 file")

    optional = parser.add_argument_group(title="Optional arguments")
    optional.add_argument("-b", "--beta", required=False, default=2.5, type=float,
                          help="beta is the strength of the smoothing using the graph topology during  partitioning. "
                               "0 will deactivate spatial smoothing.")
    optional.add_argument("--depth", required=False, default=30, type=int,
                          help="Number of samplings at each sampling point")
    optional.add_argument("--min", required=False, default=1, type=int, help="Minimum number of organisms in a sample")
    optional.add_argument("--max", required=False, type=float, default=100,
                          help="Maximum number of organisms in a sample (if above the number of provided organisms, "
                               "the provided organisms will be the maximum)")

    optional.add_argument("-ms", "--max_degree_smoothing", required=False, default=10, type=float,
                          help="max. degree of the nodes to be included in the smoothing process.")
    optional.add_argument('-o', '--output', required=False, type=str,
                          default="ppanggolin_output" + time.strftime("_DATE%Y-%m-%d_HOUR%H.%M.%S",
                                                                      time.localtime()) + "_PID" + str(os.getpid()),
                          help="Output directory")
    optional.add_argument("-fd", "--free_dispersion", required=False, default=False, action="store_true",
                          help="use if the dispersion around the centroid vector of each partition during must be free."
                               " It will be the same for all organisms by default.")
    optional.add_argument("-ck", "--chunk_size", required=False, default=500, type=int,
                          help="Size of the chunks when performing partitioning using chunks of organisms. "
                               "Chunk partitioning will be used automatically "
                               "if the number of genomes is above this number.")
    optional.add_argument("-K", "--nb_of_partitions", required=False, default=-1, type=int,
                          help="Number of partitions to use. Must be at least 2. "
                               "By default reuse K if it exists else compute it.")
    optional.add_argument("--reestimate_K", required=False, action="store_true",
                          help=" Will recompute the number of partitions for each sample "
                               "(between the values provided by --krange) (VERY intensive. Can take a long time.)")
    optional.add_argument("-Kmm", "--krange", nargs=2, required=False, type=int, default=[3, -1],
                          help="Range of K values to test when detecting K automatically. "
                               "Default between 3 and the K previously computed "
                               "if there is one, or 20 if there are none.")
    optional.add_argument("--soft_core", required=False, type=float, default=0.95, help="Soft core threshold")
    optional.add_argument("-se", "--seed", type=int, default=42, help="seed used to generate random numbers")

    return parser
