#!/usr/bin/env python3
# coding:utf-8


# default libraries
import logging
from collections import defaultdict, Counter
import random
from math import pi

# local libraries
from ppanggolin.utils import jaccard_similarities
from ppanggolin.formats import checkPangenomeInfo
from ppanggolin.RGP.spot import compBorder

# installed libraries
from scipy.spatial.distance import pdist
from scipy.sparse import csc_matrix
from scipy.cluster.hierarchy import linkage, dendrogram
import networkx as nx

from tqdm import tqdm
from bokeh.plotting import ColumnDataSource, figure, save
from bokeh.io import output_file
from bokeh.layouts import column, row
from bokeh.models import WheelZoomTool, LabelSet, Slider, CustomJS, HoverTool, RadioGroup, Div


def checkPredictedSpots(pangenome):
    """ checks pangenome status and .h5 files for predicted spots, raises an error if they were not predicted"""
    if pangenome.status["spots"] == "No":
        raise Exception("You are trying to draw spots for a pangenome that does not have spots predicted. "
                        "Please see the 'spot' subcommand.")


def makeColorsForIterable(it):
    """randomly picks a color for all elements of a given iterable"""
    famcol = {}
    for element in it:
        col = list(random.choices(range(256), k=3))
        if element == "none":
            famcol[element] = "#D3D3D3"
        else:
            famcol[element] = '#%02x%02x%02x' % (col[0], col[1], col[2])
    return famcol


def orderGeneLists(geneLists, overlapping_match, exact_match, set_size):
    geneLists = lineOrderGeneLists(geneLists, overlapping_match, exact_match, set_size)
    return rowOrderGeneLists(geneLists)


def rowOrderGeneLists(geneLists):
    famDict = defaultdict(set)

    for index, genelist in enumerate([genelist[0] for genelist in geneLists]):
        for gene in genelist:
            if hasattr(gene, "family"):
                famDict[gene.family].add(index)
    all_indexes = []
    all_columns = []
    data = []
    for famIndex, RGPindexes in enumerate(famDict.values()):
        all_indexes.extend([famIndex] * len(RGPindexes))
        all_columns.extend(RGPindexes)
        data.extend([1.0] * len(RGPindexes))

    mat_p_a = csc_matrix((data, (all_indexes, all_columns)), shape=(len(famDict), len(geneLists)), dtype='float')
    dist = pdist(1 - jaccard_similarities(mat_p_a, 0).todense())
    hc = linkage(dist, 'single')

    dendro = dendrogram(hc, no_plot=True)

    new_geneLists = [geneLists[index] for index in dendro["leaves"]]

    return new_geneLists


def lineOrderGeneLists(geneLists, overlapping_match, exact_match, set_size):
    classified = set([0])  # first gene list has the right order
    new_classify = set()

    to_classify = set(range(1, len(geneLists)))  # the others may (or may not) have it

    while len(to_classify) != 0:
        for classIndex in classified:
            base_border1 = [gene.family for gene in geneLists[classIndex][1][0]]
            base_border2 = [gene.family for gene in geneLists[classIndex][1][1]]
            for unclassIndex in list(to_classify):
                border1 = [gene.family for gene in geneLists[unclassIndex][1][0]]
                border2 = [gene.family for gene in geneLists[unclassIndex][1][1]]
                if compBorder(base_border1, border1, overlapping_match, exact_match, set_size) and \
                        compBorder(base_border2, border2, overlapping_match, exact_match, set_size):
                    to_classify.discard(unclassIndex)
                    new_classify.add(unclassIndex)
                elif compBorder(base_border2, border1, overlapping_match, exact_match, set_size) and \
                        compBorder(base_border1, border2, overlapping_match, exact_match, set_size):
                    # reverse the order of the genes to match the 'reference'
                    geneLists[unclassIndex][0] = geneLists[unclassIndex][0][::-1]
                    # inverse the borders
                    former_border_1 = geneLists[unclassIndex][1][0]
                    former_border_2 = geneLists[unclassIndex][1][1]
                    geneLists[unclassIndex][1][0] = former_border_2
                    geneLists[unclassIndex][1][1] = former_border_1

                    # specify the new 'classified' and remove from unclassified
                    to_classify.discard(unclassIndex)
                    new_classify.add(unclassIndex)
        classified |= new_classify  # the newly classified will help to check the unclassified,
        # the formerly classified are not useful for what remains (if something remains)
        new_classify = set()
    return geneLists


def subgraph(spot, outname, with_border=True, set_size=3, multigenics=None, fam2mod=None):
    """ write a pangenome subgraph of the gene families of a spot in gexf format"""
    g = nx.Graph()

    for rgp in spot.regions:
        if with_border:
            borders = rgp.getBorderingGenes(set_size, multigenics)
            minpos = min([gene.position for border in borders for gene in border])
            maxpos = max([gene.position for border in borders for gene in border])
        else:
            minpos = rgp.startGene.position
            maxpos = rgp.stopGene.position
        GeneList = rgp.contig.genes[minpos:maxpos + 1]
        prev = None
        for gene in GeneList:
            g.add_node(gene.family.name, partition=gene.family.namedPartition)
            if fam2mod is not None:
                curr_mod = fam2mod.get(gene.family)
                if curr_mod is not None:
                    g.nodes[gene.family.name]["module"] = curr_mod
            try:
                g.nodes[gene.family.name]["occurrence"] += 1
            except KeyError:
                g.nodes[gene.family.name]["occurrence"] = 1
            if gene.name != "":
                if "name" in g.nodes[gene.family.name]:
                    try:
                        g.nodes[gene.family.name]["name"][gene.name] += 1
                    except KeyError:
                        g.nodes[gene.family.name]["name"][gene.name] = 1
                else:
                    g.nodes[gene.family.name]["name"] = Counter([gene.name])
            if prev is not None:
                g.add_edge(gene.family.name, prev)
                try:
                    g[gene.family.name][prev]["rgp"].add(rgp)
                except KeyError:
                    g[gene.family.name][prev]["rgp"] = set(rgp)
            prev = gene.family.name
    for node1, node2 in g.edges:
        g[node1][node2]["weight"] = len(g[node1][node2]["rgp"]) / len(spot.regions)
        del g[node1][node2]["rgp"]
    for node in g.nodes:
        if "name" in g.nodes[node]:
            g.nodes[node]["name"] = g.nodes[node]["name"].most_common(1)[0][0]

    nx.write_gexf(g, outname)


def mkSourceData(genelists, famCol, fam2mod):
    partitionColors = {"shell": "#00D860", "persistent": "#F7A507", "cloud": "#79DEFF"}

    df = {'name': [], 'ordered': [], 'strand': [], "start": [], "stop": [], 'module': [], 'module_color': [], 'x': [],
          'y': [], 'width': [], 'family_color': [], 'partition_color': [], 'partition': [], "family": [], "product": [],
          "x_label": [], "y_label": [], "label": [], "gene_type": [], 'gene_ID': [], "gene_local_ID": []}

    for index, GeneList in enumerate(genelists):
        genelist = GeneList[0]

        if genelist[0].start < genelist[1].start:
            # if the order has been inverted, positionning elements on the figure is different
            ordered = True
            start = genelist[0].start
        else:
            ordered = False
            start = genelist[0].stop

        for gene in genelist:
            df["ordered"].append(str(ordered))
            df["strand"].append(gene.strand)
            df["start"].append(gene.start)
            df["stop"].append(gene.stop)
            df["gene_type"].append(gene.type)
            df["product"].append(gene.product)
            df["gene_local_ID"].append(gene.local_identifier)
            df['gene_ID'].append(gene.ID)

            if "RNA" in gene.type:  # dedicated values for RNA genes
                df["name"].append(gene.product)
                df["family"].append(gene.type)
                df["partition"].append("none")
                df["family_color"].append("#A109A7")
                df["partition_color"].append("#A109A7")
                df["module"].append("none")
            else:
                df["name"].append(gene.name)
                df["family"].append(gene.family.name)
                df["partition"].append(gene.family.namedPartition)
                df["family_color"].append(famCol[gene.family])
                df["partition_color"].append(partitionColors[gene.family.namedPartition])
                df["module"].append(fam2mod.get(gene.family, "none"))

            df["x"].append((abs(gene.start - start) + abs(gene.stop - start)) / 2)
            df["width"].append(gene.stop - gene.start)
            df["x_label"].append(df["x"][-1] - int(df["width"][-1] / 2))
            if ordered:
                if gene.strand == "+":
                    df["y"].append((index * 10) + 1)
                else:

                    df["y"].append((index * 10) - 1)
            else:
                if gene.strand == "+":
                    df["y"].append((index * 10) - 1)
                else:
                    df["y"].append((index * 10) + 1)
            df["y_label"].append(df["y"][-1] + 1.5)
    df["label"] = df["name"]
    df["line_color"] = df["partition_color"]
    df["fill_color"] = df["family_color"]

    # define colors for modules
    mod2col = makeColorsForIterable(set(df["module"]))
    mod_colors = []
    for mod in df["module"]:
        mod_colors.append(mod2col[mod])
    df["module_color"] = mod_colors

    # defining things that we will see when hovering over the graphical elements
    TOOLTIPS = [
        ("start", "@start"),
        ("stop", "@stop"),
        ("name", "@name"),
        ("product", "@product"),
        ("family", "@family"),
        ("module", "@module"),
        ("partition", "@partition"),
        ("local identifier", "@gene_local_ID"),
        ("gene ID", "@gene_ID"),
        ("ordered", "@ordered"),
        ("strand", "@strand"),
    ]

    return ColumnDataSource(data=df), TOOLTIPS


def addGeneTools(recs, sourceData):
    """ define tools to change the outline and fill colors of genes"""

    def colorStr(color_element):
        """javascript code to switch between partition, family and module color for the given 'color_element'"""
        return f"""
            if(this.active == 0){{
                source.data['{color_element}'] = source.data['partition_color'];
            }}else if(this.active == 1){{
                source.data['{color_element}'] = source.data['family_color'];
            }}else if(this.active == 2){{
                source.data['{color_element}'] = source.data['module_color'];
            }}
            recs.{color_element} = source.data['{color_element}'];
            source.change.emit();
        """

    radio_line_color = RadioGroup(labels=["partition", "family", "module"], active=0)
    radio_fill_color = RadioGroup(labels=["partition", "family", "module"], active=1)

    radio_line_color.js_on_click(CustomJS(args=dict(recs=recs, source=sourceData),
                                          code=colorStr("line_color")))

    radio_fill_color.js_on_click(CustomJS(args=dict(recs=recs, source=sourceData),
                                          code=colorStr("fill_color")))

    color_header = Div(text="<b>Genes:</b>")
    line_title = Div(text="""Color to use for gene outlines:""",
                     width=200, height=100)
    fill_title = Div(text="""Color to fill genes with:""",
                     width=200, height=100)

    gene_outline_size = Slider(start=0, end=10, value=5, step=0.1, title="Gene outline size:")
    gene_outline_size.js_on_change('value', CustomJS(args=dict(other=recs),
                                                     code="""
                other.glyph.line_width = this.value;
                """
                                                     ))

    return column(color_header, row(column(line_title, radio_line_color), column(fill_title, radio_fill_color)),
                  gene_outline_size)


def addGeneLabels(fig, sourceData):
    labels = LabelSet(x='x_label', y='y_label', text='label', source=sourceData, render_mode='canvas',
                      text_font_size="18px")
    slider_font = Slider(start=0, end=64, value=16, step=1, title="Gene label font size in px")
    slider_angle = Slider(start=0, end=pi / 2, value=0, step=0.01, title="Gene label angle in radian")

    radio_label_type = RadioGroup(labels=["name", "product", "family", "local identifier", "gene ID", "none"], active=0)

    slider_angle.js_link('value', labels, 'angle')

    slider_font.js_on_change('value',
                             CustomJS(args=dict(other=labels),
                                      code="other.text_font_size = this.value+'px';"
                                      )
                             )

    radio_label_type.js_on_click(CustomJS(args=dict(other=labels, source=sourceData),
                                          code="""
                if(this.active == 5){
                    source.data['label'] = [];
                    for(var i=0;i<source.data['name'].length;i++){
                        source.data['label'].push('');
                    }
                }else if(this.active == 3){
                    source.data['label'] = source.data['gene_local_ID'];
                }else if(this.active == 4){
                    source.data['label'] = source.data['gene_ID'];
                }
                else{
                    source.data['label'] = source.data[this.labels[this.active]];
                }
                other.source = source;
                source.change.emit();
                """
                                          ))

    label_header = Div(text="<b>Gene labels:</b>")
    radio_title = Div(text="""Gene labels to use:""",
                      width=200, height=100)
    labels_block = column(label_header, row(slider_font, slider_angle), column(radio_title, radio_label_type))

    fig.add_layout(labels)

    return labels_block, labels


def mkGenomes(geneLists, ordered_counts):
    df = {"name": [], "width": [], "occurrences": [], 'x': [], 'y': [], "x_label": []}

    for index, GeneList in enumerate(geneLists):
        genelist = GeneList[0]
        df["occurrences"].append(ordered_counts[index])
        df["y"].append(index * 10)
        if genelist[0].start < genelist[1].start:
            # if the order has been inverted, positionning elements on the figure is different
            df["width"].append(abs(genelist[-1].stop - genelist[0].start))
        else:
            #order has been inverted
            df["width"].append(abs(genelist[0].stop - genelist[-1].start))
        df["x"].append((df["width"][-1]) / 2)
        df["x_label"].append(0)
        df["name"].append(genelist[0].organism.name)
    TOOLTIP = [
        ("name", "@name"),
        ("occurrences", "@occurrences"),
    ]
    return ColumnDataSource(data=df), TOOLTIP


def addGenomeTools(fig, geneRecs, genomeRecs, geneSource, genomeSource, nb, geneLabels):
    # add genome labels
    genomeLabels = LabelSet(x='x_label', y='y', x_offset=-20, text='name', text_align="right", source=genomeSource,
                            render_mode='canvas', text_font_size="16px")
    fig.add_layout(genomeLabels)

    slider_font = Slider(start=0, end=64, value=16, step=1, title="Genome label font size in px")
    slider_font.js_on_change('value',
                             CustomJS(args=dict(other=genomeLabels),
                                      code="other.text_font_size = this.value+'px';"
                                      )
                             )

    slider_offset = Slider(start=-400, end=0, value=-20, step=1, title="Genome label offset")
    slider_offset.js_link('value', genomeLabels, 'x_offset')

    slider_spacing = Slider(start=1, end=40, value=10, step=1, title="Genomes spacing")
    slider_spacing.js_on_change('value', CustomJS(
        args=dict(geneRecs=geneRecs, geneSource=geneSource, genomeRecs=genomeRecs, genomeSource=genomeSource,
                  nb_elements=nb, genomeLabels=genomeLabels, geneLabels=geneLabels),
        code="""
            var current_val = genomeSource.data['y'][genomeSource.data['y'].length - 1] / (nb_elements-1);
            for (let i=0 ; i < genomeSource.data['y'].length ; i++){
                genomeSource.data['y'][i] =  (genomeSource.data['y'][i] * this.value) / current_val;
            }
            for (let i=0 ; i < geneSource.data['y'].length ; i++){
                if((geneSource.data['ordered'][i] == 'True' && geneSource.data['strand'][i] == '+') || (geneSource.data['ordered'][i] == 'False' && geneSource.data['strand'][i] == '-') ){
                    geneSource.data['y'][i] = (((geneSource.data['y'][i]-1) * this.value) / current_val) +1;
                    geneSource.data['y_label'][i] = (((geneSource.data['y_label'][i]-1-1.5) * this.value) / current_val) + 1 + 1.5;
                }else{
                    geneSource.data['y'][i] = (((geneSource.data['y'][i]+1) * this.value) / current_val) -1;
                    geneSource.data['y_label'][i] = (((geneSource.data['y_label'][i]+1-1.5) * this.value) / current_val) -1 + 1.5;

                }
            }
            geneRecs.source = geneSource;
            genomeRecs.source = genomeSource;
            geneLabels.source = geneSource;
            genomeLabels.source = genomeSource;
            geneSource.change.emit();
            genomeSource.change.emit();
        """))

    genome_header = Div(text="<b>Genomes:</b>")
    return column(genome_header, slider_spacing, slider_font, slider_offset)


def drawCurrSpot(genelists, ordered_counts, fam2mod, famCol, filename):
    # prepare the source data

    output_file(filename + ".html")

    # generate the figure and add some tools to it
    wheel_zoom = WheelZoomTool()
    fig = figure(title="spot graphic", plot_width=1600, plot_height=600,
                 tools=["pan", "box_zoom", "reset", "save", wheel_zoom, "ywheel_zoom", "xwheel_zoom"])
    fig.axis.visible = True
    fig.toolbar.active_scroll = wheel_zoom

    # genome rectangles
    genomeSource, genomeTooltip = mkGenomes(genelists, ordered_counts)
    genomeRecs = fig.rect(x='x', y='y', fill_color="dimgray", width="width", height=0.5, source=genomeSource)
    genomeRecs_hover = HoverTool(renderers=[genomeRecs], tooltips=genomeTooltip, mode="mouse",
                                 point_policy="follow_mouse")
    fig.add_tools(genomeRecs_hover)

    # gene rectanges
    GeneSource, GeneTooltips = mkSourceData(genelists, famCol, fam2mod)
    recs = fig.rect(x='x', y='y', line_color='line_color', fill_color='fill_color', width='width', height=2,
                    line_width=5, source=GeneSource)
    recs_hover = HoverTool(renderers=[recs], tooltips=GeneTooltips, mode="mouse", point_policy="follow_mouse")
    fig.add_tools(recs_hover)
    # gene modification tools
    gene_tools = addGeneTools(recs, GeneSource)

    # label modification tools
    labels_tools, labels = addGeneLabels(fig, GeneSource)

    # genome tool
    genome_tools = addGenomeTools(fig, recs, genomeRecs, GeneSource, genomeSource, len(genelists), labels)

    save(column(fig, row(labels_tools, gene_tools), row(genome_tools)))


def drawSelectedSpots(selected_spots, pangenome, output, overlapping_match, exact_match, set_size, disable_bar):
    logging.getLogger().info("Ordering genes among regions, and drawing spots...")

    multigenics = pangenome.get_multigenics(pangenome.parameters["RGP"]["dup_margin"])
    # bar = tqdm(range(len(selected_spots)), unit = "spot", disable = disable_bar)

    fam2mod = {}
    for mod in pangenome.modules:
        for fam in mod.families:
            fam2mod[fam] = f"module_{mod.ID}"

    for spot in tqdm(selected_spots, unit="spot", disable=disable_bar):

        fname = output + '/spot_' + str(spot.ID)

        # write rgps representatives and the rgps they are identical to
        out_struc = open(fname + '_identical_rgps.tsv', 'w')
        out_struc.write('representative_rgp\trepresentative_rgp_organism\tidentical_rgp\tidentical_rgp_organism\n')
        for keyRGP, otherRGPs in spot.getUniq2RGP().items():
            for rgp in otherRGPs:
                out_struc.write(f"{keyRGP.name}\t{keyRGP.organism.name}\t{rgp.name}\t{rgp.organism.name}\n")
        out_struc.close()

        Fams = set()
        GeneLists = []

        for rgp in spot.regions:
            borders = rgp.getBorderingGenes(set_size, multigenics)
            minpos = min([gene.position for border in borders for gene in border])
            maxpos = max([gene.position for border in borders for gene in border])
            GeneList = rgp.contig.genes[minpos:maxpos + 1]
            minstart = min([gene.start for border in borders for gene in border])
            maxstop = max([gene.stop for border in borders for gene in border])
            RNAstoadd = set()
            for rna in rgp.contig.RNAs:
                if minstart < rna.start < maxstop:
                    RNAstoadd.add(rna)
            GeneList.extend(RNAstoadd)
            GeneList = sorted(GeneList, key=lambda x: x.start)

            Fams |= {gene.family for gene in GeneList if gene.type == "CDS"}

            GeneLists.append([GeneList, borders, rgp])
        famcolors = makeColorsForIterable(Fams)
        # order all rgps the same way, and order them by similarity in gene content
        GeneLists = orderGeneLists(GeneLists, overlapping_match, exact_match, set_size)

        countUniq = spot.countUniqOrderedSet()

        # keep only the representative rgps for the figure
        uniqGeneLists = []
        ordered_counts = []
        for genelist in GeneLists:
            curr_genelist_count = countUniq.get(genelist[2], None)
            if curr_genelist_count is not None:
                uniqGeneLists.append(genelist)
                ordered_counts.append(curr_genelist_count)

        drawCurrSpot(uniqGeneLists, ordered_counts, fam2mod, famcolors, fname)
        subgraph(spot, fname + ".gexf", set_size=set_size, multigenics=multigenics, fam2mod=fam2mod)
    logging.getLogger().info(f"Done drawing spot(s), they can be found in the directory: '{output}'")


def drawSpots(pangenome, output, spot_list, disable_bar):
    # check that the pangenome has spots
    checkPredictedSpots(pangenome)

    needMod = False
    if pangenome.status["modules"] != "No":
        # modules are not required to be loaded, but if they have been computed we load them.
        needMod = True

    checkPangenomeInfo(pangenome, needAnnotations=True, needFamilies=True, needGraph=False, needPartitions=True,
                       needRGP=True, needSpots=True, needModules=needMod, disable_bar=disable_bar)

    selected_spots = set()
    curated_spot_list = ['spot_' + str(s) if 'spot' not in s else str(s) for s in spot_list.split(',')]

    if spot_list == 'all' or any(x == 'all' for x in curated_spot_list):
        selected_spots = [s for s in pangenome.spots if len(s.getUniqOrderedSet()) > 1]
    else:
        selected_spots = [s for s in pangenome.spots if "spot_" + str(s.ID) in curated_spot_list]
    if len(selected_spots) < 10:
        logging.getLogger().info(
            f"Drawing the following spots: {','.join(['spot_' + str(s.ID) for s in selected_spots])}")
    else:
        logging.getLogger().info(f"Drawing {len(selected_spots)} spots")

    drawSelectedSpots(selected_spots, pangenome, output,
                      overlapping_match=pangenome.parameters["spots"]["overlapping_match"],
                      exact_match=pangenome.parameters["spots"]["exact_match"],
                      set_size=pangenome.parameters["spots"]["set_size"],
                      disable_bar=disable_bar)
