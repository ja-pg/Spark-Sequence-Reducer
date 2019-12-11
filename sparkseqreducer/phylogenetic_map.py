#     phylogenetic_map: The functions generate the taxonomic tree for any taxid
#     Copyright (C) 2019  ja-pg
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.


def generate_dict(nodes_filename=None, names_filename=None):
    """ Builds the following dictionary from NCBI taxonomy nodes.dmp and
    names.dmp files
    """
    standard_ranks = stdranks = ['species', 'genus', 'family', 'order', 'class', 'phylum', 'superkingdom']
    if nodes_filename and names_filename:

        taxid2name = {}
        with open(names_filename) as names_file:
            for line in names_file:
                line = [elt for elt in line.split('|')]
                if line[3] == "\tscientific name\t":
                    taxid = int(line[0].strip('\t'))
                    taxid2name[taxid] = line[1][1:-1].strip('\t')

        dic = {}  # 0 - taxidname, 1 - rank, 2 - parent taxid, 3 - childrens
        with open(nodes_filename) as nodes_file:
            for line in nodes_file:
                line = [elt for elt in line.split('|')][:3]
                taxid = int(line[0].strip('\t'))
                parent_taxid = int(line[1].strip('\t'))

                if taxid in dic:  # 18204/1308852
                    dic[taxid][1] = line[2][1:-1].strip('\t')
                    dic[taxid][2] = parent_taxid
                else:  # 1290648/1308852
                    dic[taxid] = [taxid2name[taxid], line[2][1:-1].strip('\t'), parent_taxid, []]
                    del taxid2name[taxid]

                try:  # 1290648/1308852
                    dic[parent_taxid][3].append(taxid)
                except KeyError:  # 18204/1308852
                    dic[parent_taxid] = [taxid2name[parent_taxid], None, None, [taxid]]
                    del taxid2name[parent_taxid]

        # to avoid infinite loop
        root_children = dic[1][3]
        root_children.remove(1)
        dic[1][2] = None
        dic[1][3] = root_children
        return dic


def get_name(dic, taxid):
    return dic[taxid][0]


def get_ascendants_with_ranks_and_names(dic, taxid, only_std_ranks):
    standard_ranks = ['species', 'genus', 'family', 'order', 'class', 'phylum', 'superkingdom', 'no rank']

    taxid = int(taxid)

    try:
        lineage = {dic[taxid][1]: (taxid, dic[taxid][0])}
        while dic[taxid][2] is not None:
            taxid = dic[taxid][2]
            lineage[dic[taxid][1]] = (taxid, dic[taxid][0])  # rank = taxid, name
        if only_std_ranks:
            std_lineage = {}
            for lvl in lineage:
                if lvl in standard_ranks:
                    std_lineage[lvl] = lineage[lvl]
            lineage = std_lineage
    except KeyError:  # Not in the dictionary
        lineage = {"unknown": (taxid, "unknown")}
    return lineage


def get_tax_ids(mapped_seq_rdd):
    return mapped_seq_rdd.map(lambda x: x[1][0]).collect()


def map_phylogenetic_info(mapped_seq_rdd, nodes_dmp, names_dmp, sc, broadcast=True):
    """ create a fasta file locally from the given rdd using collect
                    :param seq_rdd: the rdd containing the sequences
                    :param filename: the path to save the fasta file
                    :return:
            """
    # create a dictionary
    tax_dic = generate_dict(nodes_dmp, names_dmp)
    if broadcast and sc:
        tax_dic_bc = sc.broadcast(tax_dic)
        # on a cluster
        ptree_rdd = mapped_seq_rdd.map(lambda x:
                                       (get_ascendants_with_ranks_and_names(tax_dic_bc.value, x[1][0], True), x[1]))
    else:
        all_tax_ids = get_tax_ids(mapped_seq_rdd)
        # use local dictionary to make a new dictionary that maps taxid to ascendants
        dic_map = {}
        for taxid in all_tax_ids:
            dic_map[taxid] = get_ascendants_with_ranks_and_names(tax_dic, taxid, True)
        ptree_rdd = mapped_seq_rdd.map(lambda x: (dic_map[x[1][0]], x[1]))
    return ptree_rdd


def get_unknowns(ptree_rdd):
    return ptree_rdd.filter(lambda x: "unknown" in x[0])


def remove_unknowns(ptree_rdd):
    n_seq = ptree_rdd.count()
    ptree_filtered_rdd = ptree_rdd.filter(lambda x: "unknown" not in x[0])
    m_seq = ptree_filtered_rdd.count()
    print("Removed sequences: ", n_seq - m_seq)
    return ptree_filtered_rdd
