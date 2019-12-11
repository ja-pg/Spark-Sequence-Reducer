#     sparkseqreducer: Contains the code to parse arguments and run the Spark Sequence Reducer algorithm
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
import argparse
import os

from pyspark import SparkContext, SparkConf
from sparkseqreducer.phylogenetic_map import map_phylogenetic_info
from sparkseqreducer.reducer import reduce
from sparkseqreducer.sequence_io import fasta_to_rdd, rdd_to_fasta_local
from sparkseqreducer.taxid_map import map_accession_to_taxid


def check_config_files():
    config = True
    db_path = os.path.isfile(os.path.join(os.path.abspath(os.path.dirname(__file__)), "data", "gb.db"))
    nodes_path = os.path.isfile(os.path.join(os.path.abspath(os.path.dirname(__file__)), "data", "nodes.dmp"))
    names_path = os.path.isfile(os.path.join(os.path.abspath(os.path.dirname(__file__)), "data", "names.dmp"))
    if not db_path:
        print("The accession to taxid database has not been found in data.")
        config = False
    if not nodes_path:
        print("nodes.dmp not found in data.")
        config = False
    if not names_path:
        print("names.dmp not found in data.")
        config = False
    return config


def get_params():

    parser = argparse.ArgumentParser(description='Spark Sequence Reducer.')
    parser.add_argument("infile",
                        help="Path to the input file containing the sequences to reduce.")
    parser.add_argument("outfile",
                        help="Path to the output file to save the resulting reduced sequences.")
    parser.add_argument("-r", "--rank", required=False, default="species", const="species", nargs="?",
                        choices=['species', 'genus', 'family', 'order', 'class', 'phylum', 'superkingdom'],
                        help="The taxonomic rank to use for the reduction.\nMust be one of the following: "
                             "species, genus, family, order, class, phylum or superkingdom")
    args = parser.parse_args()
    return vars(args)


if __name__ == "__main__":

    # Get all arguments
    std_ranks = ['species', 'genus', 'family', 'order', 'class', 'phylum', 'superkingdom']
    params = get_params()

    if not check_config_files():
        print("Spark Sequence Reducer has not been configured, exiting...")
        exit(1)

    conf = SparkConf().setAppName("Pyspark Sequence Reducer")
    sc = SparkContext(conf=conf)

    print(sc.defaultParallelism)

    print("Loading sequence files...")
    # Create a rdd from the fasta file indicated by a path
    fasta_rdd = fasta_to_rdd(params["infile"], sc)
    # fasta_rdd is a rdd(accession.version, [header, sequence])
    original_n_seq = fasta_rdd.count()
    print(original_n_seq, " sequences have been loaded in ", fasta_rdd.getNumPartitions(), "partitions")
    print("Done")

    print("Getting sequence TaxIDs")
    # Map every sequence in the rdd with a TaxID
    seq_rdd = map_accession_to_taxid(fasta_rdd)
    # seq_rdd = rdd((key=accession_version, v=[tax_id, header, sequence]))
    # Filter Sequences without assigned TaxID
    seq_rdd = seq_rdd.filter(lambda x: x[1][0] != "None")
    taxid_mapped_n_seq = seq_rdd.count()
    print(taxid_mapped_n_seq, " sequences have been mapped to their TaxID, ",
          original_n_seq - taxid_mapped_n_seq, " not mapped and will be filtered out.")
    print("Done")

    print("Getting sequence phylogenetic information...")
    # Use the TaxID from each sequence to create the phylogenetic information
    # The standard ranks are: 'species', 'genus', 'family', 'order', 'class', 'phylum', 'superkingdom', 'no rank'
    # The respective ranks of the sequence are returned as a dictionary:
    # phylo_dict={'superkingdom': (tax_id, name), 'no rank': (tax_id, name), 'genus': (tax_id, name),
    # 'species': (tax_id, name) 'family': (tax_id, name)}
    use_broadcast = True
    phylo_rdd = map_phylogenetic_info(seq_rdd,
                                      os.path.join(os.path.dirname(__file__), "data", "nodes.dmp"),
                                      os.path.join(os.path.dirname(__file__), "data", "names.dmp"),
                                      sc, broadcast=use_broadcast)
    # phylo_rdd = rdd((key=phylo_dict, v=[tax_id, header, sequence]))
    # filter not assigned
    phylo_rdd = phylo_rdd.filter(lambda x: "unknown" not in x[0])
    phylo_mapped_n_seq = phylo_rdd.count()
    print("The phylogenetic information of", phylo_mapped_n_seq, " sequences has been mapped")
    print("Done")

    print("Starting Reduction algorithm...")
    # Reduce the sequences to the given rank
    redseq_rdd = reduce(phylo_rdd, params["rank"])
    print("The sequences have benn reduced to ", redseq_rdd.count(), " ranks")
    print("Done")

    print("Saving results...")
    # Persist the generated reduced sequences
    local = True
    if local:
        rdd_to_fasta_local(redseq_rdd, params["outfile"])
    else:
        # For future saves methods
        pass
    print("Done")

    sc.stop()
    print("The reduced sequence database has been generated and saved, exiting")
