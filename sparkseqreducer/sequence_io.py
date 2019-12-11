#     sequence_io: Functions to read sequence files to rdd and write rdd to sequence files
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

import re


def fasta_to_rdd(file_loc, sc):
    """ create a rdd using a fasta file from the given path and a sparkcontext
            :param file_loc: the path to the the fasta file/files
            :param sc: the spark context to use to create the rdd
            :return: rdd(accession.version, [header, sequence])
    """
    # Read the files in the directory using newAPIHadoopFile with double line skip as delimiter
    # Split the sequences (fasta format: header+multi-line sequence) in a multi-fasta file or multiple fasta files
    fasta_files_rdd = sc.newAPIHadoopFile(
        file_loc,
        inputFormatClass="org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
        keyClass="org.apache.hadoop.io.LongWritable", valueClass="org.apache.hadoop.io.Text",
        conf={"textinputformat.record.delimiter": "\n\n"})
    # newAPIHadoopFile generates a RDD of tuples with a arbitrary key for each element

    # A sequence on a fasta format is split on multiple lines,
    # the first newline splits the header from the beginning of the sequence
    # we have to replace the rest of newlines characters to have a complete sequence
    fasta_files_rdd = fasta_files_rdd.map(lambda x: x[1].replace('\n', '<', 1).replace('\n', '').split('<'))
    # rdd([header, sequence])

    # Use regular expressions to extract the accession version from the sequence header and use it as key
    # ----------------------------------------------------------------------------------------# Note: An accession
    # number applies to the complete record and is usually a combination of a letter(s) and numbers, such as a single
    # letter followed by five digits (e.g., U12345) or two letters followed by six digits Records from the RefSeq
    # database of reference sequences have a different accession number format that begins with two letters followed
    # by an underscore bar and six or more digits
    # ----------------------------------------------------------------------------------------#
    fasta_files_rdd = fasta_files_rdd.map(lambda x: (re.search("^>([_A-Za-z0-9.]+).*", x[0]).group(1), x))
    # rdd((key=accession.version, value=[header,sequence]))

    return fasta_files_rdd


def rdd_to_fasta_local(seq_rdd, filename):
    """ create a fasta file locally from the given rdd using collect
                :param seq_rdd: the rdd containing the sequences rdd((taxid,))
                :param filename: the path to save the fasta file
                :return:
        """
    seqs_rank_list = seq_rdd.collect()
    with open(filename + ".fasta", "w") as f:
        for taxids_seqs in seqs_rank_list:
            taxid = taxids_seqs[0]
            seqs = taxids_seqs[1]
            f.write(seqs[0][1] + ", Representative " + str(taxid) + "\n")
            # write first sequence
            for subseq in [seqs[0][0][j:j + 70] for j in range(0, len(seqs[0][0]), 70)]:
                f.write(subseq + "\n")
            f.write("\n")
            # write reduced sequences
            for i in range(1, len(seqs)):
                f.write(seqs[i][1] + ", Unique" + str(i) + "\n")
                for subseq in [seqs[i][0][j:j + 70] for j in range(0, len(seqs[i][0]), 70)]:
                    f.write(subseq + "\n")
                f.write("\n")
