#     reducer: The implementation of the sequence reduction algorithm using Spark combineByKey
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
from sparkseqreducer import stretcher
import os


# Reduce function using emboss stretcher (with wrapper)
def remove_overlaps(ranges):
    """
    Merges overlapping ranges of a list of lists of 2 elements
    the first element must be the start of the range
    the second element must be the end of the range,
    the ranges must be already sorted by the start of the range
    """
    result = []
    current_start_end = [-float("inf"), -float("inf")]
    for start_end in ranges:
        if start_end[0] > current_start_end[1]:
            # there is not an overlap when this happens, last range ends after the last one ended
            result.append(start_end)
            current_start_end = start_end
        else:
            # an overlap happens
            # replace last range, current_start_end[0] is guaranteed to be lower
            result[-1] = [current_start_end[0], start_end[1]]
            # use max just in case the next range end before last range ended
            current_start_end[1] = max(current_start_end[1], start_end[1])
    return result


def string_similarity(a, b):
    # len(a)==len(b)
    sim = 0.0
    for i in range(len(a)):
        if a[i] == b[i] and (a[i] != '-' and b[i] != '-'):
            sim += 1.0
    sim = sim / float(len(a))
    return sim


def align_reduce(sequences, seq_a, seq_b, threshold=0.95, stride=100):
    # align using stretcher, Comparison matrix must be on the same directory on all nodes
    seq_a_aligned, seq_b_aligned = stretcher.align(seq_a[0], seq_b[0],
                                                   os.path.join(os.path.dirname(__file__), "../data/EDNAFULL"))

    if len(seq_a_aligned) != len(seq_b_aligned):  # something went wrong while aligning
        return [""]

    else:
        # Explore the similarity between regions of size = stride
        diff_regions = []
        i = 0
        while i < len(seq_a_aligned):  # Check the similarity of all slices of size = stride
            start_end = [i, i + stride]
            if (string_similarity(seq_a_aligned[i:i + stride],
                                  seq_b_aligned[i:i + stride]) < threshold):  # check similarity in slice
                while i < len(seq_a_aligned) and (string_similarity(seq_a_aligned[i:i + stride],
                                                                    seq_b_aligned[i:i + stride]) < threshold):
                    # continue looking on adjacent slices
                    start_end[1] = i + stride
                    i = i + stride
                # if region is different by threshold, append region to the left and region to the right to final
                # sequence
                start_end[0] = max(0, start_end[0] - stride)
                start_end[1] = start_end[1] + stride
                diff_regions.append(start_end)
            else:
                i = i + stride
        # search for overlaps, reduce to one sequence
        # optimize using an interval tree?
        diff_regions = remove_overlaps(diff_regions)
        # use the list of ranges to get the unique parts of the aligned sequence
        for start_end in diff_regions:
            region = seq_b_aligned[start_end[0]:start_end[1]].replace('-',
                                                                      '')  # region, includes stride to the left and
            # stride to the right
            sequences.append((region, seq_b[1]))

    return sequences


# SPARK RDD createCombiner for "combineByKey" function
def seq_to_list(seq_a):
    # Receives a Value (tuple of (seq, header)) and turns it into a list
    return [seq_a]


# SPARK RDD mergeValue for "combineByKey" function
def pairwise_reduction_merge_sequence(sequences, seq_b, threshold=0.95, stride=100):
    # pop the longest sequence from Sequences
    seq_a = sequences.pop(0)

    # Check which one is the longest sequence
    if len(seq_b[0]) > len(seq_a[0]):
        seq_a, seq_b = seq_b, seq_a

    # Save original longest sequence
    sequences.insert(0, seq_a)

    return align_reduce(sequences, seq_a, seq_b, threshold, stride)


# SPARK RDD mergeCombiners for "combineByKey" function
def pairwise_merge_reduce_sequences(sequences_a, sequences_b, threshold=0.95, stride=100):
    # pop the longest sequence from SequencesA, and SequencesB
    seq_a = sequences_a.pop(0)
    seq_b = sequences_b.pop(0)

    # Extend Sequences A with Sequences B
    sequences_a.extend(sequences_b)

    # Check which one is the longest sequence
    if len(seq_b[0]) > len(seq_a[0]):
        seq_a, seq_b = seq_b, seq_a

    # Save the longest sequence of SequencesA or SequencesB at the first position of SequencesA
    sequences_a.insert(0, seq_a)

    return align_reduce(sequences_a, seq_a, seq_b, threshold, stride)


def reduce(pmapped_rdd, rank="species"):
    """ create a rdd that combines sequences with the same chosen rank identified by his TaxID
                        :param pmapped_rdd: the rdd containing the sequences and the phylogenetic info dict as key
                        :param rank: the rank to use as key to combine and reduce the sequences in pmapped_rdd
                        :return: reduced_sequence_rdd a rdd generated by spark combineByKey
                """
    # create a key/value rdd
    # key = rank tax_id, value = (seq, fastaHeader)
    tax_seq_rdd = pmapped_rdd.map(lambda x: (x[0][rank][0], (x[1][2], x[1][1]))).partitionBy(12)

    # CombineByKey, reduce all sequences sharing rank TaxID
    reduced_sequence_rdd = tax_seq_rdd.combineByKey(seq_to_list,
                                                    pairwise_reduction_merge_sequence,
                                                    pairwise_merge_reduce_sequences)
    return reduced_sequence_rdd
