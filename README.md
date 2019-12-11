#Spark Sequence Reducer

Spark Sequence Reducer is an application designed to reduce the size of whole genome reference 
sequence databases typically used in sequence analysis tools. Spark Sequence Reducer
uses Apache Spark and the MapReduce like model to achieve 
high scalability, this makes possible the reduction of large sequence databases in an acceptable
time frame, that otherwise, wouldn't be possible.

Spark Sequence Reducer uses taxonomic classification data of the sequences in the reference 
databases to find and remove highly similar regions in sequences sharing a taxon of a determined rank. 
The algorithm saves similar data only once, but keeps unique regions of every sequence, 
avoiding data loss.

## Requirements

* A Python 3 or Python 2 (Python 3 preferred) on all nodes of your cluster.
* GCC for C code compilation.
* A configured Apache Spark installation.

## Compiling Stretcher

Spark Sequence Reducer uses Stretcher, a global alignment algorithm to find highly similar regions of 
closely related sequences. Stretcher only uses linear space to find the optimal pairwise 
global alignment. The low memory used by the algorithm makes possible the
 alignment in parallel of a larger number of sequences as part of the reduction process.
 
A precompiled dynamic library (stretcher.so) is provided, but you can compile it manually if needed using gcc:
 
```
gcc -I Headers/ -I Sources/ -I pcre/ -o stretcher pcre/*.c Sources/*.c stretcher.c -lm
```


Stretcher is part of [Emboss](), and has been modified for standalone use. 
Spark Sequence Reducer wraps Stretcher using ctypes to facilitate calling the program within python code.

## Configuration instructions
* Compile stretcher, the global alignment algorithm (See _Compiling Stretcher_).
* Download [nucl_gb.accession2taxid.gz](ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/accession2taxid/nucl_gb.accession2taxid.gz),
 [taxdump.tar.gz](ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz) to the `data/` directory 
of spark sequence reducer (Either manually or running `ncbitax-download.sh`).
* Run `configure-gb.sh` to create and configure secondary files using the taxonomy files.
* If running on multiple machines, replicate the spark sequence reducer directory to all the nodes in your cluster.

## Running Spark Sequence Reducer

 To run the program using spark-submit:
 
```
$SPARK_HOME/bin/spark-submit [spark-options] sparkseqreducer.py [-h]
                          [-r [{species,genus,family,order,class,phylum,superkingdom}]]
                          infile outfile
```
Arguments:
```
positional arguments:
  infile                Path to the input file containing the reference sequences to
                        reduce.
  outfile               Path to the output file to save the resulting reduced
                        sequences.

optional arguments:
  -h, --help            show this help message and exit
  -r [{species,genus,family,order,class,phylum,superkingdom}], --rank [{species,genus,family,order,class,phylum,superkingdom}]
                        The taxonomic rank to use for the reduction. Must be
                        one of the following: species, genus, family, order,
                        class, phylum or superkingdom
```

For example, to reduce a fasta file to output.fasta with species selected as the taxonomic rank for reduction:
```$SPARK_HOME/bin/spark-submit sparkseqreducer.py --rank species example.fasta $HOME/output```