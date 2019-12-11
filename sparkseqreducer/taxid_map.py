#     taxid_map: Functions to call and manage a local db that contains taxonomic ids for every accession number
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
import csv
import sqlite3
import gzip
import os
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def close_connection(conn):
    try:
        conn.close()
    except Error as e:
        print(e)


def create_accession2taxid_db(conn, filepath):
    """ create a table and insert into it using a compressed file
        :param conn: Connection object
        :param filepath: the path to the compressed file
        :return:
        """
    with gzip.open(filepath, "rt") as f:
        #create the table sql
        sql_create_map_table = """ CREATE TABLE IF NOT EXISTS accession_taxid_map (
                                        accession TEXT,
                                        accession_version TEXT,
                                        taxid INTEGER,
                                        gi INTEGER); """
        #insert into sql
        sql_insert_csv_table = "INSERT INTO accession_taxid_map values (?, ?, ?, ?);"
        #create index sql
        sql_create_index = "CREATE UNIQUE INDEX idx ON accession_taxid_map(accession_version);"
        try:
            c = conn.cursor()
            # create the table
            c.execute(sql_create_map_table)
            conn.commit()

            #fill table using csv file
            csv_reader = csv.reader(f, delimiter = '\t')
            next(csv_reader)
            for row in csv_reader:
                c.execute(sql_insert_csv_table, row)
            conn.commit()

            #create index
            c.execute(sql_create_index)
            conn.commit()
        except Error as e:
            print(e)
            return -1
        return 0


def get_taxid_from_accession_db(accession):
    try:
        # (the db needs to be manually replicated to all worker nodes)
        # The db needs to be indexed for best performance
        conn = create_connection(os.path.join(os.path.dirname(__file__), "../data", "gb.db"))
        c = conn.cursor()
        c.execute("""SELECT * from accession_taxid_map where accession_version = '""" + str(accession) + """'""")
        rows = c.fetchall()
        for row in rows:
            return row[2]
    except Error as e:
        print(e)
        return -1
    finally:
        conn.close()
    return 0


def map_accession_to_taxid(seq_rdd):
    """ create a rdd that contains the taxonomy id of the sequence as part of his value
                    :param seq_rdd: the rdd((key=accession_version, v=[header, sequence]))
                    :return: seq_rdd: a rdd((key=accession_version, v=[tax_id, header, sequence]))
            """
    # Use the given key=accession.version to map a TaxID using a locally stored database
    return seq_rdd.map(lambda x: (x[0], [str(get_taxid_from_accession_db(x[0])), x[1][0], x[1][1]]))


def join_map_accession_to_taxid(seq_rdd,  taxid_rdd):
    # Join seq_rdd with a rdd that contains all TaxIDs using accession number.
    # This function works better when using multiple nodes and when you can use a broadcasted join (more memory usage)
    mapped_rdd = seq_rdd.join(taxid_rdd)

    return mapped_rdd


def count_none_mapppings(mapped_seq_rdd):
    return mapped_seq_rdd.filter(lambda x: x[1][0] == 'None').count()


def remove_none_mappings(mapped_seq_rdd):
    return mapped_seq_rdd.filter(lambda x: x[1][0] != "None")
