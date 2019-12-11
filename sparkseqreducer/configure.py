#     configure: Functions to create and configure a local db using accession2taxid data
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
from sparkseqreducer import taxid_map


def configure_db(nucl_data_file):
    conn = taxid_map.create_connection("../data/gb.db")
    if conn is not None:
        taxid_map.create_accession2taxid_db(conn, nucl_data_file)
        taxid_map.close_connection(conn)
    return 0


if __name__ == "__main__":
    print("Creating a database using nucl_gb.accession2taxid.gz ...")
    configure_db("../data/nucl_gb.accession2taxid.gz")
    print("The database has been created")
