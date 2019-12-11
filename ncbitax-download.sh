#!/bin/bash

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
acc2taxid="nucl_gb.accession2taxid.gz"
taxdmp="taxdmp.tar.gz"
names_file="names.dmp"
nodes_file="nodes.dmp"

function download_files() {
  #ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz
  #ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/accession2taxid/nucl_gb.accession2taxid.gz
  wget ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz
  wget ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/accession2taxid/nucl_gb.accession2taxid.gz
}

function extract_tax() {
  tar -zxvf  taxdump.tar.gz names.taxdmp nodes.dmp
}

#cd to the project path
cd "$parent_path/data/" || exit
#check if acc2taxid and taxdump are already in data
if [[ -e "$acc2taxid" && (-e "$taxdmp" || (-e "$names_file" && -e "$nodes_file")) ]]; then
  echo "$acc2taxid already on data."
  if [[ ! -e "$names_file" || ! -e "$nodes_file" ]]; then
      echo "extracting $names_file and $nodes_file to data..."
      extract_tax
  else
    echo "$names_file and $nodes_file already in data. continuing..."
  fi
else
  echo "$acc2taxid and $taxdmp not found in data:"
  echo "$acc2taxid and $taxdmp files are needed to configure this tool."
  echo "Do you wish to automatically download $acc2taxid and $taxdmp files from the ncbi servers?"
  select yn in "Yes" "No";
  do
    case $yn in
        Yes ) download_files && extract_tax
          break;;
        No ) exit;;
    esac
  done
fi


echo "All files have been created, you can now proceed to configuration"