#!/usr/bin/env bash

# Create directories if they don't exist
mkdir -p metadata-excel metadata-excel-wrong metadata-jsonp

# Read from list.txt, run the command in parallel for each line, and output the results to an Excel file in the metadata-excel directory
cat list.txt | parallel -j8 'echo {}; datasets summary genome accession --as-json-lines {} | dataformat excel genome --outputfile ./metadata-excel/{}.xlsx'

# Read from wrong_accession.txt, run the command in parallel for each line, and output the results to an Excel file in the metadata-excel-wrong directory
cat wrong_accession.txt | parallel -j8 'echo {}; datasets summary genome accession --as-json-lines {} | dataformat excel genome --outputfile ./metadata-excel-wrong/{}.xlsx'

# Read from list.txt, run the command in parallel for each line, and output the results to a JSON file in the metadata-jsonp directory
cat list.txt | parallel -j8 'echo {}; datasets summary genome accession {} > metadata-jsonp/{}.json'