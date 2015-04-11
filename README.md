# qsdata2
qsdata2 collects information about data storage from various commands on a linux box, and outputs it in csv-formatted file

This enables an enterprise csvfile-based ETL environment to monitor linux servers and desktops

The program works with local disk (including HP controllers), NFS4 imports and exports and ISCSI imports and exports.
Various heuristics are applied when combining the individual command outputs into a single matrix, in order to make disparate data appear consistent