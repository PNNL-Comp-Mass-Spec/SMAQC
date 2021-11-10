SMAQC
=====

SMAQC (Software Metrics for Analysis of Quality Control) computes quality metrics 
for an LC-MS/MS dataset. The quality metrics are based on the metrics proposed 
by Rudnick and Stein, as described in "Performance metrics for liquid 
chromatography-tandem mass spectrometry systems in proteomics analyses.", \
Mol Cell Proteomics. 2010 Feb; 9(2):225-41. doi: 10.1074/mcp.M900223-MCP200.
([PMID: 19837981](https://pubmed.ncbi.nlm.nih.gov/19837981/) and [PMC2830836](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2830836/))

SMAQC requires that the dataset first be processed with [MASIC](https://github.com/PNNL-Comp-Mass-Spec/MASIC/releases), then processed with 
[MSGF+](https://github.com/MSGFPlus/msgfplus/releases) or [X!Tandem](https://www.thegpm.org/tandem/).
The MSGF+ or X!Tandem results must be post-processed with the [PeptideHitResultsProcessor](https://github.com/PNNL-Comp-Mass-Spec/PHRP) (PHRP).

SMAQC reads the data from the _syn.txt file along with the parallel
text files created by PHRP.  It uses this information to compute peptide
count related metrics (peptides are filtered on MSGF_SpecProb
less than 1E-12). SMAQC also reads the data from the _ScanStats.txt,
_SICstats.txt, and _ScanStatsEx.txt files created by MASIC
to determine chromatography and scan-related metrics.

## Console Switches

```
SMAQC.exe
 DatasetFolderPath [/O:OutputFilePath] [/DB:DatabaseFolder]
 [/I:InstrumentID] [/M:MeasurementsFile]
```

DatasetFolderPath specifies path to the folder with the dataset(s) to process; use quotes if spaces

Use /O to specify the output file path. If /O is not used, results will only be stored in the SQLite database\
Examples: `/O:Metrics.txt` or `/O:"C:\Results Folder\Metrics.txt"`

Use /DB to specify where the SQLite database should be created (default is with the .exe)

Use /I to specify an instrument ID (number or text); defaults to /I:1

Use /M to specify the path to the XML file containing the measurements to run.\
If /M is not used, all of the metrics will be computed


## Contacts

Written by Matthew Monroe, in collaboration with computer science students
at Washington State University for the Department of Energy (PNNL, Richland, WA) in 2012 \
E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov \
Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://panomics.pnnl.gov/ or https://www.pnnl.gov/integrative-omics

## License

SMAQC is licensed under the 2-Clause BSD License; 
you may not use this program except in compliance with the License.  You may obtain 
a copy of the License at https://opensource.org/licenses/BSD-2-Clause
