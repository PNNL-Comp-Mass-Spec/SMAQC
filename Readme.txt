SMAQC computes quality metrics for a LC-MS/MS dataset.  The software
requires that the dataset first be processed with MASIC, then processed
with MSGF+ or X!Tandem.  The MSGF+ or X!Tandem results must be
post-processed with the PeptideHitResultsProcessor (PHRP).

SMAQC reads the data from the _syn.txt file along with the parallel
text files created by PHRP.  It uses this information to compute peptide
count related metrics.  SMAQC also reads the data from the
_ScanStats.txt, _SICstats.txt, and _ScanStatsEx.txt files created by
MASIC to determine chromatography and scan-related metrics.

The quality metrics computed by SMAQC are based on the metrics proposed
by Rudnick and Stein, as described in "Performance metrics for liquid
chromatography-tandem mass spectrometry systems in proteomics analyses.",
Mol Cell Proteomics. 2010 Feb;9(2):225-41. doi: 10.1074/mcp.M900223-MCP200.

Program syntax:
SMAQC.exe
 DatasetFolderPath [/O:OutputFilePath] [/DB:DatabaseFolder]
 [/I:InstrumentID] [/M:MeasurementsFile]

DatasetFolderPath specifies path to the folder with the dataset(s) to process; use quotes if spaces

Use /O to specify the output file path. If /O is not used, then 
results will only be stored in the SQLite database
Examples: /O:Metrics.txt   or   /O:"C:\Results Folder\Metrics.txt"

Use /DB to specify where the SQLite database should be created (default is with the .exe)
Use /I to specify an instrument ID (number or text); defaults to /I:1

Use /M to specify the path to the XML file containing the measurements to run.
If /M is not used, then all of the metrics will be computed

-------------------------------------------------------------------------------
Written by Matthew Monroe, in collaboration with computer science students 
at Washington State University for the Department of Energy (PNNL, Richland, WA) in 2012

E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
Website: http://panomics.pnnl.gov/ or http://omics.pnl.gov or http://www.sysbio.org/resources/staff/
-------------------------------------------------------------------------------

Licensed under the Apache License, Version 2.0; you may not use this file except 
in compliance with the License.  You may obtain a copy of the License at 
http://www.apache.org/licenses/LICENSE-2.0

