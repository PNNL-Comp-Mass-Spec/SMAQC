using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace SMAQC
{
    internal class DataFileFormatter
    {
        // Ignore Spelling: frag, xt, hyperscore

        /// <summary>
        /// List of files that need to be reformatted
        /// e.g. ScanStats, ScanStatsEx, SICStats
        /// Keys are filename suffixes
        /// Values are the known columns that we want to load from the file
        /// </summary>
        /// <remarks>
        /// Column names are scrubbed to remove spaces, parentheses, and slashes
        /// </remarks>
        private readonly Dictionary<string, List<string>> mValidFilesToReFormat;

        /// <summary>
        /// Temporary file path with scrubbed data
        /// </summary>
        private string mTempFilePath = string.Empty;

        /// <summary>
        /// Constructor
        /// </summary>
        public DataFileFormatter()
        {
            // ReSharper disable once UseObjectOrCollectionInitializer
            mValidFilesToReFormat = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

            // ScanStats columns
            mValidFilesToReFormat.Add("ScanStats",
                FieldCleaner(new List<string> {
                    "Dataset",
                    "ScanNumber",
                    "ScanTime",
                    "ScanType",
                    "TotalIonIntensity",
                    "BasePeakIntensity",
                    "BasePeakMZ",
                    "BasePeakSignalToNoiseRatio",
                    "IonCount",
                    "IonCountRaw",
                    "ScanTypeName"}));

            // ScanStatsEx columns
            mValidFilesToReFormat.Add("ScanStatsEx",
                FieldCleaner(new List<string> {
                    "Dataset",
                    "ScanNumber",
                    "Ion Injection Time (ms)",
                    "Scan Segment",
                    "Scan Event",
                    "Master Index",
                    "Elapsed Scan Time (sec)",
                    "Charge State",
                    "Monoisotopic M/Z",
                    "MS2 Isolation Width",
                    "FT Analyzer Settings",
                    "FT Analyzer Message",
                    "FT Resolution",
                    "Conversion Parameter B",
                    "Conversion Parameter C",
                    "Conversion Parameter D",
                    "Conversion Parameter E",
                    "Collision Mode",
                    "Scan Filter Text",
                    "Source Voltage (kV)",
                    "Source Current (uA)"}));

            // SICStats columns
            mValidFilesToReFormat.Add("SICStats",
               FieldCleaner(new List<string> {
                    "Dataset",
                    "ParentIonIndex",
                    "MZ",
                    "SurveyScanNumber",
                    "FragScanNumber",
                    "OptimalPeakApexScanNumber",
                    "PeakApexOverrideParentIonIndex",
                    "CustomSICPeak",
                    "PeakScanStart",
                    "PeakScanEnd",
                    "PeakScanMaxIntensity",
                    "PeakMaxIntensity",
                    "PeakSignalToNoiseRatio",
                    "FWHMInScans",
                    "PeakArea",
                    "ParentIonIntensity",
                    "PeakBaselineNoiseLevel",
                    "PeakBaselineNoiseStDev",
                    "PeakBaselinePointsUsed",
                    "StatMomentsArea",
                    "CenterOfMassScan",
                    "PeakStDev",
                    "PeakSkew",
                    "PeakKSStat",
                    "StatMomentsDataCountUsed"}));

            // xt columns (X!Tandem)
            mValidFilesToReFormat.Add("xt",
                FieldCleaner(new List<string> {
                    "Result_ID",
                    "Group_ID",
                    "Scan",
                    "Charge",
                    "Peptide_MH",
                    "Peptide_Hyperscore",
                    "Peptide_Expectation_Value_Log(e)",
                    "Multiple_Protein_Count",
                    "Peptide_Sequence",
                    "DeltaCn2",
                    "y_score",
                    "y_ions",
                    "b_score",
                    "b_ions",
                    "Delta_Mass",
                    "Peptide_Intensity_Log(I)",
                    "DelM_PPM"}));

            // xt_ResultToSeqMap columns
            mValidFilesToReFormat.Add("xt_ResultToSeqMap",
                FieldCleaner(new List<string> {
                    "Result_ID",
                    "Unique_Seq_ID"}));

            // xt_SeqToProteinMap columns
            mValidFilesToReFormat.Add("xt_SeqToProteinMap",
               FieldCleaner(new List<string> {
                    "Unique_Seq_ID",
                    "Cleavage_State",
                    "Terminus_State",
                    "Protein_Name",
                    "Protein_Expectation_Value_Log(e)",
                    "Protein_Intensity_Log(I)"}));
        }

        /// <summary>
        /// Destructor
        /// </summary>
        ~DataFileFormatter()
        {
            // Ensure temp file does not still exist
            if (!string.IsNullOrEmpty(mTempFilePath) && File.Exists(mTempFilePath))
                DeleteTempFile(mTempFilePath);
        }

        #region "Properties"

        /// <summary>
        /// Temporary file path
        /// </summary>
        public string TempFilePath => mTempFilePath;

        #endregion

        /// <summary>
        /// This function checks each file to see if it should be re-formatted
        /// If yes, data in the file is processed and stored in mTempFilePath
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="dataset"></param>
        /// <returns>True if reformatted, false if not</returns>
        public bool HandleFile(string filePath, string dataset)
        {
            // Check if is valid file
            var knownColumns = GetColumnsForKnownFile(filePath, dataset);

            if (knownColumns.Count == 0)
                return false;

            // Maps observed column index to desired column index in db (-1 means do not store the given column in the db)

            // Pad hash table with pointer to correct values
            var columnCount = MapColumnsToKnownFields(filePath, out var columnIndexMap, knownColumns);

            // Obtain a temp file path
            mTempFilePath = Path.GetTempFileName();

            // Call internal rebuild function
            RebuildFile(filePath, mTempFilePath, columnCount, columnIndexMap);

            return true;
        }

        /// <summary>
        /// Rebuild filename using padding to ensure all columns match up
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <param name="targetFilePath"></param>
        /// <param name="columnCount">Total number of columns in the data file</param>
        /// <param name="columnIndexMap">Maps observed column index to desired column index in DB (-1 means do not store the given column in the DB)</param>
        private void RebuildFile(string sourceFilePath, string targetFilePath, int columnCount, IReadOnlyList<int> columnIndexMap)
        {
            var headerParsed = false;

            // Split given data files by tab
            var delimiters = new[] { '\t' };

            var knownColumnCount = (from item in columnIndexMap where item > -1 select item).Count();

            // Open files used for r/w
            using (var sourceFileReader = new StreamReader(new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            using (var updatedFileWriter = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                // Loop through each line
                while (!sourceFileReader.EndOfStream)
                {
                    var line = sourceFileReader.ReadLine();
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    var dataToWrite = new List<string>();

                    // Do split operation
                    List<string> parts;

                    if (!headerParsed)
                    {
                        // Header line
                        // Clean column names to ensure database compatibility
                        parts = FieldCleaner(line.Split(delimiters, StringSplitOptions.None).ToList());
                        headerParsed = true;
                    }
                    else
                    {
                        parts = line.Split(delimiters, StringSplitOptions.None).ToList();
                    }

                    // Loop through each part
                    for (var i = 0; i < parts.Count; i++)
                    {
                        if (i == columnCount)
                        {
                            // Too many columns for this line; ignore them
                            break;
                        }

                        if (columnIndexMap[i] > -1)
                        {
                            // Known column; add it
                            dataToWrite.Add(parts[i]);
                        }
                    }

                    while (dataToWrite.Count < knownColumnCount)
                    {
                        // Missing columns for this line; add them
                        dataToWrite.Add(string.Empty);
                    }

                    // Write line
                    updatedFileWriter.WriteLine(string.Join("\t", dataToWrite));
                }
            }
        }

        /// <summary>
        /// This function defines a mapping between the column index in the file vs. the column index to which the data should be written in the database
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="columnIndexMap">Maps observed column index to desired column index in DB (-1 means do not store the given column in the DB)</param>
        /// <param name="knownColumns"></param>
        /// <returns>Total number of columns in the input file</returns>
        private int MapColumnsToKnownFields(string filePath, out List<int> columnIndexMap, IReadOnlyList<string> knownColumns)
        {
            int columnCount;
            columnIndexMap = new List<int>();

            using (var file = new StreamReader(filePath))
            {
                var line = file.ReadLine();

                if (string.IsNullOrWhiteSpace(line))
                    return 0;

                // Split given data files by tab
                var delimiters = new[] { '\t' };

                // Do split operation
                // Clean fields to ensure consistency
                var parts = FieldCleaner(line.Split(delimiters, StringSplitOptions.None).ToList());

                // Set columnCount
                columnCount = parts.Count;

                // Loop through each column
                foreach (var column in parts)
                {
                    // Search for column name that is found in our file line
                    var index = FindIndexOfColumnName(knownColumns, column);

                    // If found [not -1]
                    if (index > -1)
                    {
                        // Console.WriteLine("store i={0} && index={1}", i, index);
                        // Store index of our part_id as index value as per function requirements [see details above each function name]
                        columnIndexMap.Add(index);
                    }
                    else
                    {
                        // Not found ... add -1 to indicate we will skip this column
                        // Console.WriteLine("store i={0} && index={1}", i, index);
                        columnIndexMap.Add(-1);
                    }
                }
            }

            return columnCount;
        }

        /// <summary>
        /// Look for columnName in knownColumns
        /// </summary>
        /// <param name="knownColumns"></param>
        /// <param name="columnName"></param>
        /// <returns>Return the index if a match or -1 if no match</returns>
        private int FindIndexOfColumnName(IReadOnlyList<string> knownColumns, string columnName)
        {
            for (var i = 0; i < knownColumns.Count; i++)
            {
                if (string.Equals(knownColumns[i], columnName, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }

            return -1;
        }

        /// <summary>
        /// Delete the file if it exists
        /// </summary>
        /// <param name="filePath"></param>
        private void DeleteTempFile(string filePath)
        {
            try
            {
                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                }
            }
            catch
            {
                // Ignore exceptions
            }
        }

        /// <summary>
        /// Is this a valid file to reformat (checks mValidFilesToReFormat list)
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="dataset"></param>
        private List<string> GetColumnsForKnownFile(string filename, string dataset)
        {
            // Get filename without extension
            var filenameNoExtension = Path.GetFileNameWithoutExtension(filename);

            if (string.IsNullOrWhiteSpace(filenameNoExtension))
                return new List<string>();

            // Remove the dataset name from the filename
            // Example result: ScanStats or ScanStatsEx
            var filenamePart = filenameNoExtension.Substring(dataset.Length + 1);

            if (mValidFilesToReFormat.TryGetValue(filenamePart, out var knownColumns))
            {
                return knownColumns;
            }

            return new List<string>();
        }

        /// <summary>
        /// Examine column names to ensure database compatibility
        /// </summary>
        /// <param name="columnNames"></param>
        /// <returns>Updated column names</returns>
        /// <remarks>Remove spaces, parentheses, / and more</remarks>
        private List<string> FieldCleaner(IEnumerable<string> columnNames)
        {
            var updatedColumnNames = new List<string>();

            foreach (var column in columnNames)
            {
                // Step #1 remove pairs of parentheses, e.g. Ion Injection Time (ms)
                var firstIndex = column.IndexOf(" (", StringComparison.Ordinal);
                var lastIndex = column.IndexOf(")", StringComparison.Ordinal);

                string updatedName;

                if (firstIndex > 0 && lastIndex > firstIndex)
                {
                    updatedName = column.Remove(firstIndex, lastIndex - firstIndex + 1);
                }
                else
                {
                    updatedName = string.Copy(column);
                }

                // Step #2 replace all spaces with underscores
                updatedName = updatedName.Replace(" ", "_");

                // Step #3 replace all forward slashes with empty string, e.g. change m/z to mz
                updatedName = updatedName.Replace("/", string.Empty);

                updatedColumnNames.Add(updatedName);
            }

            return updatedColumnNames;
        }
    }
}
