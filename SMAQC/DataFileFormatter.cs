using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace SMAQC
{
    class DataFileFormatter
    {
        /// <summary>
        /// List of files that need to be reformatted
        /// e.g. ScanStats, ScanStatsEx, SICstats
        /// Keys are filename suffixes
        /// Values are the known fields that we want to load from the file
        /// </summary>
        /// <remarks>
        /// Field names are scrubbed to remove spaces, parentheses, and slashes
        /// </remarks>
        private readonly Dictionary<string, List<string>> mValidFilesToReFormat;

        /// <summary>
        /// Temporary file path with scrubbed data
        /// </summary>
        private string mTempFilePath = "";

        /// <summary>
        /// Constructor
        /// </summary>
        public DataFileFormatter()
        {
            // ReSharper disable once UseObjectOrCollectionInitializer
            mValidFilesToReFormat = new Dictionary<string, List<string>>(StringComparer.InvariantCultureIgnoreCase);

            // ScanStats fields
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

            // ScanStatsEx fields
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


            // SICstats fields
            mValidFilesToReFormat.Add("SICstats",
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


            // Xt fields
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


            // Xt_resulttoseqmap fields
            mValidFilesToReFormat.Add("xt_ResultToSeqMap",
                FieldCleaner(new List<string> {
                    "Result_ID",
                    "Unique_Seq_ID"}));


            // Xt_seqtoproteinmap fields
            mValidFilesToReFormat.Add("xt_SeqToProteinMap",
               FieldCleaner(new List<string> {
                    "Unique_Seq_ID",
                    "Cleavage_State",
                    "Terminus_State",
                    "Protein_Name",
                    "Protein_Expectation_Value_Log(e)",
                    "Protein_Intensity_Log(I)"}));

        }

        // Destructor
        ~DataFileFormatter()
        {
            // Ensure temp file does not still exist
            if (!string.IsNullOrEmpty(mTempFilePath) && File.Exists(mTempFilePath))
                ensure_temp_file_removed(mTempFilePath);
        }

        #region "Properties"

        /// <summary>
        /// Temporary file path
        /// </summary>
        public string TempFilePath
        {
            get { return mTempFilePath; }
        }

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
            var knownFields = GetFieldsForKnownFile(filePath, dataset);

            if (knownFields.Count == 0)
                return false;

            // Maps observed column index to desired column index in db (-1 means do not store the given column in the db)

            // Pad hash table with pointer to correct values
            var columnCount = MapColumnsToKnownFields(filePath, out var columnIndexMap, knownFields);

            // Obtain a temp file path
            mTempFilePath = Path.GetTempFileName();

            // Call internal rebuild function
            RebuildFile(filePath, mTempFilePath, columnCount, columnIndexMap);

            return true;
        }

        /// <summary>
        /// Rebuild filename using padding to ensure all fields match up
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="save_to_filename"></param>
        /// <param name="columnCount">Total number of columns in the data file</param>
        /// <param name="columnIndexMap">Maps observed column index to desired column index in DB (-1 means do not store the given column in the DB)</param>
        private void RebuildFile(string filename, string save_to_filename, int columnCount, List<int> columnIndexMap)
        {
            var headerParsed = false;

            // Split given data files by tab
            var delimiters = new[] { '\t' };

            var knownColumnCount = (from item in columnIndexMap where item > -1 select item).Count();

            // Open files used for r/w
            using (var sourceFileReader = new StreamReader(filename))
            {
                using (var updatedFileWriter = new StreamWriter(save_to_filename))
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
                            // Clean fields to ensure consistency
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

                            // If not an allowed field ignore
                            if (columnIndexMap[i] > -1)
                            {
                                dataToWrite.Add(parts[i]);
                            }

                        }

                        while (dataToWrite.Count < knownColumnCount)
                        {
                            // Missing columns for this line; add them
                            dataToWrite.Add("");
                        }

                        // Write line
                        updatedFileWriter.WriteLine(string.Join("\t", dataToWrite));

                    }
                }
            }
        }

        /// <summary>
        /// This function defines a mapping between the column index in the file vs. the column index to which the data should be written in the database
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="columnIndexMap">Maps observed column index to desired column index in DB (-1 means do not store the given column in the DB)</param>
        /// <param name="knownFields"></param>
        /// <returns>Total number of columns in the input file</returns>
        private int MapColumnsToKnownFields(string filePath, out List<int> columnIndexMap, List<string> knownFields)
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
                    var index = findIndexOfColumnName(knownFields, column);

                    // If found [not -1]
                    if (index > -1)
                    {
                        // Console.writeline("store i={0} && index={1}", i, index);
                        // Store index of our part_id as index value as per function requirements [see details above func name]
                        columnIndexMap.Add(index);
                    }
                    else
                    {
                        // Not found ... add -1 to indicate we will skip this column
                        // Console.writeline("store i={0} && index={1}", i, index);
                        columnIndexMap.Add(-1);
                    }
                }

            }

            return columnCount;
        }

        /// <summary>
        /// Look for fieldName in knownFields
        /// </summary>
        /// <param name="knownFields"></param>
        /// <param name="fieldName"></param>
        /// <returns>Return the index if a match or -1 if no match</returns>
        private int findIndexOfColumnName(List<string> knownFields, string fieldName)
        {
            for (var i = 0; i < knownFields.Count; i++)
            {
                // If found name ... return index
                if (string.Equals(knownFields[i], fieldName, StringComparison.InvariantCultureIgnoreCase))
                {
                    return i;
                }
            }

            return -1;
        }

        // Ensure file has been deleted
        private void ensure_temp_file_removed(string filePath)
        {
            // Ensure temp file does not still exist
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }
        }

        /// <summary>
        /// Is this a valid file to reformat (checks mValidFilesToReFormat list)
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="dataset"></param>
        /// <returns></returns>
        private List<string> GetFieldsForKnownFile(string filename, string dataset)
        {
            // Get filename without extension
            var filenameNoExtension = Path.GetFileNameWithoutExtension(filename);

            if (string.IsNullOrWhiteSpace(filenameNoExtension))
                return new List<string>();

            // Remove the dataset name from the filename
            // Example result: ScanStats or ScanStatsEx
            var filenamePart = filenameNoExtension.Substring(dataset.Length + 1);


            if (mValidFilesToReFormat.TryGetValue(filenamePart, out var knownFields))
            {
                return knownFields;
            }

            return new List<string>();
        }

        // Field cleaner to ensure database consistency by removing spaces, parentheses, / and more
        private List<string> FieldCleaner(IEnumerable<string> field_array)
        {
            var updatedFields = new List<string>();

            // Loop through each field
            foreach (var field in field_array)
            {
                // Step #1 remove (...)
                var first_index = field.IndexOf(" (", StringComparison.Ordinal);
                var last_index = field.IndexOf(")", StringComparison.Ordinal);

                string updatedField;

                // If there is a (...)
                if (first_index > 0 && last_index > first_index)
                {
                    updatedField = field.Remove(first_index, last_index - first_index + 1);
                }
                else
                {
                    updatedField = string.Copy(field);
                }

                // Step #2 replace all " " with "_"
                updatedField = updatedField.Replace(" ", "_");

                // Step #3 replace all "/" with "" [required due to things like ScanStatsEx having m/z when it should be mz]
                updatedField = updatedField.Replace("/", "");

                updatedFields.Add(updatedField);
            }

            return updatedFields;
        }

      
    }
}
