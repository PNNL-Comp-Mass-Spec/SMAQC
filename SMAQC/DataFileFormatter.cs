using System;
using System.Collections.Generic;
using System.IO;

namespace SMAQC
{
    class DataFileFormatter
    {
        private readonly List<string> mValidFilesToReFormat = new List<string>();                     // List of files that are valid to reformat
        private readonly string[,] mFieldList = new string[10, 30];                                   // List of fields

        private string mTempFilePath = "";

        /// <summary>
        /// Constructor
        /// </summary>
        public DataFileFormatter()
        {
            // Set valid files to reformat
            mValidFilesToReFormat.Add("ScanStats");
            mValidFilesToReFormat.Add("ScanStatsEx");
            mValidFilesToReFormat.Add("SICstats");
            mValidFilesToReFormat.Add("xt");
            mValidFilesToReFormat.Add("xt_ResultToSeqMap");
            mValidFilesToReFormat.Add("xt_SeqToProteinMap");

            // ScanStats fields
            mFieldList[0, 0] = "Dataset";
            mFieldList[0, 1] = "ScanNumber";
            mFieldList[0, 2] = "ScanTime";
            mFieldList[0, 3] = "ScanType";
            mFieldList[0, 4] = "TotalIonIntensity";
            mFieldList[0, 5] = "BasePeakIntensity";
            mFieldList[0, 6] = "BasePeakMZ";
            mFieldList[0, 7] = "BasePeakSignalToNoiseRatio";
            mFieldList[0, 8] = "IonCount";
            mFieldList[0, 9] = "IonCountRaw";
            mFieldList[0, 10] = "ScanTypeName";

            // ScanStatsEx fields
            mFieldList[1, 0] = "Dataset";
            mFieldList[1, 1] = "ScanNumber";
            mFieldList[1, 2] = "Ion Injection Time (ms)";
            mFieldList[1, 3] = "Scan Segment";//
            mFieldList[1, 4] = "Scan Event";
            mFieldList[1, 5] = "Master Index";
            mFieldList[1, 6] = "Elapsed Scan Time (sec)";
            mFieldList[1, 7] = "Charge State";
            mFieldList[1, 8] = "Monoisotopic M/Z";
            mFieldList[1, 9] = "MS2 Isolation Width";
            mFieldList[1, 10] = "FT Analyzer Settings";
            mFieldList[1, 11] = "FT Analyzer Message";
            mFieldList[1, 12] = "FT Resolution";
            mFieldList[1, 13] = "Conversion Parameter B";
            mFieldList[1, 14] = "Conversion Parameter C";
            mFieldList[1, 15] = "Conversion Parameter D";
            mFieldList[1, 16] = "Conversion Parameter E";
            mFieldList[1, 17] = "Collision Mode";
            mFieldList[1, 18] = "Scan Filter Text";
            mFieldList[1, 19] = "Source Voltage (kV)";
            mFieldList[1, 20] = "Source Current (uA)";

            // Sicstats fields
            mFieldList[2, 0] = "Dataset";
            mFieldList[2, 1] = "ParentIonIndex";
            mFieldList[2, 2] = "MZ";
            mFieldList[2, 3] = "SurveyScanNumber";
            mFieldList[2, 4] = "FragScanNumber";
            mFieldList[2, 5] = "OptimalPeakApexScanNumber";
            mFieldList[2, 6] = "PeakApexOverrideParentIonIndex";
            mFieldList[2, 7] = "CustomSICPeak";
            mFieldList[2, 8] = "PeakScanStart";
            mFieldList[2, 9] = "PeakScanEnd";
            mFieldList[2, 10] = "PeakScanMaxIntensity";
            mFieldList[2, 11] = "PeakMaxIntensity";
            mFieldList[2, 12] = "PeakSignalToNoiseRatio";
            mFieldList[2, 13] = "FWHMInScans";
            mFieldList[2, 14] = "PeakArea";
            mFieldList[2, 15] = "ParentIonIntensity";
            mFieldList[2, 16] = "PeakBaselineNoiseLevel";
            mFieldList[2, 17] = "PeakBaselineNoiseStDev";
            mFieldList[2, 18] = "PeakBaselinePointsUsed";
            mFieldList[2, 19] = "StatMomentsArea";
            mFieldList[2, 20] = "CenterOfMassScan";
            mFieldList[2, 21] = "PeakStDev";
            mFieldList[2, 22] = "PeakSkew";
            mFieldList[2, 23] = "PeakKSStat";
            mFieldList[2, 24] = "StatMomentsDataCountUsed";

            // Xt fields
            mFieldList[3, 0] = "Result_ID";
            mFieldList[3, 1] = "Group_ID";
            mFieldList[3, 2] = "Scan";
            mFieldList[3, 3] = "Charge";
            mFieldList[3, 4] = "Peptide_MH";
            mFieldList[3, 5] = "Peptide_Hyperscore";
            mFieldList[3, 6] = "Peptide_Expectation_Value_Log(e)";
            mFieldList[3, 7] = "Multiple_Protein_Count";
            mFieldList[3, 8] = "Peptide_Sequence";
            mFieldList[3, 9] = "DeltaCn2";
            mFieldList[3, 10] = "y_score";
            mFieldList[3, 11] = "y_ions";
            mFieldList[3, 12] = "b_score";
            mFieldList[3, 13] = "b_ions";
            mFieldList[3, 14] = "Delta_Mass";
            mFieldList[3, 15] = "Peptide_Intensity_Log(I)";
            mFieldList[3, 16] = "DelM_PPM";

            // Xt_resulttoseqmap fields
            mFieldList[4, 0] = "Result_ID";
            mFieldList[4, 1] = "Unique_Seq_ID";

            // Xt_seqtoproteinmap fields
            mFieldList[5, 0] = "Unique_Seq_ID";
            mFieldList[5, 1] = "Cleavage_State";
            mFieldList[5, 2] = "Terminus_State";
            mFieldList[5, 3] = "Protein_Name";
            mFieldList[5, 4] = "Protein_Expectation_Value_Log(e)";
            mFieldList[5, 5] = "Protein_Intensity_Log(I)";

            // Clean fields from 2 dim array
            mFieldList = FieldCleaner2d(mFieldList);
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

        // This function checks each file to see if it should be re-formated and then takes care of it
        // Returns false == no rebuild || true == rebuild
        public bool HandleFile(string filename, string dataset)
        {
            // Maps observed column index to desired column index in db (-1 means do not store the given column in the db)
            var ListFieldID = new List<int>();                    

            // Check if is valid file
            var ValidFilesToReFormat_id = is_valid_file_to_reformat(filename, dataset);

            // Is this a file that needs re-formating?
            if (ValidFilesToReFormat_id >= 0)
            {
                // Pad hash table with pointer to correct values
                var numOfColumns = padHashTable(filename, ref ListFieldID, ValidFilesToReFormat_id);

                // Obtain a temp file path
                mTempFilePath = Path.GetTempFileName();

                // Call internal rebuild function
                rebuildFile(filename, mTempFilePath, numOfColumns, ListFieldID);

                return true;
            }

            // Clear hash table
            ListFieldID.Clear();

            return false;
        }

        // Rebuild filename using padding to ensure all fields match up
        private void rebuildFile(string filename, string save_to_filename, int numOfColumns, List<int> ListFieldID)
        {
            var line_num = 0;

            // Open files used for r/w
            using (var file_read = new StreamReader(filename))
            {
                using (var file_write = new StreamWriter(save_to_filename))
                {
                    // Loop through each line
                    string line;
                    while ((line = file_read.ReadLine()) != null)
                    {
                        // Declare new line
                        var line_temp = "";

                        // Split given data files by tab
                        var delimiters = new[] { '\t' };

                        // Do split operation
                        var parts = line.Split(delimiters, StringSplitOptions.None);

                        // If column and data mismatch
                        if (parts.Length != numOfColumns)
                        {
                            // Number of columns is not the expected number
                            // This normally happens on ScanStatsEx. nothing we can do due to bad tool. ignore line.

                            // Next line
                            continue;
                        }

                        // Reading the first line ... skip it as no longer needed
                        if (line_num == 0)
                        {
                            // Clean fields to ensure consistency
                            parts = FieldCleaner(parts);

                            // Inc
                            line_num++;

                            // Next line
                            // Continue;
                        }

                        // Now reading data lines [assuming line_num > 0]

                        // Loop through each part
                        for (var i = 0; i < parts.Length; i++)
                        {
                            // If not an allowed field ignore
                            if (ListFieldID[i] > -1)
                            {
                                if (line_temp.Length > 0)
                                    line_temp += "\t";

                                // Append to line_temp
                                line_temp += parts[i];

                            }

                        }

                        // Write line
                        file_write.WriteLine(line_temp);

                    }
                }
            }
        }

        // This function defines a mapping between the column index in the file vs. the column index to which the data should be written in the database
        /*
        1. IDENTIFIES USING ValidFilesToReFormat_id THE CORRECT [x][] ARRAY
        2. STORES VALUES IN HashFieldID IN FOLLOWING FORMAT, IF ValidFilesToReFormat_id=1
            ASSUME FILE HAS ONLY: 'FT Analyzer Settings', 'Conversion Parameter C', 'Dataset' IN FILE
            1. Find offset for each:: 'FT Analyzer Settings'=10, 'Conversion Parameter C'=14, 'Dataset'=0
            2. store in HashFieldID as [0]=10, [1]=14, [2]=0, ...all others == -1
        */
        private int padHashTable(string file_to_load, ref List<int> ListFieldID, int ValidFilesToReFormat_id)
        {
            int numOfcolumns;

            // Open + read
            using (var file = new StreamReader(file_to_load))
            {
                var line = file.ReadLine();

                if (string.IsNullOrWhiteSpace(line))
                    return 0;

                // Split given data files by tab
                var delimiters = new[] {'\t'};

                // Do split operation
                var parts = line.Split(delimiters, StringSplitOptions.None);

                // Clean fields to ensure consistency
                parts = FieldCleaner(parts);

                // Set numofcolumns
                numOfcolumns = parts.Length;

                // Loop through each column
                foreach (var column in parts)
                {
                    // Search for column name that is found in our file line
                    var index = findIndexOfColumnName(ValidFilesToReFormat_id, column);

                    // If found [not -1]
                    if (index != -1)
                    {
                        // Console.writeline("store i={0} && index={1}", i, index);
                        // Store index of our part_id as index value as per function requirements [see details above func name]
                        ListFieldID.Add(index);
                    }
                    else
                    {
                        // Not found ... add -1 to indicate we will skip this column
                        // Console.writeline("store i={0} && index={1}", i, index);
                        ListFieldID.Add(-1);
                    }
                }

            }

            return numOfcolumns;
        }

        // Searches [id][x] for name ... if found returns index ... else -1
        private int findIndexOfColumnName(int ValidFilesToReFormat_id, string name)
        {
            // Get bound of second dim
            var bound = mFieldList.GetLength(1);

            for (var i = 0; i < bound; i++)
            {
                // Check to ensure that we are not null
                if (mFieldList[ValidFilesToReFormat_id, i] == null)
                    break;

                // If found name ... return index
                if (mFieldList[ValidFilesToReFormat_id, i].Equals(name))
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

        // Is this a valid file to reformat [checks validfilestoreformat list]
        private int is_valid_file_to_reformat(string filename, string dataset)
        {
            // Step # 1 get filename without extension
            filename = Path.GetFileNameWithoutExtension(filename);

            if (string.IsNullOrWhiteSpace(filename))
                return -1;

            // Step # 3 now do actual removing of data prefix
            filename = filename.Substring(dataset.Length + 1);                  // Returns ScanStats, ScanStatsEx, ...

            // Loop through all valid files that we reformat
            for (var i = 0; i < mValidFilesToReFormat.Count; i++)
            {
                // If found a match
                if (filename.Equals(mValidFilesToReFormat[i], StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }

            return -1;
        }

        // Field cleaner to ensure database consistency by removing spaces, (,), / and more from 2 dim arrays
        private string[,] FieldCleaner2d(string[,] field_array)
        {
            var dim1 = field_array.GetLength(0); // [X][]
            var dim2 = field_array.GetLength(1); // [][X]

            // Loop through first dimension
            for (var j = 0; j < dim1; j++)
            {
                // Loop through second dimension [stores all fields]
                for (var i = 0; i < dim2; i++)
                {
                    // If null skip
                    if (field_array[j, i] == null)
                        continue;

                    // Step #1 remove (...)
                    var first_index = field_array[j, i].IndexOf(" (", StringComparison.Ordinal);
                    var last_index = field_array[j, i].IndexOf(")", StringComparison.Ordinal);

                    // If there is a (...)
                    if (first_index > 0 && last_index > 0)
                    {
                        field_array[j, i] = field_array[j, i].Remove(first_index, last_index - first_index + 1);
                    }

                    // Step #2 replace all " " with "_"
                    field_array[j, i] = field_array[j, i].Replace(" ", "_");

                    // Step #3 replace all "/" with "" [required due to things like ScanStatsEx having m/z when it should be mz]
                    field_array[j, i] = field_array[j, i].Replace("/", "");
                }
            }

            return field_array;
        }

        // Field cleaner to ensure database consistency by removing spaces, (,), / and more
        private string[] FieldCleaner(string[] field_array)
        {

            // Loop through each field
            for (var i = 0; i < field_array.Length; i++)
            {
                // Step #1 remove (...)
                var first_index = field_array[i].IndexOf(" (", StringComparison.Ordinal);
                var last_index = field_array[i].IndexOf(")", StringComparison.Ordinal);

                // If there is a (...)
                if (first_index > 0 && last_index > first_index)
                {
                    field_array[i] = field_array[i].Remove(first_index, last_index - first_index + 1);
                }

                // Step #2 replace all " " with "_"
                field_array[i] = field_array[i].Replace(" ", "_");

                // Step #3 replace all "/" with "" [required due to things like ScanStatsEx having m/z when it should be mz]
                field_array[i] = field_array[i].Replace("/", "");
            }

            return field_array;
        }

        // This function returns whether or not we are currently working with _ScanStatsEx.txt
        private bool ScanStatsExBugFixer(string file_to_load)
        {
            var value = file_to_load.IndexOf("_ScanStatsEx.txt", StringComparison.OrdinalIgnoreCase);

            // If found return true
            if (value >= 0)
            {
                return true;
            }

            // Else return false
            return false;
        }
    }
}