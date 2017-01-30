using System;
using System.Collections.Generic;
using System.IO;

namespace SMAQC
{
    class DataFileFormatter
    {
        // Declare variables
        private readonly List<string> ValidFilesToReFormat = new List<string>();                     // List of files that are valid to reformat
        private readonly string[,] FieldList = new string[10, 30];                                   // List of fields

        private string mTempFilePath = "";

        // Constructor
        public DataFileFormatter()
        {
            // Set valid files to reformat
            ValidFilesToReFormat.Add("ScanStats");
            ValidFilesToReFormat.Add("ScanStatsEx");
            ValidFilesToReFormat.Add("SICstats");
            ValidFilesToReFormat.Add("xt");
            ValidFilesToReFormat.Add("xt_ResultToSeqMap");
            ValidFilesToReFormat.Add("xt_SeqToProteinMap");

            // Scanstats fields
            FieldList[0, 0] = "Dataset";
            FieldList[0, 1] = "ScanNumber";
            FieldList[0, 2] = "ScanTime";
            FieldList[0, 3] = "ScanType";
            FieldList[0, 4] = "TotalIonIntensity";
            FieldList[0, 5] = "BasePeakIntensity";
            FieldList[0, 6] = "BasePeakMZ";
            FieldList[0, 7] = "BasePeakSignalToNoiseRatio";
            FieldList[0, 8] = "IonCount";
            FieldList[0, 9] = "IonCountRaw";
            FieldList[0, 10] = "ScanTypeName";

            // Scanstatsex fields
            FieldList[1, 0] = "Dataset";
            FieldList[1, 1] = "ScanNumber";
            FieldList[1, 2] = "Ion Injection Time (ms)";
            FieldList[1, 3] = "Scan Segment";//
            FieldList[1, 4] = "Scan Event";
            FieldList[1, 5] = "Master Index";
            FieldList[1, 6] = "Elapsed Scan Time (sec)";
            FieldList[1, 7] = "Charge State";
            FieldList[1, 8] = "Monoisotopic M/Z";
            FieldList[1, 9] = "MS2 Isolation Width";
            FieldList[1, 10] = "FT Analyzer Settings";
            FieldList[1, 11] = "FT Analyzer Message";
            FieldList[1, 12] = "FT Resolution";
            FieldList[1, 13] = "Conversion Parameter B";
            FieldList[1, 14] = "Conversion Parameter C";
            FieldList[1, 15] = "Conversion Parameter D";
            FieldList[1, 16] = "Conversion Parameter E";
            FieldList[1, 17] = "Collision Mode";
            FieldList[1, 18] = "Scan Filter Text";
            FieldList[1, 19] = "Source Voltage (kV)";
            FieldList[1, 20] = "Source Current (uA)";

            // Sicstats fields
            FieldList[2, 0] = "Dataset";
            FieldList[2, 1] = "ParentIonIndex";
            FieldList[2, 2] = "MZ";
            FieldList[2, 3] = "SurveyScanNumber";
            FieldList[2, 4] = "FragScanNumber";
            FieldList[2, 5] = "OptimalPeakApexScanNumber";
            FieldList[2, 6] = "PeakApexOverrideParentIonIndex";
            FieldList[2, 7] = "CustomSICPeak";
            FieldList[2, 8] = "PeakScanStart";
            FieldList[2, 9] = "PeakScanEnd";
            FieldList[2, 10] = "PeakScanMaxIntensity";
            FieldList[2, 11] = "PeakMaxIntensity";
            FieldList[2, 12] = "PeakSignalToNoiseRatio";
            FieldList[2, 13] = "FWHMInScans";
            FieldList[2, 14] = "PeakArea";
            FieldList[2, 15] = "ParentIonIntensity";
            FieldList[2, 16] = "PeakBaselineNoiseLevel";
            FieldList[2, 17] = "PeakBaselineNoiseStDev";
            FieldList[2, 18] = "PeakBaselinePointsUsed";
            FieldList[2, 19] = "StatMomentsArea";
            FieldList[2, 20] = "CenterOfMassScan";
            FieldList[2, 21] = "PeakStDev";
            FieldList[2, 22] = "PeakSkew";
            FieldList[2, 23] = "PeakKSStat";
            FieldList[2, 24] = "StatMomentsDataCountUsed";

            // Xt fields
            FieldList[3, 0] = "Result_ID";
            FieldList[3, 1] = "Group_ID";
            FieldList[3, 2] = "Scan";
            FieldList[3, 3] = "Charge";
            FieldList[3, 4] = "Peptide_MH";
            FieldList[3, 5] = "Peptide_Hyperscore";
            FieldList[3, 6] = "Peptide_Expectation_Value_Log(e)";
            FieldList[3, 7] = "Multiple_Protein_Count";
            FieldList[3, 8] = "Peptide_Sequence";
            FieldList[3, 9] = "DeltaCn2";
            FieldList[3, 10] = "y_score";
            FieldList[3, 11] = "y_ions";
            FieldList[3, 12] = "b_score";
            FieldList[3, 13] = "b_ions";
            FieldList[3, 14] = "Delta_Mass";
            FieldList[3, 15] = "Peptide_Intensity_Log(I)";
            FieldList[3, 16] = "DelM_PPM";

            // Xt_resulttoseqmap fields
            FieldList[4, 0] = "Result_ID";
            FieldList[4, 1] = "Unique_Seq_ID";

            // Xt_seqtoproteinmap fields
            FieldList[5, 0] = "Unique_Seq_ID";
            FieldList[5, 1] = "Cleavage_State";
            FieldList[5, 2] = "Terminus_State";
            FieldList[5, 3] = "Protein_Name";
            FieldList[5, 4] = "Protein_Expectation_Value_Log(e)";
            FieldList[5, 5] = "Protein_Intensity_Log(I)";

            // Clean fields from 2 dim array
            FieldList = FieldCleaner2d(FieldList);
        }

        // Destructor
        ~DataFileFormatter()
        {
            // Ensure temp file does not still exist
            if (!string.IsNullOrEmpty(mTempFilePath) && File.Exists(mTempFilePath))
                ensure_temp_file_removed(mTempFilePath);
        }

        // Tempfilepath property
        public string TempFilePath
        {
            get
            {
                return mTempFilePath;
            }
        }


        // This function checks each file to see if it should be re-formated and then takes care of it
        // Returns false == no rebuild || true == rebuild
        public bool handleFile(string filename, string dataset)
        {
            // Declare variables
            var ListFieldID = new List<int>();                    // Maps observed column index to desired column index in db (-1 means do not store the given column in the db)

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
            // Declare variables
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
                            // This normally happens on scanstatsex. nothing we can do due to bad tool. ignore line.

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
            // Declare variables
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
            var bound = FieldList.GetLength(1);

            for (var i = 0; i < bound; i++)
            {
                // Check to ensure that we are not null
                if (FieldList[ValidFilesToReFormat_id, i] == null)
                    break;

                // If found name ... return index
                if (FieldList[ValidFilesToReFormat_id, i].Equals(name))
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
            filename = filename.Substring(dataset.Length + 1);                  // Returns scanstats, scanstatsex, ...

            // Loop through all valid files that we reformat
            for (var i = 0; i < ValidFilesToReFormat.Count; i++)
            {
                // If found a match
                if (filename.Equals(ValidFilesToReFormat[i], StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }

            return -1;
        }

        // Field cleaner to ensure database consistancy by removing spaces, (,), / and more from 2 dim arrays
        private string[,] FieldCleaner2d(string[,] field_array)
        {
            // Declare variables
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

                    // Step #3 replace all "/" with "" [required due to things like scanstatsex having m/z when it should be mz]
                    field_array[j, i] = field_array[j, i].Replace("/", "");
                }
            }

            return field_array;
        }

        // Field cleaner to ensure database consistancy by removing spaces, (,), / and more
        private string[] FieldCleaner(string[] field_array)
        {
            // Declare variables

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

                // Step #3 replace all "/" with "" [required due to things like scanstatsex having m/z when it should be mz]
                field_array[i] = field_array[i].Replace("/", "");
            }

            return field_array;
        }

        // This function returns whether or not we are currently working with _scanstatsex.txt
        public bool ScanStatsExBugFixer(string file_to_load)
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