using System;
using System.Collections.Generic;
using System.IO;

namespace SMAQC
{
    class OutputFileManager
    {
        // Declare variables
        private readonly DBWrapper DBWrapper;                                                                // Ref db interface object
        private bool first_use;																			     // Is the first use?
        private readonly string smaqc_version;                                                               // Smaqc version
        private readonly List<string> fields;                                                                // Smaqc fields

        // Constructor
        public OutputFileManager(ref DBWrapper DBWrapper, string ProgVersion, List<string> ProgFields)
        {
            this.DBWrapper = DBWrapper;
            first_use = true;
            smaqc_version = ProgVersion;
            fields = ProgFields;
        }

        // Save data handler
        public void SaveData(string dataset, string filePath, int scan_id, int dataset_number)
        {
            try
            {

                string targetFilePath;

                if (filePath.EndsWith(@"\") || Directory.Exists(filePath))
                {
                    // User provided a folder path
                    var diTargetFolder = new DirectoryInfo(filePath);
                    if (!diTargetFolder.Exists)
                    {
                        Console.WriteLine("Creating folder " + diTargetFolder.FullName);
                        diTargetFolder.Create();
                    }
                    targetFilePath = Path.Combine(diTargetFolder.FullName, "SMAQC_results.txt");
                }
                else
                {
                    targetFilePath = filePath;
                }

                if (first_use)
                {
                    // Create the file and append the first set of metrics
                    CreateOutputFileForFirstTimeUse(dataset, targetFilePath, scan_id, dataset_number);

                    // Set first_use to false
                    first_use = false;
                }
                else
                {
                    // Append to the file
                    AppendAdditionalMeasurementsToOutputFile(dataset, targetFilePath, scan_id, dataset_number);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error saving the results: " + ex.Message);
                throw;
            }
        }

        // Create the file + add metrics for first time use
        private void CreateOutputFileForFirstTimeUse(string dataset, string filename, int scan_id, int dataset_number)
        {
            // Declare variables
            var dctResults = new Dictionary<string, string>();                                             // Scan results
            var dctValidResults = new SortedDictionary<string, string>();

            // Calculate relative result_id
            var result_id = scan_id + dataset_number;

            // Set query to retrieve scan results
            DBWrapper.setQuery("SELECT * FROM scan_results WHERE result_id='" + result_id + "' LIMIT 1;");

            // Init reader
            DBWrapper.initReader();

            // Read it into our hash table
            DBWrapper.readSingleLine(fields.ToArray(), ref dctResults);

            // Get count
            var count = dctResults.Count;

            // Ensure there is data!
            if (count > 0)
            {
                // Open temp file
                using (var file = new StreamWriter(filename))
                {

                    file.WriteLine("SMAQC SCANNER RESULTS");
                    file.WriteLine("-----------------------------------------------------------");
                    file.WriteLine("SMAQC Version: " + smaqc_version + "");
                    // file.WriteLine("results from scan id: " + scan_id + "");
                    file.WriteLine("Instrument ID: " + dctResults["instrument_id"] + "");
                    file.WriteLine("Scan Date: " + dctResults["scan_date"] + "");
                    file.WriteLine("[Data]");
                    file.WriteLine("Dataset, Measurement Name, Measurement Value");

                    // Remove from hash table
                    dctResults.Remove("instrument_id");
                    dctResults.Remove("scan_date");
                    dctResults.Remove("scan_id");
                    dctResults.Remove("random_id");

                    // Loop through all that should be left [our measurements]
                    foreach (var key in dctResults.Keys)
                    {
                        // Ensure that all keys have data [this is really a fix for sqlite due to not supporting nulls properly]
                        if (!string.IsNullOrEmpty(dctResults[key]))
                        {
                            // Add to sorted dictionary
                            dctValidResults.Add(key, dctResults[key]);
                        }
                    }

                    // Loop through each sorted dictionary
                    foreach (var pair in dctValidResults)
                    {
                        // Add: dataset, measurement name,

                        var outLine = string.Format("" + dataset + ", " + pair.Key + ",");

                        // If there is a non-null value
                        if (!pair.Value.Equals("Null"))
                        {
                            outLine += " " + pair.Value;
                        }

                        file.WriteLine(outLine);
                    }

                    file.WriteLine();
                    // Close file
                }
            }
            else
            {
                Console.WriteLine("Error: The scan id provided either does not exist, or has no results!");
            }

        }

        // Append additional measurement data to output file
        private void AppendAdditionalMeasurementsToOutputFile(string dataset, string filename, int scan_id, int dataset_number)
        {
            // Declare variables
            var dctResults = new Dictionary<string, string>();                                             // Hash table for scan results
            var dctValidResults = new SortedDictionary<string, string>();

            // Calculate relative result_id
            var result_id = scan_id + dataset_number;

            // Set query to retrieve scan results
            DBWrapper.setQuery("SELECT * FROM scan_results WHERE result_id='" + result_id + "' LIMIT 1;");

            // Init reader
            DBWrapper.initReader();

            // Read it into our hash table
            DBWrapper.readSingleLine(fields.ToArray(), ref dctResults);

            // Get count
            var count = dctResults.Count;

            // Ensure there is data!
            if (count > 0)
            {
                // Open temp file
                var file = File.AppendText(filename);

                // Remove from hash table
                dctResults.Remove("instrument_id");
                dctResults.Remove("scan_date");
                dctResults.Remove("scan_id");
                dctResults.Remove("random_id");

                // Loop through all that should be left [our measurements]
                foreach (var key in dctResults.Keys)
                {
                    // Ensure that all keys have data [this is really a fix for sqlite due to not supporting nulls properly]
                    if (!string.IsNullOrEmpty(dctResults[key]))
                    {
                        // Add to sorted dictionary
                        dctValidResults.Add(key, dctResults[key]);
                    }
                }

                // Loop through each sorted dictionary
                foreach (var pair in dctValidResults)
                {
                    // Add: dataset, measurement name,
                    var outLine = string.Format("" + dataset + ", " + pair.Key + ",");

                    // If there is a non-null value
                    if (!pair.Value.Equals("Null"))
                    {
                        outLine += " " + pair.Value;
                    }

                    file.WriteLine(outLine);
                }

                file.WriteLine();

                file.Close();
            }
            else
            {
                Console.WriteLine("Error: The scan id provided either does not exist, or has no results!");
            }

        }


    }
}
