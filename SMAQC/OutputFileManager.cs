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
            mMetricNames = metricNames;
        }

        // Save data handler
        public void SaveData(string dataset, string filePath, int scan_id)
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

                if (mFirstUse)
                {
                    // Create the file and append the first set of metrics
                    CreateOutputFileForFirstTimeUse(dataset, targetFilePath, scan_id);

                    // Set first_use to false
                    mFirstUse = false;
                }
                else
                {
                    // Append to the file
                    AppendAdditionalMeasurementsToOutputFile(dataset, targetFilePath, scan_id);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error saving the results: " + ex.Message);
                throw;
            }
        }

        // Create the file + add metrics for first time use
        private void CreateOutputFileForFirstTimeUse(string dataset, string filename, int scan_id)
        {
            // Scan results
            var dctResults = new Dictionary<string, string>();
            var dctValidResults = new SortedDictionary<string, string>();

            // Set query to retrieve scan results
            mDBWrapper.SetQuery("SELECT * FROM scan_results WHERE scan_id ='" + scan_id + "' LIMIT 1;");

            // Init reader
            mDBWrapper.initReader();

            // Read it into our hash table
            mDBWrapper.ReadSingleLine(mMetricNames.ToArray(), ref dctResults);

            // Get count
            var count = dctResults.Count;

            // Ensure there is data!
            if (count <= 0)
            {
                Console.WriteLine("Error: The scan id provided either does not exist, or has no results!");
                return;
            }

            // Create the result file
            using (var file = new StreamWriter(filename))
            {
                file.WriteLine("SMAQC SCANNER RESULTS");
                file.WriteLine("-----------------------------------------------------------");
                file.WriteLine("SMAQC Version: " + mSMAQCVersion + "");
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
            }

        }

        // Append additional measurement data to output file
        private void AppendAdditionalMeasurementsToOutputFile(string dataset, string filename, int scan_id)
        {
            // Hash table for scan results
            var dctResults = new Dictionary<string, string>();
            var dctValidResults = new SortedDictionary<string, string>();

            // Set query to retrieve scan results
            mDBWrapper.SetQuery("SELECT * FROM scan_results WHERE scan_id ='" + scan_id + "' LIMIT 1;");

            // Init reader
            mDBWrapper.initReader();

            // Read it into our hash table
            mDBWrapper.ReadSingleLine(mMetricNames.ToArray(), ref dctResults);

            // Get count
            var count = dctResults.Count;

            // Ensure there is data!
            if (count <= 0)
            {

                Console.WriteLine("Error: The scan id provided either does not exist, or has no results!");
                return;
            }

            // Append to the result file
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


    }
}
