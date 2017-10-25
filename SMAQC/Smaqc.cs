﻿using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Xml;
using PRISM;

namespace SMAQC
{
    internal static class Smaqc
    {
        private struct udtOptions
        {
            public string InputFolderPath;
            public string Instrument_id;
            public string MeasurementsFile;
            public string OutputFilePath;
            public string DBFolderPath;

            public void Clear()
            {
                InputFolderPath = string.Empty;
                Instrument_id = "1";
                MeasurementsFile = string.Empty;
                OutputFilePath = string.Empty;
                DBFolderPath = string.Empty;
            }
        }

        private const bool WIPE_TEMP_DATA_AT_START = true;
        private const bool KEEP_TEMP_DATA_AT_END = false;

        // DB Interface object
        private static DBWrapper mDBWrapper;

        /// <summary>
        /// Aggregate object
        /// </summary>
        private static Aggregate mAggregate;

        /// <summary>
        /// Measurement engine that computes the metrics
        /// </summary>
        private static Measurement mMeasurement;

        // Filter engine
        private static Filter mFilter;

        // Measurement engine
        private static MeasurementEngine mMeasurementEngine;

        private static readonly SystemLogManager mSystemLogManager = new SystemLogManager();

        /// <summary>
        /// Output engine
        /// </summary>
        private static OutputFileManager mOutputFileManager;

        /// <summary>
        /// Options
        /// </summary>
        private static udtOptions mOptions;

        private static readonly StringBuilder mQueryBuilder = new StringBuilder();

        // Measurement results
        private static Dictionary<string, string> mResults = new Dictionary<string, string>();

        private const string SMAQC_BUILD_DATE = "October 24, 2017";

        // Define the filename suffixes
        private static readonly string[] mMasicFileExtensions = { "_ScanStats", "_ScanStatsEx", "_SICStats", "_ReporterIons" };

        // Metrics to track in the database
        // Note that LoadMeasurementInfoFile uses this list to define the default metrics to run (skipping "instrument_id", "random_id", and "scan_date")
        private static readonly List<string> mMetricNames = new List<string> {
            "instrument_id", "random_id", "scan_date", "C_1A", "C_1B",
            "C_2A", "C_2B", "C_3A", "C_3B", "C_4A", "C_4B", "C_4C", "DS_1A", "DS_1B", "DS_2A", "DS_2B",
            "DS_3A", "DS_3B", "IS_1A", "IS_1B", "IS_2", "IS_3A", "IS_3B", "IS_3C", "MS1_1",
            "MS1_2A", "MS1_2B", "MS1_3A", "MS1_3B", "MS1_5A", "MS1_5B", "MS1_5C",
            "MS1_5D", "MS2_1", "MS2_2", "MS2_3", "MS2_4A", "MS2_4B", "MS2_4C", "MS2_4D", "P_1A",
            "P_1B", "P_2A", "P_2B", "P_2C", "P_3", "Phos_2A", "Phos_2C",
            "Keratin_2A", "Keratin_2C", "P_4A", "P_4B", "Trypsin_2A", "Trypsin_2C",
            "MS2_RepIon_All", "MS2_RepIon_1Missing", "MS2_RepIon_2Missing", "MS2_RepIon_3Missing"};

        static int Main(string[] args)
        {

            var random = new Random();
            var randomId = random.Next();

            var objParseCommandLine = new clsParseCommandLine();
            var success = false;

            mOptions.Clear();

            if (objParseCommandLine.ParseCommandLine())
            {
                if (SetOptionsUsingCommandLineParameters(objParseCommandLine))
                    success = true;
            }

            if (!success ||
                objParseCommandLine.NeedToShowHelp ||
                objParseCommandLine.ParameterCount + objParseCommandLine.NonSwitchParameterCount == 0 ||
                string.IsNullOrEmpty(mOptions.InputFolderPath))
            {
                ShowProgramHelp();
                return -1;

            }

            // Show the processing options
            Console.WriteLine();
            Console.WriteLine("Instrument ID: ".PadRight(20) + mOptions.Instrument_id);
            Console.WriteLine("Path to datasets: ".PadRight(20) + mOptions.InputFolderPath);
            if (string.IsNullOrEmpty(mOptions.MeasurementsFile))
                Console.WriteLine("Using default metrics");
            else
                Console.WriteLine("Measurements file: ".PadRight(20) + mOptions.MeasurementsFile);
            if (!string.IsNullOrEmpty(mOptions.OutputFilePath))
                Console.WriteLine("Text results file: ".PadRight(20) + mOptions.OutputFilePath);
            Console.WriteLine("SQLite DB folder: ".PadRight(20) + mOptions.DBFolderPath);
            Console.WriteLine();

            try
            {
                // Create the application log
                mSystemLogManager.CreateApplicationLog();

                mSystemLogManager.AddApplicationLog("SMAQC Version " + GetAppVersion());

            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error initializing log file: " + ex.Message);
                Thread.Sleep(1500);
                return 6;
            }


            try
            {

                // Obtain the list of measurements to run
                var lstMeasurementsToRun = LoadMeasurementInfoFile(mOptions.MeasurementsFile);

                try
                {

                    // Connect to the database
                    mDBWrapper = new DBWrapper(mOptions.DBFolderPath, false);

                    if (WIPE_TEMP_DATA_AT_START)
                        mDBWrapper.ClearTempTables();

                    mAggregate = new Aggregate(mOptions.InputFolderPath);
                    mMeasurement = new Measurement(randomId, mDBWrapper);
                    mMeasurementEngine = new MeasurementEngine(lstMeasurementsToRun, mMeasurement, mSystemLogManager);
                    mFilter = new Filter(mDBWrapper, mOptions.Instrument_id, randomId, mSystemLogManager);
                    mOutputFileManager = new OutputFileManager(mDBWrapper, GetAppVersion(), mMetricNames);

                    try
                    {
                        var errorCode = ProcessDatasets(randomId, lstMeasurementsToRun);
                        if (errorCode != 0)
                        {
                            Thread.Sleep(1500);
                            return errorCode;
                        }
                    }
                    catch (Exception ex)
                    {
                        var message = "Error processing datasets: " + ex.Message;
                        ShowErrorMessage(message);
                        mSystemLogManager.AddApplicationLog(message);
                        Thread.Sleep(1500);
                        return 9;
                    }

                    try
                    {
                        mSystemLogManager.AddApplicationLog("SMAQC analysis complete");

                        mSystemLogManager.CloseLogFile();
                    }
                    catch (Exception ex)
                    {
                        ShowErrorMessage("Error closing the log file: " + ex.Message);
                        Thread.Sleep(1500);
                        return 10;
                    }

                }
                catch (Exception ex)
                {
                    ShowErrorMessage("Error initializing database wrapper: " + ex.Message);
                    Thread.Sleep(1500);
                    return 7;
                }

            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error initializing program: " + ex.Message);
                Thread.Sleep(1500);
                return 8;
            }

            return 0;
        }

        /// <summary>
        /// Process the datasets in the specified input folder
        /// </summary>
        /// <param name="randomId"></param>
        /// <param name="lstMeasurementsToRun"></param>
        /// <returns>0 if success, otherwise an error code</returns>
        private static int ProcessDatasets(int randomId, IReadOnlyCollection<string> lstMeasurementsToRun)
        {

            mSystemLogManager.AddApplicationLog("Searching for Text Files...");

            // Detect datasets
            var DatasetNames = mAggregate.DetectDatasets();

            if (DatasetNames.Count == 0)
            {
                // No datasets were found
                mSystemLogManager.AddApplicationLogError("Unable to find any datasets in " + mOptions.InputFolderPath);
                mSystemLogManager.AddApplicationLog("Exiting...");
                return -2;
            }

            // Process the datasets
            foreach (var datasetName in DatasetNames)
            {
                // Get the next available scan id
                var scanId = DetermineResultId();

                try
                {
                    mAggregate.SetDatasetName(datasetName);

                    // getMasicFileImportList returns a dictionary where
                    // Keys are file paths and Values are lists of header column suffixes to ignore
                    var masicFileList = mAggregate.GetMasicFileImportList("*.txt");

                    // Ensure that the MASIC files exist
                    var scanStatsMissing = false;
                    var sicStatsMissing = false;
                    var scanStatsExMissing = false;
                    var reporterIonsMissing = false;

                    // Find the missing files
                    foreach (var sSuffix in mMasicFileExtensions)
                    {
                        var bMatchFound = false;

                        foreach (var candidateFile in masicFileList)
                        {
                            var sFileName = Path.GetFileNameWithoutExtension(candidateFile.Key);
                            if (sFileName != null && sFileName.EndsWith(sSuffix, true, System.Globalization.CultureInfo.CurrentCulture))
                            {
                                bMatchFound = true;
                                break;
                            }
                        }

                        if (bMatchFound)
                            continue;

                        if (string.Equals(sSuffix, "_ScanStats", StringComparison.InvariantCultureIgnoreCase))
                        {
                            scanStatsMissing = true;
                        }
                        else
                        if (string.Equals(sSuffix, "_ScanStatsEx", StringComparison.InvariantCultureIgnoreCase))
                        {
                            scanStatsExMissing = true;
                        }
                        else
                        if (string.Equals(sSuffix, "_SICStats", StringComparison.InvariantCultureIgnoreCase))
                        {
                            sicStatsMissing = true;
                        }
                        else
                        if (string.Equals(sSuffix, "_ReporterIons", StringComparison.InvariantCultureIgnoreCase))
                        {
                            reporterIonsMissing = true;
                        }
                        else
                        {
                            mSystemLogManager.AddApplicationLog("  Missing unrecognized file: " + datasetName + sSuffix + ".txt");
                        }
                    }

                    if (scanStatsMissing || sicStatsMissing)
                    {
                        // Missing required files
                        mSystemLogManager.AddApplicationLog("Required MASIC data files not found in " +
                                                             mOptions.InputFolderPath);
                    }

                    if (scanStatsMissing)
                    {
                        mSystemLogManager.AddApplicationLog("  Missing file: " + datasetName + "_ScanStats.txt");
                        mSystemLogManager.AddApplicationLog("Exiting...");
                        mSystemLogManager.CloseLogFile();
                        return 11;
                    }

                    if (sicStatsMissing)
                    {
                        mSystemLogManager.AddApplicationLog("  Missing file: " + datasetName + "_SicStats.txt");
                        mSystemLogManager.AddApplicationLog("Exiting...");
                        mSystemLogManager.CloseLogFile();
                        return 12;
                    }

                    if (scanStatsExMissing)
                    {
                        // _ScanStatsEx.txt is missing; that's OK
                        mSystemLogManager.AddApplicationLog("Did not find file " + datasetName +
                                                                "_ScanStatsEx.txt; metrics MS1_1 and MS2_1 will not be computed");
                    }

                    if (reporterIonsMissing)
                    {
                        // _ReporterIons.txt is missing; that's OK
                        mSystemLogManager.AddApplicationLog("Did not find file " + datasetName +
                                                                "_ReporterIons.txt; MS2_RepIon metrics will not be computed");
                    }

                    // Load the data and store in the database
                    mSystemLogManager.AddApplicationLog("Parsing and Inserting Data into DB Temp Tables");

                    // Load data using PHRP
                    mFilter.LoadFilesUsingPHRP(mOptions.InputFolderPath, datasetName);

                    // Load the MASIC data
                    mFilter.LoadFilesAndInsertIntoDB(masicFileList, mMasicFileExtensions, datasetName);

                    // Run the measurements
                    mSystemLogManager.AddApplicationLog("Now running Measurements on " + datasetName);

                    mResults = mMeasurementEngine.RunMeasurements();

                    // Store the results
                    mSystemLogManager.AddApplicationLog("Saving Scan Results");
                    AddScanResults(mOptions.Instrument_id, randomId, scanId, lstMeasurementsToRun);

                    if (!KEEP_TEMP_DATA_AT_END)
                    {
                        // Remove the working data
                        mDBWrapper.ClearTempTables(randomId);
                    }

                    if (!string.IsNullOrEmpty(mOptions.OutputFilePath))
                    {
                        // Write the results to a file
                        mOutputFileManager.SaveData(datasetName, mOptions.OutputFilePath, scanId);

                        mSystemLogManager.AddApplicationLog("Scan output has been saved to " + mOptions.OutputFilePath);
                    }
                    else
                    {
                        mSystemLogManager.AddApplicationLog("Scan result saved to SQLite DB (Scan ID=" + scanId + ")");
                    }

                    mResults.Clear();

                }
                catch (Exception ex)
                {
                    mSystemLogManager.AddApplicationLog("Error processing dataset " + datasetName + ": " + ex.Message);
                    Console.WriteLine(ex.StackTrace);
                    return 5;
                }

            }

            return 0;
        }

        private static string GetAppPath()
        {
            return System.Reflection.Assembly.GetExecutingAssembly().Location;
        }

        private static string GetAppVersion()
        {
            return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " (" + SMAQC_BUILD_DATE + ")";
        }

        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine objParseCommandLine)
        {
            // Returns True if no problems; otherwise, returns false
            var lstValidParameters = new List<string> { "O", "DB", "I", "M" };

            try
            {
                // Make sure no invalid parameters are present
                if (objParseCommandLine.InvalidParametersPresent(lstValidParameters))
                {
                    var badArguments = new List<string>();
                    foreach (var item in objParseCommandLine.InvalidParameters(lstValidParameters))
                    {
                        badArguments.Add("/" + item);
                    }

                    ShowErrorMessage("Invalid commmand line parameters", badArguments);

                    return false;
                }

                // Query objParseCommandLine to see if various parameters are present
                if (objParseCommandLine.NonSwitchParameterCount > 0)
                {
                    mOptions.InputFolderPath = objParseCommandLine.RetrieveNonSwitchParameter(0);
                }

                if (objParseCommandLine.RetrieveValueForParameter("O", out var strValue))
                {
                    if (string.IsNullOrWhiteSpace(strValue))
                        ShowErrorMessage("/O does not have a value; not overriding the output file path");
                    else
                        mOptions.OutputFilePath = strValue;
                }

                if (objParseCommandLine.RetrieveValueForParameter("DB", out strValue))
                {
                    if (string.IsNullOrWhiteSpace(strValue))
                        ShowErrorMessage("/DB does not have a value; not overriding the database folder path");
                    else
                    {
                        mOptions.DBFolderPath = strValue;
                    }
                }

                if (objParseCommandLine.RetrieveValueForParameter("I", out strValue))
                {
                    if (string.IsNullOrWhiteSpace(strValue))
                        ShowErrorMessage("/I does not have a value; not overriding the Instrument ID");
                    else
                    {
                        mOptions.Instrument_id = strValue;
                    }
                }

                if (objParseCommandLine.RetrieveValueForParameter("M", out strValue))
                {
                    if (string.IsNullOrWhiteSpace(strValue))
                        ShowErrorMessage("/M does not have a value; not customizing the metrics to compute");
                    else
                    {
                        mOptions.MeasurementsFile = strValue;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error parsing the command line parameters: " + Environment.NewLine + ex.Message);
            }

            return false;
        }

        private static void ShowErrorMessage(string message)
        {
            ConsoleMsgUtils.ShowError(message);
        }

        private static void ShowErrorMessage(string title, IEnumerable<string> errorMessages)
        {
            ConsoleMsgUtils.ShowErrors(title, errorMessages);
        }

        private static void ShowProgramHelp()
        {
            var exeName = Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().Location);

            try
            {
                Console.WriteLine();
                Console.WriteLine("SMAQC computes quality metrics for a LC-MS/MS dataset.  The software");
                Console.WriteLine("requires that the dataset first be processed with MASIC, then processed");
                Console.WriteLine("with MSGF+ or X!Tandem.  The MSGF+ or X!Tandem results must be");
                Console.WriteLine("post-processed with the PeptideHitResultsProcessor (PHRP).");
                Console.WriteLine();
                Console.WriteLine("SMAQC reads the data from the _syn.txt file along with the parallel");
                Console.WriteLine("text files created by PHRP.  It uses this information to compute peptide");
                Console.WriteLine("count related metrics (peptides are filtered on MSGF_SpecProb");
                Console.WriteLine("less than " + Measurement.MSGF_SPECPROB_THRESHOLD.ToString("0E+00") + "). SMAQC also reads the data from the _ScanStats.txt, ");
                Console.WriteLine("_SICstats.txt, and _ScanStatsEx.txt files created by MASIC");
                Console.WriteLine("to determine chromatography and scan-related metrics.");
                Console.WriteLine();
                Console.WriteLine("The quality metrics computed by SMAQC are based on the metrics proposed");
                Console.WriteLine("by Rudnick and Stein, as described in \"Performance metrics for liquid ");
                Console.WriteLine("chromatography-tandem mass spectrometry systems in proteomics analyses.\",");
                Console.WriteLine("Mol Cell Proteomics. 2010 Feb;9(2):225-41. doi: 10.1074/mcp.M900223-MCP200.");
                Console.WriteLine();
                Console.WriteLine("Program syntax:" + Environment.NewLine + exeName);
                Console.WriteLine(" DatasetFolderPath [/O:OutputFilePath] [/DB:DatabaseFolder]");
                Console.WriteLine(" [/I:InstrumentID] [/M:MeasurementsFile]");
                Console.WriteLine();
                Console.WriteLine("DatasetFolderPath specifies path to the folder with the dataset(s) to process; use quotes if spaces");
                Console.WriteLine();
                Console.WriteLine("Use /O to specify the output file path. If /O is not used, then");
                Console.WriteLine("results will only be stored in the SQLite database");
                Console.WriteLine("Examples: /O:Metrics.txt   or   /O:\"C:\\Results Folder\\Metrics.txt\"");
                Console.WriteLine();
                Console.WriteLine("Use /DB to specify where the SQLite database should be created (default is with the .exe)");
                Console.WriteLine("Use /I to specify an instrument ID (number or text); defaults to /I:1");
                Console.WriteLine();
                Console.WriteLine("Use /M to specify the path to the XML file containing the measurements to run.");
                Console.WriteLine("If /M is not used, then all of the metrics will be computed");
                Console.WriteLine();
                Console.WriteLine("Program written by Matthew Monroe, in collaboration with computer science students");
                Console.WriteLine("at Washington State University for the Department of Energy (PNNL, Richland, WA) in 2012");
                Console.WriteLine("");
                Console.WriteLine("Version: " + GetAppVersion());
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com");
                Console.WriteLine("Website: http://panomics.pnnl.gov/ or http://omics.pnl.gov or http://www.sysbio.org/resources/staff/");
                Console.WriteLine();

                // Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                Thread.Sleep(1500);

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error displaying the program syntax: " + ex.Message);
            }

        }

        /// <summary>
        /// Store the results
        /// </summary>
        /// <param name="instrumentId"></param>
        /// <param name="randomId"></param>
        /// <param name="scanId"></param>
        /// <param name="lstMeasurementsToRun"></param>
        static void AddScanResults(string instrumentId, int randomId, int scanId, IReadOnlyCollection<string> lstMeasurementsToRun)
        {

            // Query to store data in scan_results
            var sql = BuildScanResultsQuery(instrumentId, randomId, scanId, lstMeasurementsToRun);

            mDBWrapper.SetQuery(sql);

            mDBWrapper.ExecuteNonQuery();

        }

        /// <summary>
        /// Construct the query to append to scan_results
        /// </summary>
        /// <param name="instrumentId"></param>
        /// <param name="randomId"></param>
        /// <param name="scanId"></param>
        /// <param name="lstMeasurementsToRun"></param>
        /// <returns></returns>
        private static string BuildScanResultsQuery(string instrumentId, int randomId, int scanId, IReadOnlyCollection<string> lstMeasurementsToRun)
        {
            mQueryBuilder.Clear();
            mQueryBuilder.Append("INSERT INTO scan_results ( scan_id, instrument_id, random_id, scan_date");

            // Append each metric, e.g. `C_1A`
            foreach (var item in lstMeasurementsToRun)
            {
                mQueryBuilder.Append(", `" + item + "`");
            }
            mQueryBuilder.Append(") VALUES (");

            // Append the metadata values
            mQueryBuilder.Append("'" + scanId + "',");
            mQueryBuilder.Append("'" + instrumentId + "',");
            mQueryBuilder.Append("'" + randomId + "',");

            mQueryBuilder.Append(mDBWrapper.GetDateTime());

            // Append the metric values (as strings), e.g. '3.2342'
            foreach (var item in lstMeasurementsToRun)
            {
                mQueryBuilder.Append(", '" + mResults[item] + "'");
            }

            mQueryBuilder.Append(");");

            return mQueryBuilder.ToString();
        }

        /// <summary>
        /// Check the DB for the next available result ID
        /// </summary>
        /// <returns></returns>
        /// <remarks>Multiple datasets processed at once will have the same Result_ID</remarks>
        static int DetermineResultId()
        {
            // In the scan_results table, result_id is PRIMARY KEY AUTOINCREMENT
            // scan_id is inserted into the table, but is actually equivalent to result_id
            mDBWrapper.SetQuery("SELECT Max(result_id) AS result_id FROM scan_results;");

            mDBWrapper.InitReader();

            string[] field_array = { "result_id" };

            mDBWrapper.ReadSingleLine(field_array, out var dctMostRecentEntry);


            if (int.TryParse(dctMostRecentEntry["result_id"], out var resultId))
            {
                return resultId + 1;
            }

            return 1;
        }

        /// <summary>
        /// Determine the names of the measurements that should be run
        /// </summary>
        /// <param name="measurementsToRunFile"></param>
        /// <returns></returns>
        static List<string> LoadMeasurementInfoFile(string measurementsToRunFile)
        {
            var useDefaultMetrics = false;
            var lstMeasurementsToRun = new List<string>();

            if (string.IsNullOrEmpty(measurementsToRunFile))
                useDefaultMetrics = true;
            else
            {
                var fiMeasurementsToRunFile = new FileInfo(measurementsToRunFile);

                if (!fiMeasurementsToRunFile.Exists)
                {
                    mSystemLogManager.AddApplicationLogWarning("Warning, measurement file was not found: " + fiMeasurementsToRunFile.FullName);
                    mSystemLogManager.AddApplicationLog("Will use the default metrics");
                    useDefaultMetrics = true;
                }
                else
                {
                    try
                    {
                        using (var fsMeasurementsFile = new FileStream(fiMeasurementsToRunFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
                        {
                            var parser = new XmlTextReader(fsMeasurementsFile);

                            while (parser.ReadToFollowing("measurement"))
                            {
                                parser.MoveToAttribute("name");
                                lstMeasurementsToRun.Add(parser.Value);

                                if (parser.Value.Equals("*", StringComparison.InvariantCulture))
                                {
                                    useDefaultMetrics = true;
                                    break;
                                }
                            }

                        }
                    }
                    catch (Exception ex)
                    {
                        ShowErrorMessage("Error reading the metrics to run from " + fiMeasurementsToRunFile.FullName + ": " + ex.Message);
                        useDefaultMetrics = true;
                    }

                }
            }

            if (useDefaultMetrics)
            {
                var metricNames = (from item in mMetricNames where item != "instrument_id" && item != "random_id" && item != "scan_date" select item);

                lstMeasurementsToRun.Clear();
                lstMeasurementsToRun.AddRange(metricNames);

            }

            return lstMeasurementsToRun;
        }

    }
}
