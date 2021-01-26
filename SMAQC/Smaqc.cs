using System;
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
        // Ignore Spelling: Phos, Rudnick, mol, doi

        private struct ProcessingOptions
        {
            public string InputDirectoryPath;
            public string Instrument_id;
            public string MeasurementsFile;
            public string OutputFilePath;
            public string DatabaseDirectoryPath;

            public void Clear()
            {
                InputDirectoryPath = string.Empty;
                Instrument_id = "1";
                MeasurementsFile = string.Empty;
                OutputFilePath = string.Empty;
                DatabaseDirectoryPath = string.Empty;
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
        private static ProcessingOptions mOptions;

        private static readonly StringBuilder mQueryBuilder = new StringBuilder();

        // Measurement results
        private static Dictionary<string, string> mResults = new Dictionary<string, string>();

        private const string SMAQC_BUILD_DATE = "January 26, 2021";

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

        private static int Main()
        {
            var random = new Random();
            var randomId = random.Next();

            var commandLineParser = new clsParseCommandLine();
            var success = false;

            mOptions = new ProcessingOptions();
            mOptions.Clear();

            if (commandLineParser.ParseCommandLine())
            {
                if (SetOptionsUsingCommandLineParameters(commandLineParser))
                    success = true;
            }

            if (!success ||
                commandLineParser.NeedToShowHelp ||
                commandLineParser.ParameterCount + commandLineParser.NonSwitchParameterCount == 0 ||
                string.IsNullOrEmpty(mOptions.InputDirectoryPath))
            {
                ShowProgramHelp();
                return -1;
            }

            // Show the processing options
            Console.WriteLine();
            Console.WriteLine("Instrument ID: ".PadRight(20) + mOptions.Instrument_id);
            Console.WriteLine("Path to datasets: ".PadRight(20) + mOptions.InputDirectoryPath);
            if (string.IsNullOrEmpty(mOptions.MeasurementsFile))
                Console.WriteLine("Using default metrics");
            else
                Console.WriteLine("Measurements file: ".PadRight(20) + mOptions.MeasurementsFile);
            if (!string.IsNullOrEmpty(mOptions.OutputFilePath))
                Console.WriteLine("Text results file: ".PadRight(20) + mOptions.OutputFilePath);
            Console.WriteLine("SQLite DB directory: ".PadRight(20) + mOptions.DatabaseDirectoryPath);
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
                var measurementsToRun = LoadMeasurementInfoFile(mOptions.MeasurementsFile);

                try
                {
                    // Connect to the database
                    mDBWrapper = new DBWrapper(mOptions.DatabaseDirectoryPath, false);

                    // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                    if (WIPE_TEMP_DATA_AT_START)
                        mDBWrapper.ClearTempTables();

                    mAggregate = new Aggregate(mOptions.InputDirectoryPath);
                    mMeasurement = new Measurement(randomId, mDBWrapper);
                    mMeasurementEngine = new MeasurementEngine(measurementsToRun, mMeasurement, mSystemLogManager);
                    mFilter = new Filter(mDBWrapper, mOptions.Instrument_id, randomId, mSystemLogManager);
                    mOutputFileManager = new OutputFileManager(mDBWrapper, GetAppVersion(), mMetricNames);

                    try
                    {
                        var errorCode = ProcessDatasets(randomId, measurementsToRun);
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
        /// Process the datasets in the specified input directory
        /// </summary>
        /// <param name="randomId"></param>
        /// <param name="measurementsToRun"></param>
        /// <returns>0 if success, otherwise an error code</returns>
        private static int ProcessDatasets(int randomId, IReadOnlyCollection<string> measurementsToRun)
        {
            mSystemLogManager.AddApplicationLog("Searching for Text Files...");

            // Detect datasets
            var DatasetNames = mAggregate.DetectDatasets();

            if (DatasetNames.Count == 0)
            {
                // No datasets were found
                mSystemLogManager.AddApplicationLogError("Unable to find any datasets in " + mOptions.InputDirectoryPath);
                mSystemLogManager.AddApplicationLogWarning(
                    "Copy _ScanStats.txt, _ScanStatsEx.txt, _SICStats.txt, plus PHRP _syn* files to the directory then try again");
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
                    foreach (var fileSuffix in mMasicFileExtensions)
                    {
                        var matchFound = false;

                        foreach (var candidateFile in masicFileList)
                        {
                            var fileName = Path.GetFileNameWithoutExtension(candidateFile.Key);
                            if (fileName?.EndsWith(fileSuffix, StringComparison.OrdinalIgnoreCase) == true)
                            {
                                matchFound = true;
                                break;
                            }
                        }

                        if (matchFound)
                            continue;

                        if (string.Equals(fileSuffix, "_ScanStats", StringComparison.OrdinalIgnoreCase))
                        {
                            scanStatsMissing = true;
                        }
                        else
                        if (string.Equals(fileSuffix, "_ScanStatsEx", StringComparison.OrdinalIgnoreCase))
                        {
                            scanStatsExMissing = true;
                        }
                        else
                        if (string.Equals(fileSuffix, "_SICStats", StringComparison.OrdinalIgnoreCase))
                        {
                            sicStatsMissing = true;
                        }
                        else
                        if (string.Equals(fileSuffix, "_ReporterIons", StringComparison.OrdinalIgnoreCase))
                        {
                            reporterIonsMissing = true;
                        }
                        else
                        {
                            mSystemLogManager.AddApplicationLog("  Missing unrecognized file: " + datasetName + fileSuffix + ".txt");
                        }
                    }

                    if (scanStatsMissing || sicStatsMissing)
                    {
                        // Missing required files
                        mSystemLogManager.AddApplicationLog("Required MASIC data files not found in " +
                                                             mOptions.InputDirectoryPath);
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
                    mFilter.LoadFilesUsingPHRP(mOptions.InputDirectoryPath, datasetName);

                    // Load the MASIC data
                    mFilter.LoadFilesAndInsertIntoDB(masicFileList, mMasicFileExtensions, datasetName);

                    // Run the measurements
                    mSystemLogManager.AddApplicationLog("Now running Measurements on " + datasetName);

                    mResults = mMeasurementEngine.RunMeasurements();

                    // Store the results
                    mSystemLogManager.AddApplicationLog("Saving Scan Results");
                    AddScanResults(mOptions.Instrument_id, randomId, scanId, measurementsToRun);

                    // ReSharper disable once ConditionIsAlwaysTrueOrFalse
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

        private static string GetAppVersion()
        {
            return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " (" + SMAQC_BUILD_DATE + ")";
        }

        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine commandLineParser)
        {
            // Returns True if no problems; otherwise, returns false
            var validParameters = new List<string> { "O", "DB", "I", "M" };

            try
            {
                // Make sure no invalid parameters are present
                if (commandLineParser.InvalidParametersPresent(validParameters))
                {
                    var badArguments = new List<string>();
                    foreach (var item in commandLineParser.InvalidParameters(validParameters))
                    {
                        badArguments.Add("/" + item);
                    }

                    ShowErrorMessage("Invalid command line parameters", badArguments);

                    return false;
                }

                // Query commandLineParser to see if various parameters are present
                if (commandLineParser.NonSwitchParameterCount > 0)
                {
                    mOptions.InputDirectoryPath = commandLineParser.RetrieveNonSwitchParameter(0);
                }

                if (commandLineParser.RetrieveValueForParameter("O", out var value))
                {
                    if (string.IsNullOrWhiteSpace(value))
                        ShowErrorMessage("/O does not have a value; not overriding the output file path");
                    else
                        mOptions.OutputFilePath = value;
                }

                if (commandLineParser.RetrieveValueForParameter("DB", out value))
                {
                    if (string.IsNullOrWhiteSpace(value))
                    {
                        ShowErrorMessage("/DB does not have a value; not overriding the database directory path");
                    }
                    else
                    {
                        mOptions.DatabaseDirectoryPath = value;
                    }
                }

                if (commandLineParser.RetrieveValueForParameter("I", out value))
                {
                    if (string.IsNullOrWhiteSpace(value))
                    {
                        ShowErrorMessage("/I does not have a value; not overriding the Instrument ID");
                    }
                    else
                    {
                        mOptions.Instrument_id = value;
                    }
                }

                if (commandLineParser.RetrieveValueForParameter("M", out value))
                {
                    if (string.IsNullOrWhiteSpace(value))
                    {
                        ShowErrorMessage("/M does not have a value; not customizing the metrics to compute");
                    }
                    else
                    {
                        mOptions.MeasurementsFile = value;
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
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                    "SMAQC (Software Metrics for Analysis of Quality Control) computes quality metrics for an LC-MS/MS dataset. " +
                    "The software requires that the dataset first be processed with MASIC, then processed " +
                    "with MS-GF+ or X!Tandem. The MS-GF+ or X!Tandem results must be " +
                    "post-processed with the PeptideHitResultsProcessor (PHRP)."));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                    "SMAQC reads the data from the _syn.txt file along with the parallel " +
                    "text files created by PHRP.  It uses this information to compute peptide " +
                    "count related metrics (peptides are filtered on MSGFDB_SpecEValue " +
                    "less than " + Measurement.MSGF_SPECPROB_THRESHOLD.ToString("0E+00") + "; this column was previously named MSGF_SpecProb). " +
                    "SMAQC also reads the data from the _ScanStats.txt, " +
                    // ReSharper disable once StringLiteralTypo
                    "_SICstats.txt, and _ScanStatsEx.txt files created by MASIC " +
                    "to determine chromatography and scan-related metrics."));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                    "The quality metrics computed by SMAQC are based on the metrics proposed " +
                    "by Rudnick and Stein, as described in \"Performance metrics for liquid " +
                    "chromatography-tandem mass spectrometry systems in proteomics analyses.\",\n" +
                    "Mol Cell Proteomics. 2010 Feb; 9(2):225-41. doi: 10.1074/mcp.M900223-MCP200."));
                Console.WriteLine();
                Console.WriteLine("Program syntax:" + Environment.NewLine + exeName);
                Console.WriteLine(" DatasetDirectoryPath [/O:OutputFilePath] [/DB:DatabaseDirectory]");
                Console.WriteLine(" [/I:InstrumentID] [/M:MeasurementsFile]");
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "DatasetDirectoryPath specifies path to the directory with the dataset(s) to process; use quotes if spaces"));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "Use /O to specify the output file path. If /O is not used, " +
                                      "results will only be stored in the SQLite database"));
                Console.WriteLine(@"Examples: /O:Metrics.txt or /O:""C:\Results Directory\Metrics.txt""");
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "Use /DB to specify where the SQLite database should be created (default is with the .exe)"));
                Console.WriteLine();
                Console.WriteLine("Use /I to specify an instrument ID (number or text); defaults to /I:1");
                Console.WriteLine();
                Console.WriteLine("Use /M to specify the path to the XML file containing the measurements to run.");
                Console.WriteLine("If /M is not used, all of the metrics will be computed");
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "Program written by Matthew Monroe, in collaboration with computer science students " +
                                      "at Washington State University for the Department of Energy (PNNL, Richland, WA) in 2012"));
                Console.WriteLine();
                Console.WriteLine("Version: " + GetAppVersion());
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov");
                Console.WriteLine("Website: https://omics.pnl.gov or https://panomics.pnl.gov/");
                Console.WriteLine();

                // Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                Thread.Sleep(750);
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
        /// <param name="measurementsToRun"></param>
        private static void AddScanResults(string instrumentId, int randomId, int scanId, IReadOnlyCollection<string> measurementsToRun)
        {
            // Query to store data in scan_results
            var sql = BuildScanResultsQuery(instrumentId, randomId, scanId, measurementsToRun);

            mDBWrapper.SetQuery(sql);

            mDBWrapper.ExecuteNonQuery();
        }

        /// <summary>
        /// Construct the query to append to scan_results
        /// </summary>
        /// <param name="instrumentId"></param>
        /// <param name="randomId"></param>
        /// <param name="scanId"></param>
        /// <param name="measurementsToRun"></param>
        private static string BuildScanResultsQuery(string instrumentId, int randomId, int scanId, IReadOnlyCollection<string> measurementsToRun)
        {
            mQueryBuilder.Clear();
            mQueryBuilder.Append("INSERT INTO scan_results ( scan_id, instrument_id, random_id, scan_date");

            // Append each metric, e.g. `C_1A`
            foreach (var item in measurementsToRun)
            {
                mQueryBuilder.AppendFormat(", `{0}`", item);
            }
            mQueryBuilder.Append(") VALUES (");

            // Append the metadata values
            mQueryBuilder.AppendFormat("'{0}',", scanId);
            mQueryBuilder.AppendFormat("'{0}',", instrumentId);
            mQueryBuilder.AppendFormat("'{0}',", randomId);

            mQueryBuilder.Append(mDBWrapper.GetDateTime());

            // Append the metric values (as strings), e.g. '3.2342'
            foreach (var item in measurementsToRun)
            {
                mQueryBuilder.AppendFormat(", '{0}'", mResults[item]);
            }

            mQueryBuilder.Append(");");

            return mQueryBuilder.ToString();
        }

        /// <summary>
        /// Check the DB for the next available result ID
        /// </summary>
        /// <remarks>Multiple datasets processed at once will have the same Result_ID</remarks>
        private static int DetermineResultId()
        {
            // In the scan_results table, result_id is PRIMARY KEY AUTOINCREMENT
            // scan_id is inserted into the table, but is actually equivalent to result_id
            mDBWrapper.SetQuery("SELECT Max(result_id) AS result_id FROM scan_results;");

            mDBWrapper.InitReader();

            string[] columnNames = { "result_id" };

            mDBWrapper.ReadSingleLine(columnNames, out var dctMostRecentEntry);

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
        private static List<string> LoadMeasurementInfoFile(string measurementsToRunFile)
        {
            var useDefaultMetrics = false;
            var measurementsToRun = new List<string>();

            if (string.IsNullOrEmpty(measurementsToRunFile))
            {
                useDefaultMetrics = true;
            }
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
                                measurementsToRun.Add(parser.Value);

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

                measurementsToRun.Clear();
                measurementsToRun.AddRange(metricNames);
            }

            return measurementsToRun;
        }
    }
}
