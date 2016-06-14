using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Xml;

namespace SMAQC
{
	class Smaqc
	{
		protected struct udtOptions
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

		protected const bool WIPE_TEMP_DATA_AT_START = true;
		protected const bool KEEP_TEMP_DATA_AT_END = true;

        // DB Interface objerct
		public static DBWrapper m_DBWrapper;
		public static Aggregate m_Aggregate;
		public static Measurement m_Measurement;

        // Filter engine
		public static Filter m_Filter;

        // Measurement engine
		public static MeasurementEngine m_MeasurementEngine;

		public static SystemLogManager m_SystemLogManager = new SystemLogManager();
		public static OutputFileManager m_OutputFileManager;

		protected static udtOptions m_Options;

        // Configuration options
		public static Dictionary<string, string> m_Configtable = new Dictionary<string, string>();

        // Measurement results
		public static Dictionary<string, string> m_Results = new Dictionary<string, string>();

		private const string SMAQC_BUILD_DATE = "June 13, 2016";

		// Define the filename suffixes
		private static readonly string[] m_MasicFileNames = { "_scanstats", "_scanstatsex", "_sicstats", "_reporterions" };

		// Fields to track in the database
		// Note that LoadMeasurementInfoFile uses this list to define the default metrics to run (skipping "instrument_id", "random_id", and "scan_date")
		private static readonly List<string> m_Fields = new List<string> { 
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
			var random_id = random.Next();

			var objParseCommandLine = new FileProcessor.clsParseCommandLine();
			var success = false;

			m_Options.Clear();

			if (objParseCommandLine.ParseCommandLine())
			{
				if (SetOptionsUsingCommandLineParameters(objParseCommandLine))
					success = true;
			}

			if (!success ||
				objParseCommandLine.NeedToShowHelp ||
				objParseCommandLine.ParameterCount + objParseCommandLine.NonSwitchParameterCount == 0 ||
				string.IsNullOrEmpty(m_Options.InputFolderPath))
			{
				ShowProgramHelp();
				return -1;

			}

			// Show the processing options
			Console.WriteLine();
			Console.WriteLine("Instrument ID: ".PadRight(20) + m_Options.Instrument_id);
			Console.WriteLine("Path to datasets: ".PadRight(20) + m_Options.InputFolderPath);
			if (string.IsNullOrEmpty(m_Options.MeasurementsFile))
				Console.WriteLine("Using default metrics");
			else
				Console.WriteLine("Measurements file: ".PadRight(20) + m_Options.MeasurementsFile);
            if (!string.IsNullOrEmpty(m_Options.OutputFilePath))
				Console.WriteLine("Text results file: ".PadRight(20) + m_Options.OutputFilePath);
			Console.WriteLine("SQLite DB folder: ".PadRight(20) + m_Options.DBFolderPath);
			Console.WriteLine();

			try
			{
				// Create the application log
				m_SystemLogManager.createApplicationLog();

				m_SystemLogManager.addApplicationLog("SMAQC Version " + GetAppVersion());

			}
			catch (Exception ex)
			{
				ShowErrorMessage("Error initializing log file: " + ex.Message);
				return 6;
			}


			try
			{

				// Obtain the list of measurements to run
				var lstMeasurementsToRun = LoadMeasurementInfoFile(m_Options.MeasurementsFile);

				try
				{

                    // Connect to the database
					m_DBWrapper = new DBWrapper(m_Options.DBFolderPath)
					{
						ShowQueryText = false
					};

					if (WIPE_TEMP_DATA_AT_START)
						m_DBWrapper.ClearTempTables();

					m_Aggregate = new Aggregate(m_Options.InputFolderPath);																	// AGGREGATE OBJECT
					m_Measurement = new Measurement(random_id, ref m_DBWrapper);															// MEASUREMENT LIST OBJECT
					m_MeasurementEngine = new MeasurementEngine(lstMeasurementsToRun, ref m_Measurement, ref m_SystemLogManager);      // MEASUREMENT ENGINE OBJECT
					m_Filter = new Filter(ref m_DBWrapper, m_Options.Instrument_id, random_id, ref m_SystemLogManager);                              // FILTER OBJECT
					m_OutputFileManager = new OutputFileManager(ref m_DBWrapper, GetAppVersion(), m_Fields);			// OUTPUTFILE MANAGER OBJECT

					try
					{
						var errorCode = ProcessDatasets(random_id, lstMeasurementsToRun);
						if (errorCode != 0)
							return errorCode;
					}
					catch (Exception ex)
					{
						var message = "Error processing datasets: " + ex.Message;
						ShowErrorMessage(message);
						m_SystemLogManager.addApplicationLog(message);
						return 9;
					}

					try
					{
						m_SystemLogManager.addApplicationLog("SMAQC analysis complete");
						
						m_SystemLogManager.CloseLogFile();
					}
					catch (Exception ex)
					{
						ShowErrorMessage("Error closing the log file: " + ex.Message);
						return 10;
					}

				}
				catch (Exception ex)
				{
					ShowErrorMessage("Error initializing database wrapper: " + ex.Message);
					return 7;
				}

			}
			catch (Exception ex)
			{
				ShowErrorMessage("Error initializing program: " + ex.Message);
				return 8;
			}

			return 0;
		}

		/// <summary>
		/// Process the datasets in the specified input folder
		/// </summary>
		/// <param name="random_id"></param>
		/// <param name="lstMeasurementsToRun"></param>
		/// <returns>0 if success, otherwise an error code</returns>
		private static int ProcessDatasets(int random_id, List<string> lstMeasurementsToRun)
		{

			m_SystemLogManager.addApplicationLog("Searching for Text Files...");

			// Detect datasets
			var DatasetNames = m_Aggregate.DetectDatasets();

			if (DatasetNames.Count == 0)
			{
                // No datasets were found
				m_SystemLogManager.addApplicationLog("Unable to find any datasets in " + m_Options.InputFolderPath);
				m_SystemLogManager.addApplicationLog("Exiting...");
				return -2;
			}

			// Get the next available result_id
			var result_id = determine_result_id();

			// Process the datasets
			var datasetNumber = 0;
			foreach (var datasetName in DatasetNames)
			{
				try
				{
					m_Aggregate.setDataset(datasetName);

					var MasicFileList = m_Aggregate.getMasicFileImportList(datasetName, "*.txt");

					// Ensure that the MASIC files exist
					if (MasicFileList.Count < 3)
					{
						// Missing files
						m_SystemLogManager.addApplicationLog("Required MASIC data files not found in " + m_Options.InputFolderPath);

						var bScanStatsExMissing = false;

						// Find the missing files
						foreach (var sSuffix in m_MasicFileNames)
						{
							var bMatchFound = false;

							foreach (var sFilePath in MasicFileList)
							{
								var sFileName = Path.GetFileNameWithoutExtension(sFilePath);
								if (sFileName != null && sFileName.EndsWith(sSuffix, true, System.Globalization.CultureInfo.CurrentCulture))
								{
									bMatchFound = true;
									break;
								}
							}

							if (!bMatchFound)
							{
								if (sSuffix == "_scanstatsex")
									bScanStatsExMissing = true;
								else
									m_SystemLogManager.addApplicationLog("  Missing file: " + datasetName + sSuffix + ".txt");
							}

						}

						if (MasicFileList.Count == 2 && bScanStatsExMissing)
							// The only missing file is the _ScanStatsEx file; that's OK
							m_SystemLogManager.addApplicationLog("Did not find file " + datasetName + "_ScanStatsEx.txt; metrics MS1_1 and MS2_1 will not be computed");
						else
						{
							m_SystemLogManager.addApplicationLog("Exiting...");

							//CLOSE THE APPLICATION LOG
							m_SystemLogManager.CloseLogFile();
							return 11;
						}
					}


					// Load the data and store in the database
					m_SystemLogManager.addApplicationLog("Parsing and Inserting Data into DB Temp Tables");

					// Load data using PHRP
					m_Filter.LoadFilesUsingPHRP(m_Options.InputFolderPath, datasetName);

					// Load the MASIC data
					m_Filter.LoadFilesAndInsertIntoDB(MasicFileList, m_MasicFileNames, datasetName);

					// Run the measurements
					m_SystemLogManager.addApplicationLog("Now running Measurements on " + datasetName);

					m_Results = m_MeasurementEngine.RunMeasurements();

					// Store the results
					m_SystemLogManager.addApplicationLog("Saving Scan Results");
					add_scan_results(m_Options.Instrument_id, random_id, result_id, lstMeasurementsToRun);

					if (!KEEP_TEMP_DATA_AT_END)
					{
                        // Remove the working data
						m_DBWrapper.ClearTempTables(random_id);
					}

					if (!string.IsNullOrEmpty(m_Options.OutputFilePath))
					{
						// Write the results to a file
						m_OutputFileManager.SaveData(datasetName, m_Options.OutputFilePath, Convert.ToInt32(m_Configtable["scan_id"]), datasetNumber);

						m_SystemLogManager.addApplicationLog("Scan output has been saved to " + m_Options.OutputFilePath);
					}
					else
					{
						m_SystemLogManager.addApplicationLog("Scan result saved to SQLite DB (Scan ID=" + m_Configtable["scan_id"] + ")");
					}

					m_Results.Clear();

				}
				catch (Exception ex)
				{
					m_SystemLogManager.addApplicationLog("Error processing dataset " + datasetName + ": " + ex.Message);
				    Console.WriteLine(ex.StackTrace);
					return 5;
				}

				datasetNumber++;
			}

			return 0;
		}

		static protected string GetAppPath()
		{
			return System.Reflection.Assembly.GetExecutingAssembly().Location;
		}

		private static string GetAppVersion()
		{
			return System.Reflection.Assembly.GetExecutingAssembly().GetName().Version + " (" + SMAQC_BUILD_DATE + ")";
		}

		private static bool SetOptionsUsingCommandLineParameters(FileProcessor.clsParseCommandLine objParseCommandLine)
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
					m_Options.InputFolderPath = objParseCommandLine.RetrieveNonSwitchParameter(0);
				}

				string strValue;
				if (objParseCommandLine.RetrieveValueForParameter("O", out strValue))
				{
					if (string.IsNullOrWhiteSpace(strValue))
						ShowErrorMessage("/O does not have a value; not overriding the output file path");
					else
						m_Options.OutputFilePath = strValue;
				}

				if (objParseCommandLine.RetrieveValueForParameter("DB", out strValue))
				{
					if (string.IsNullOrWhiteSpace(strValue))
						ShowErrorMessage("/DB does not have a value; not overriding the database folder path");
					else
					{
						m_Options.DBFolderPath = strValue;
					}
				}

				if (objParseCommandLine.RetrieveValueForParameter("I", out strValue))
				{
					if (string.IsNullOrWhiteSpace(strValue))
						ShowErrorMessage("/I does not have a value; not overriding the Instrument ID");
					else
					{
						m_Options.Instrument_id = strValue;
					}
				}

				if (objParseCommandLine.RetrieveValueForParameter("M", out strValue))
				{
					if (string.IsNullOrWhiteSpace(strValue))
						ShowErrorMessage("/M does not have a value; not customizing the metrics to compute");
					else
					{
						m_Options.MeasurementsFile = strValue;
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

		private static void ShowErrorMessage(string strMessage)
		{
			const string strSeparator = "------------------------------------------------------------------------------";

			Console.WriteLine();
			Console.WriteLine(strSeparator);
			Console.WriteLine(strMessage);
			Console.WriteLine(strSeparator);
			Console.WriteLine();

			WriteToErrorStream(strMessage);
		}

		private static void ShowErrorMessage(string strTitle, IEnumerable<string> items)
		{
			const string strSeparator = "------------------------------------------------------------------------------";

		    Console.WriteLine();
			Console.WriteLine(strSeparator);
			Console.WriteLine(strTitle);
			var strMessage = strTitle + ":";

			foreach (var item in items)
			{
				Console.WriteLine("   " + item);
				strMessage += " " + item;
			}
			Console.WriteLine(strSeparator);
			Console.WriteLine();

			WriteToErrorStream(strMessage);
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
				System.Threading.Thread.Sleep(750);

			}
			catch (Exception ex)
			{
				Console.WriteLine("Error displaying the program syntax: " + ex.Message);
			}

		}


		private static void WriteToErrorStream(string strErrorMessage)
		{
			try
			{
				using (var swErrorStream = new StreamWriter(Console.OpenStandardError()))
				{
					swErrorStream.WriteLine(strErrorMessage);
				}
			}
			// ReSharper disable once EmptyGeneralCatchClause
			catch
			{
				// Ignore errors here
			}
		}

		static void ShowErrorMessage(string message, bool pauseAfterError)
		{
			Console.WriteLine();
			Console.WriteLine("===============================================");

			Console.WriteLine(message);

			if (pauseAfterError)
			{
				Console.WriteLine("===============================================");
				System.Threading.Thread.Sleep(1500);
			}
		}

		/// <summary>
		/// Store the results
		/// </summary>
		/// <param name="instrument_id"></param>
		/// <param name="random_id"></param>
		/// <param name="result_id"></param>
		/// <param name="lstMeasurementsToRun"></param>
		static void add_scan_results(string instrument_id, int random_id, int result_id, List<string> lstMeasurementsToRun)
		{

            // Remove scan_id (required in case multiple datasets were processed 
			if (m_Configtable.ContainsKey("scan_id"))
				m_Configtable.Remove("scan_id");

			//BUILD SCAN RESULTS QUERY
			var scan_results_query = build_scan_results_query(instrument_id, random_id, result_id, lstMeasurementsToRun);

			//SET QUERY TO STORE DATA TO SCAN_STATS
			m_DBWrapper.setQuery(scan_results_query);

			//EXECUTE QUERY
			m_DBWrapper.QueryNonQuery();

			//SET SCAN_ID
			m_Configtable.Add("scan_id", result_id.ToString());
		}

		//BUILD SCAN_RESULTS INSERT QUERY
		static string build_scan_results_query(string instrument_id, int random_id, int result_id, List<string> lstMeasurementsToRun)
		{
			//DECLARE VARIABLES

			//HEAD OF RESULTS STRING QUERY
			var scan_results_query = "INSERT INTO scan_results ( scan_id, instrument_id, random_id, scan_date";

			//BUILD METRICS FIELDS [, `C_1A` ...]
			foreach (var item in lstMeasurementsToRun)
			{
				scan_results_query += ", `" + item + "`";
			}
			scan_results_query += ") VALUES (";

			//BUILD VALUES FOR SCAN_ID, INSTRUMENT_ID, RANDOM_ID
			scan_results_query += "'" + result_id + "',";
			scan_results_query += "'" + instrument_id + "',";
			scan_results_query += "'" + random_id + "',";

			//BUILD DATE STRINGS
			scan_results_query += m_DBWrapper.getDateTime();// +",";

			//BUILD METRICS VALUE LIST ["'" + resultstable["C_1A"] + "',"]
			foreach (var item in lstMeasurementsToRun)
			{
				scan_results_query += ", '" + m_Results[item] + "'";
				//Console.WriteLine("KEY=[{0}] -- VALUE=[{1}]", key, resultstable[measurementsDict[key]]);
			}

			//BUILD END
			scan_results_query += ");";

			return scan_results_query;
		}

        /// <summary>
        /// Check the DB for the next available result ID
        /// </summary>
        /// <returns></returns>
        /// <remarks>Multiple datasets processed at once will have the same Result_ID</remarks>
		static int determine_result_id()
		{
			var dctMostRecentEntry = new Dictionary<string, string>();
			int result_id;

			m_DBWrapper.setQuery("SELECT Max(result_id) AS result_id FROM scan_results;");

			m_DBWrapper.initReader();

			string[] field_array = { "result_id" };

			m_DBWrapper.readSingleLine(field_array, ref dctMostRecentEntry);

			if (int.TryParse(dctMostRecentEntry["result_id"], out result_id))
				result_id++;
			else
				result_id = 1;

			return result_id;
		}

		
		//[Obsolete("These config values are not used")]
		//static void loadConfig()
		//{
		//    // Define the default config values

		//    m_Configtable.Clear();
		//    m_Configtable.Add("dbhost", "localhost");
		//    m_Configtable.Add("dbuser", "root");
		//    m_Configtable.Add("dbpass", "password");
		//    m_Configtable.Add("dbname", "SMAQC_DB");
		//    m_Configtable.Add("dbtype", "SQLite");

		//    string configFilePath = Path.GetDirectoryName(GetAppPath());
		//    if (string.IsNullOrWhiteSpace(configFilePath))
		//        configFilePath = "";

		//    var fiConfigFile = new FileInfo(Path.Combine(configFilePath, "config.xml"));

		//    if (!fiConfigFile.Exists)
		//    {
		//        return;
		//    }

		//    //OPEN XML DOC + INIT PARSER
		//    using (var configFile = new FileStream(fiConfigFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
		//    {

		//        var parser = new XmlTextReader(configFile);
		//        var configKeys = (from item in m_Configtable select item.Key).ToList();

		//        // LOOP THROUGH EACH VARIABLE IN XML CONFIG
		//        // Note that this code logic requires that the settings in the config file be in the 
		//        foreach (string fieldName in configKeys)
		//        {
		//            if (parser.ReadToFollowing(fieldName))
		//            {
		//                //READ ATTRIBUTE
		//                parser.MoveToAttribute("value");

		//                //ADD TO HASH TABLE
		//                m_Configtable[fieldName] = parser.Value;
		//            }
		//        }
		//    }

		//}

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
					m_SystemLogManager.addApplicationLog("Warning, measurement file was not found: " + fiMeasurementsToRunFile.FullName);
					m_SystemLogManager.addApplicationLog("Will use the default metrics");
					useDefaultMetrics = true;
				}
				else
				{
					try
					{
						using (var fsMeasurementsFile = new FileStream(fiMeasurementsToRunFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
						{
							var parser = new XmlTextReader(fsMeasurementsFile);

							//LOOP THROUGH ENTIRE XML FILE
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
				var metricNames = (from item in m_Fields where item != "instrument_id" && item != "random_id" && item != "scan_date" select item);

				lstMeasurementsToRun.Clear();
				lstMeasurementsToRun.AddRange(metricNames);

			}

			//RETURN OUR LIST
			return lstMeasurementsToRun;
		}

	}
}
