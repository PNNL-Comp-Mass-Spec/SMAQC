using System;
using System.IO;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Xml;
using System.Reflection;
using System.Collections;
using System.Data.SQLite;

namespace SMAQC
{
	class Smaqc
	{
		protected const bool USE_PHRP = true;

		protected const bool WIPE_TEMP_DATA_AT_START = true;
		protected const bool KEEP_TEMP_DATA_AT_END = false;

		//DECLARE VARIABLES
		public static DBWrapper m_DBWrapper;                                                              //CREATE DB INTERFACE OBJECT
		public static Aggregate m_Aggregate;                                                            //CREATE AGGREGATE OBJECT
		public static Measurement m_Measurement;                                                        //CREATE MEASUREMENT OBJECT
		public static Filter m_Filter;                                                                  //CREATE FILTER ENGINE OBJECT
		public static MeasurementEngine m_MeasurementEngine;                                            //CREATE MEASUREMENTENGINE OBJECT
		public static SystemLogManager m_SystemLogManager = new SystemLogManager();                     //SYSTEM LOG MANAGER OBJECT
		public static OutputFileManager m_OutputFileManager;                                            //OUTPUT MANAGER OBJECT

		public static Dictionary<string, string> m_Configtable = new Dictionary<string, string>();        //CONFIG OPTIONS

		public static Dictionary<string, string> m_Results = new Dictionary<string, string>();			//MEASUREMENT RESULTS

		//DECLARE VERSION, BUILD DATE, VALID FILE TABLES AND MEASUREMENT FIELDS
		private static string SMAQC_VERSION = "1.2";
		private static string SMAQC_BUILD_DATE = "June 26, 2013";

		// Define the filename suffixes
		private static string[] m_MasicFileNames = { "_scanstats", "_scanstatsex", "_sicstats" };

		// These filename suffixes will be deprecated once we switch to using PHRPReader
		private static string[] m_XTandemFileNames = { "_xt", "_xt_resulttoseqmap", "_xt_seqtoproteinmap" };

		private static string[] fields = { "instrument_id", "random_id", "scan_date", "C_1A", "C_1B", 
                                  "C_2A", "C_2B", "C_3A", "C_3B", "C_4A", "C_4B", "C_4C", "DS_1A", "DS_1B", "DS_2A", "DS_2B", 
                                  "DS_3A", "DS_3B", "IS_1A", "IS_1B", "IS_2", "IS_3A", "IS_3B", "IS_3C", "MS1_1", 
                                  "MS1_2A", "MS1_2B", "MS1_3A", "MS1_3B", "MS1_4A", "MS1_5A", "MS1_5B", "MS1_5C", 
                                  "MS1_5D", "MS2_1", "MS2_2", "MS2_3", "MS2_4A", "MS2_4B", "MS2_4C", "MS2_4D", "P_1A", 
                                  "P_1B", "P_2A", "P_2B", "P_2C", "P_3" };

		static void Main(string[] args)
		{
			//DECLARE VARIABLES
			string sInputFolderPath;
			string sMeasurementsFile;
			Random random = new Random();                                                           //TEMP RANDOM ID
			string outputfile = "";                                                                 //FILE TO SAVE OUTPUT TO [IF SPECIFIED]
			int random_id = random.Next();                                                          //GET THE RANDOM ID [.Next() REQUIRED FOR THIS]
			string instrument_id = "";                                                              //INIT TO -1 BY DEFAULT
			string dbFolderPath = "";
			bool bSuccess;

			List<string> lstMeasurementsToRun;

			bSuccess = ParseCommandLine(args, ref outputfile, ref instrument_id, ref dbFolderPath, out sInputFolderPath, out sMeasurementsFile);
			if (!bSuccess)
				NotifyError("Error parsing the command line arguments", 5);

			try
			{

				//CREATE APPLICATION LOG
				m_SystemLogManager.createApplicationLog();

				//PRINT NAME + VERSION
				m_SystemLogManager.addApplicationLog("SMAQC Version " + SMAQC_VERSION + " [BUILD DATE: " + SMAQC_BUILD_DATE + "]");

			}
			catch (Exception ex)
			{
				NotifyError("Error initializing log file: " + ex.Message, 6);
			}


			try
			{

				//LOAD CONFIG INTO HASH TABLE
				loadConfig();

				//FETCH LIST<STRING> OF ALL MEASUREMENT FUNCTIONS TO RUN
				lstMeasurementsToRun = LoadMeasurementInfoFile(sInputFolderPath, sMeasurementsFile);

				try
				{

					DBWrapper.eDBTypeConstants dbType = DBWrapper.eDBTypeConstants.SQLite;

					if (m_Configtable["dbtype"].Equals("MySQL", StringComparison.OrdinalIgnoreCase))
						dbType = SMAQC.DBWrapper.eDBTypeConstants.MySql;

					//CREATE CONNECTIONS
					m_DBWrapper = new DBWrapper(m_Configtable["dbhost"], m_Configtable["dbuser"],
										m_Configtable["dbpass"], m_Configtable["dbname"],
										dbType, dbFolderPath);

					m_DBWrapper.ShowQueryText = false;

					if (WIPE_TEMP_DATA_AT_START)
						m_DBWrapper.ClearTempTables();

					//DB OBJECT
					m_Aggregate = new Aggregate(sInputFolderPath);																	// AGGREGATE OBJECT
					m_Measurement = new Measurement(random_id, ref m_DBWrapper);															// MEASUREMENT LIST OBJECT
					m_MeasurementEngine = new MeasurementEngine(lstMeasurementsToRun, ref m_Measurement, ref m_SystemLogManager);      // MEASUREMENT ENGINE OBJECT
					m_Filter = new Filter(ref m_DBWrapper, instrument_id, random_id, ref m_SystemLogManager);                              // FILTER OBJECT
					m_OutputFileManager = new OutputFileManager(ref m_DBWrapper, SMAQC_VERSION, SMAQC_BUILD_DATE, fields);			// OUTPUTFILE MANAGER OBJECT

					try
					{
						ProcessDatasets(sInputFolderPath, outputfile, random_id, instrument_id, lstMeasurementsToRun);
					}
					catch (Exception ex)
					{
						NotifyError("Error finding datasets to process: " + ex.Message, 9);
					}

					try
					{
						//OUTPUT A SMAQC ANALYSIS COMPLETE MESSAGE [MENTOR REQUIREMENT]
						m_SystemLogManager.addApplicationLog("SMAQC analysis complete");

						//CLOSE THE APPLICATION LOG
						m_SystemLogManager.CloseLogFile();
					}
					catch (Exception ex)
					{
						NotifyError("Error closing the log file: " + ex.Message, 10);
					}

				}
				catch (Exception ex)
				{
					NotifyError("Error initializing database wrapper: " + ex.Message, 7);
				}

			}
			catch (Exception ex)
			{
				NotifyError("Error initializing loading configuration: " + ex.Message, 8);
			}


		}

		private static void NotifyError(string message)
		{
			NotifyError(message, 1);
		}

		private static void NotifyError(string message, int exitCode)
		{
			Console.WriteLine(message);
			System.Threading.Thread.Sleep(2000);
			Environment.Exit(exitCode);

		}
		private static void ProcessDatasets(string sInputFolderPath, string outputfile, int random_id, string instrument_id, List<string> lstMeasurementsToRun)
		{

			//FETCH FILE LISTING OF VALID FILES WE NEED TO PARSE
			m_SystemLogManager.addApplicationLog("Searching for Text Files...");

			//DETECT DATA SETS [SCAN + FIND OUT HOW MANY + WHICH ARE THEIR FILE PREFIXES [QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45 == Example]
			List<string> DatasetNames = m_Aggregate.DetectDatasets();                   //RETURN LIST OF DATASET NAMES
			int total_datasets = m_Aggregate.numberOfDataSets();                        //RETURN # OF DATA SETS IN FOLDER TO SCAN

			//IF WE DID NOT FIND ONE DATA SET
			if (DatasetNames.Count == 0)
			{
				m_SystemLogManager.addApplicationLog("Unable to find any datasets in {0}" + sInputFolderPath);
				m_SystemLogManager.addApplicationLog("Exiting...");
			}


			//DETERMINE RESULT_ID [USED FOR SCAN_ID]
			int result_id = determine_result_id();

			//LOOP THROUGH EACH DATA SET
			int datasetNumber = 0;
			foreach (string datasetName in DatasetNames)
			{
				try
				{
					//SET DATASET
					m_Aggregate.setDataset(datasetName);

					List<string> MasicFileList = m_Aggregate.getMasicFileImportList(datasetName, "*.txt");

					List<string> XTandemFileList;
					if (USE_PHRP)
						XTandemFileList = new List<string>();
					else
						XTandemFileList = m_Aggregate.getXTandemFileImportList(datasetName, "*.txt");

					//ENSURE FILES HAVE BEEN FOUND OR EXIT
					if (MasicFileList.Count < 3)
					{
						//NOT ALL MASIC FILES HAVE BEEN FOUND! ... EXIT AS CANNOT RUN METRICS WITH INCOMPLETE DATASET
						m_SystemLogManager.addApplicationLog("Required MASIC data files not found in " + sInputFolderPath);

						bool bScanStatsExMissing = false;

						// Find the missing files
						foreach (string sSuffix in m_MasicFileNames)
						{
							bool bMatchFound = false;

							foreach (string sFilePath in MasicFileList)
							{
								string sFileName = System.IO.Path.GetFileNameWithoutExtension(sFilePath);
								if (sFileName.EndsWith(sSuffix, true, System.Globalization.CultureInfo.CurrentCulture))
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

							NotifyError("", 11);
						}
					}

					if (!USE_PHRP && XTandemFileList.Count < 3)
					{

						m_SystemLogManager.addApplicationLog("Required X!Tandem data files not found in " + sInputFolderPath);

						// Find the missing files
						foreach (string sSuffix in m_XTandemFileNames)
						{
							bool bMatchFound = false;

							foreach (string sFilePath in XTandemFileList)
							{
								string sFileName = System.IO.Path.GetFileNameWithoutExtension(sFilePath);
								if (sFileName.EndsWith(sSuffix, true, System.Globalization.CultureInfo.CurrentCulture))
								{
									bMatchFound = true;
									break;
								}
							}

							if (!bMatchFound)
							{
								m_SystemLogManager.addApplicationLog("  Missing file: " + datasetName + sSuffix + ".txt");
							}

						}

						m_SystemLogManager.addApplicationLog("Exiting...");

						//CLOSE THE APPLICATION LOG
						m_SystemLogManager.CloseLogFile();

						NotifyError("", 11);
					}


					//LOOP THROUGH EACH FILE IN FileList, CREATE TEMP FILE, REWRITE TO ADD ADDITIONAL COLUMNS (AND ONLY HAVE THE DESIRED COLUMNS) AND BULK INSERT INTO DB
					m_SystemLogManager.addApplicationLog("Parsing and Inserting Data into DB Temp Tables");

					if (USE_PHRP)
					{
						// Load data using PHRP
						m_Filter.LoadFilesUsingPHRP(sInputFolderPath, datasetName);
					}
					else
					{
						// Load X!Tandem data
						m_Filter.LoadFilesAndInsertIntoDB(XTandemFileList, m_XTandemFileNames, datasetName);
					}

					// Load the MASIC data
					m_Filter.LoadFilesAndInsertIntoDB(MasicFileList, m_MasicFileNames, datasetName);


					//AT THIS POINT OUR DB IS FULL OF ALL DATA FROM OUR TEXT FILES
					m_SystemLogManager.addApplicationLog("Now running Measurements on " + datasetName);

					//RUN MEASUREMENT ENGINE
					m_MeasurementEngine.UsingPHRP = USE_PHRP;

					m_Results = m_MeasurementEngine.run();

					//ADD TO SCAN RESULTS
					m_SystemLogManager.addApplicationLog("Saving Scan Results");
					add_scan_results(instrument_id, random_id, result_id, lstMeasurementsToRun);

					//CLEAR TEMP TABLES
					if (!KEEP_TEMP_DATA_AT_END)
					{
						m_DBWrapper.ClearTempTables(random_id);
					}

					//IF OPTIONAL OUTPUTFILE IS PASSED WE WRITE TO FILE
					if (!String.IsNullOrEmpty(outputfile))
					{
						//WRITE TO FILE
						m_OutputFileManager.SaveData(datasetName, outputfile, Convert.ToInt32(m_Configtable["scan_id"]), datasetNumber);

						//SAVE TO LOG
						m_SystemLogManager.addApplicationLog("Scan output has been saved to " + outputfile);
					}
					else
					{
						//DONE SO PRINT END MSG
						m_SystemLogManager.addApplicationLog("Scan result saved to SQLite DB (Scan ID=" + m_Configtable["scan_id"] + ")");
					}

					m_Results.Clear();

				}
				catch (Exception ex)
				{
					m_SystemLogManager.addApplicationLog("Error processing dataset " + datasetName + ": " + ex.Message);
					NotifyError("", 5);
				}

				datasetNumber++;
			}
		}

		private static bool ParseCommandLine(string[] args, ref string outputfile, ref string instrument_id, ref string dbFolderPath, out string path_to_scan_files, out string sMeasurementsFile)
		{

			//IF NO ARGUMENTS PASSED | INCORRECT AMOUNT
			//-i 3 -d "C:\Users\xion\Documents\Visual Studio 2010\Projects\SMAQC\SMAQC" -m measurementsToRun.xml -o outputfile.txt == 8
			//-i 3 -d "C:\Users\xion\Documents\Visual Studio 2010\Projects\SMAQC\SMAQC" -m measurementsToRun.xml == 6
			if (args.Length != 6 && args.Length != 8 && args.Length != 10)
			{
				Console.WriteLine("Usage: SMAQC.exe [Required Options] [Optional]\n");
				Console.WriteLine("[Required Options]");

				Console.WriteLine("\t-i [instrument_id]");
				Console.WriteLine("\t\tInstrument id of device (integer)");

				Console.WriteLine("\t-d [path]");
				Console.WriteLine("\t\tPath to folder with dataset(s); use quotes if spaces");

				Console.WriteLine("\t-m [measurements file]");
				Console.WriteLine("\t\tPath to XML file containing measurements to be run; use quotes if spaces");

				Console.WriteLine("\n[Optional]");
				Console.WriteLine("\t-o filename.txt");
				Console.WriteLine("\t\tPath to file to save scan results to; use quotes if spaces");

				Console.WriteLine("\t-db [path]");
				Console.WriteLine("\t\tPath to folder where SQLite database should be created (default is same folder as .Exe)");

				System.Threading.Thread.Sleep(1500);

				NotifyError("", 1);
			}

			//SET VARIABLES BASED ON ARGUMENTS
			instrument_id = args[1];
			path_to_scan_files = args[3];
			sMeasurementsFile = args[5];

			if (String.IsNullOrEmpty(instrument_id))
			{
				NotifyError("Instrument_ID parameter is empty; unable to continue", 2);
			}

			if (String.IsNullOrEmpty(path_to_scan_files))
			{
				NotifyError("Path to datasets is empty; unable to continue", 3);
			}

			if (String.IsNullOrEmpty(sMeasurementsFile))
			{
				NotifyError("Measurements file path is empty; unable to continue", 4);
			}

			if (args.Length >= 8)
			{
				//OUTPUT FILENAME
				outputfile = args[7];
			}

			if (args.Length >= 10)
			{
				//OUTPUT FILENAME
				dbFolderPath = args[9];
			}
			else
			{
				System.IO.FileInfo diAppFile = new System.IO.FileInfo(GetAppPath());
				dbFolderPath = diAppFile.DirectoryName;
			}

			Console.WriteLine();
			Console.WriteLine("Instrument ID: ".PadRight(20) + instrument_id);
			Console.WriteLine("Path to datasets: ".PadRight(20) + path_to_scan_files);
			Console.WriteLine("Measurements file: ".PadRight(20) + sMeasurementsFile);
			if (!String.IsNullOrEmpty(outputfile))
				Console.WriteLine("Text results file: ".PadRight(20) + outputfile);
			Console.WriteLine("SQLite DB folder: ".PadRight(20) + dbFolderPath);
			Console.WriteLine();

			return true;

		}

		//FUNCTION INSERTS INTO SCAN_RESULTS
		static void add_scan_results(string instrument_id, int random_id, int result_id, List<string> lstMeasurementsToRun)
		{
			
			string scan_results_query = "";

			//REMOVE SCAN ID [NEEDED AS IF USING MULTIPLE DATASETS PROGRAM WILL CRASH DUE TO DUPLICATE KEY]
			m_Configtable.Remove("scan_id");

			//BUILD SCAN RESULTS QUERY
			scan_results_query = build_scan_results_query(instrument_id, random_id, result_id, lstMeasurementsToRun);

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
			string scan_results_query = "";

			//HEAD OF RESULTS STRING QUERY
			scan_results_query = "INSERT INTO scan_results ( scan_id, instrument_id, random_id, scan_date";

			//BUILD METRICS FIELDS [, `C_1A` ...]
			foreach (string item in lstMeasurementsToRun)
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
			foreach (string item in lstMeasurementsToRun)
			{
				scan_results_query += ", '" + m_Results[item] + "'";
				//Console.WriteLine("KEY=[{0}] -- VALUE=[{1}]", key, resultstable[measurementsDict[key]]);
			}

			//BUILD END
			scan_results_query += ");";

			//RETURN
			return scan_results_query;
		}

		//THIS FUNCTION CHECKS OUR DB TO SEE WHAT RESULT ID IS NEXT AND USES IT FOR OUR SCAN RESULT ... SO MULTIPLE DATASETS WILL HAVE A SINGLE SCAN_RESULT INSTEAD OF MULTIPLE
		static int determine_result_id()
		{
			//DECLARE VARIABLES
			Dictionary<string, string> dctMostRecentEntry = new Dictionary<string, string>();
			int result_id = 0;

			//SET QUERY
			m_DBWrapper.setQuery("SELECT Max(result_id) AS result_id FROM scan_results;");

			//INIT READER
			m_DBWrapper.initReader();

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "result_id" };

			//READ LINE
			m_DBWrapper.readSingleLine(fields, ref dctMostRecentEntry);

			//DETERMINE NEXT RESULT_ID
			if (int.TryParse(dctMostRecentEntry["result_id"], out result_id))
				result_id++;
			else
				result_id = 1;			

			return result_id;
		}

		//LOAD CONFIG.XML VARIABLES INTO HASH TABLE
		static void loadConfig()
		{
			//DECLARE VARIABLES
			string[] xml_variables = { "dbhost", "dbuser", "dbpass", "dbname", "dbtype" };
			string attribute = "value";

			string configFilePath = System.IO.Path.GetDirectoryName(GetAppPath());
			configFilePath = System.IO.Path.Combine(configFilePath, "config.xml");

			FileStream configFile = null;

			//OPEN XML DOC + INIT PARSER
			List<string> measurements = new List<string>();

			try
			{
				configFile = File.Open(configFilePath, FileMode.Open);
			}
			catch (FileNotFoundException ex)
			{
				NotifyError("Error:: Could not open config file (" + configFilePath + "): " + ex.Message, 12);
			}

			XmlTextReader parser = new XmlTextReader(configFile);

			//LOOP THROUGH EACH VARIABLE IN XML CONFIG
			for (int i = 0; i < xml_variables.Length; i++)
			{
				//FIND VARIABLE IF POSSIBLE
				Boolean found = parser.ReadToFollowing(xml_variables[i]);

				//IF VARIABLE IS IN XML FILE
				if (found)
				{
					//READ ATTRIBUTE
					parser.MoveToAttribute(attribute);

					//ADD TO HASH TABLE
					m_Configtable.Add(xml_variables[i], parser.Value);
				}
			}

			//CLOSE
			parser.Close();
		}

		/// <summary>
		/// Determine the names of the measurements that should be run
		/// </summary>
		/// <param name="path_to_scan_files"></param>
		/// <param name="measurementsToRunFile"></param>
		/// <returns></returns>
		static List<string> LoadMeasurementInfoFile(string path_to_scan_files, string measurementsToRunFile)
		{
			FileStream fsMeasurementsFile = null;
			List<string> lstMeasurementsToRun = new List<string>();
			System.IO.FileInfo fiMeasurementsToRunFile = new System.IO.FileInfo(measurementsToRunFile);

			if (!fiMeasurementsToRunFile.Exists)
			{
				NotifyError("Error:: Measurements file not found at : " + measurementsToRunFile, 13);
			}

			try
			{
				fsMeasurementsFile = File.Open(fiMeasurementsToRunFile.FullName, FileMode.Open);
			}
			catch (FileNotFoundException ex)
			{
				NotifyError("Error:: Could not open measurements file (" + measurementsToRunFile + "): " + ex.Message, 13);
			}

			XmlTextReader parser = new XmlTextReader(fsMeasurementsFile);

			//LOOP THROUGH ENTIRE XML FILE
			while (parser.ReadToFollowing("measurement"))
			{
				parser.MoveToAttribute("name");
				lstMeasurementsToRun.Add(parser.Value);
			}

			//CLOSE
			parser.Close();

			//RETURN OUR LIST
			return lstMeasurementsToRun;
		}

		static protected string GetAppPath()
		{
			return System.Reflection.Assembly.GetExecutingAssembly().Location;
		}

	}
}
