using System;
using System.Windows;
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
		//DECLARE VARIABLES
		public static DBWrapper DBWrapper;                                                              //CREATE DB INTERFACE OBJECT
		public static Aggregate m_Aggregate;                                                            //CREATE AGGREGATE OBJECT
		public static Measurement m_Measurement;                                                        //CREATE MEASUREMENT OBJECT
		public static Filter m_Filter;                                                                  //CREATE FILTER ENGINE OBJECT
		public static MeasurementEngine m_MeasurementEngine;                                            //CREATE MEASUREMENTENGINE OBJECT
		public static SystemLogManager m_SystemLogManager = new SystemLogManager();                     //SYSTEM LOG MANAGER OBJECT
		public static OutputFileManager m_OutputFileManager;                                            //OUTPUT MANAGER OBJECT

		public static Hashtable configtable = new Hashtable();                                          //CONFIG HASH TABLE
		public static Hashtable resultstable = new Hashtable();                                         //MEASUREMENT RESULTS HASH TABLE
		public static Hashtable measurementsDict = new Hashtable();                                     //STORE MEASUREMENTS WE ARE RUNNING HERE
		private static string appDirectoryPath = System.IO.Path.GetDirectoryName(GetAppPath());         //GET PATH TO SMAQC.exe [USEFUL IF NOT RUNNING IN SAME DIRECTORY]

		//DECLARE VERSION, BUILD DATE, VALID FILE TABLES AND MEASUREMENT FIELDS
		private static String SMAQC_VERSION = "1.08";
		private static String SMAQC_BUILD_DATE = "September 27, 2012";
		private static String[] valid_file_tables = { "_scanstats", "_scanstatsex", "_sicstats", "_xt", 
                                                       "_xt_resulttoseqmap", "_xt_seqtoproteinmap" };   //VALID FILE / TABLES
		private static String[] fields = { "instrument_id", "random_id", "scan_date", "C_1A", "C_1B", 
                                  "C_2A", "C_2B", "C_3A", "C_3B", "C_4A", "C_4B", "C_4C", "DS_1A", "DS_1B", "DS_2A", "DS_2B", 
                                  "DS_3A", "DS_3B", "IS_1A", "IS_1B", "IS_2", "IS_3A", "IS_3B", "IS_3C", "MS1_1", 
                                  "MS1_2A", "MS1_2B", "MS1_3A", "MS1_3B", "MS1_4A", "MS1_5A", "MS1_5B", "MS1_5C", 
                                  "MS1_5D", "MS2_1", "MS2_2", "MS2_3", "MS2_4A", "MS2_4B", "MS2_4C", "MS2_4D", "P_1A", 
                                  "P_1B", "P_2A", "P_2B", "P_2C", "P_3" };

		static void Main(string[] args)
		{
			//DECLARE VARIABLES
			String path_to_scan_files;
			String measurementsFile;
			Random random = new Random();                                                           //TEMP RANDOM ID
			String outputfile = "";                                                                      //FILE TO SAVE OUTPUT TO [IF SPECIFIED]
			int r_id = random.Next();                                                               //GET THE RANDOM ID [.Next() REQUIRED FOR THIS]
			String instrument_id = "";                                                              //INIT TO -1 BY DEFAULT
			String dbFolderPath = "";
			bool bSuccess;

			bSuccess = ParseCommandLine(args, ref outputfile, ref instrument_id, ref dbFolderPath, out path_to_scan_files, out measurementsFile);
			if (!bSuccess)
				Environment.Exit(5);

			try
			{

				//CREATE APPLICATION LOG
				m_SystemLogManager.createApplicationLog();

				//PRINT NAME + VERSION
				m_SystemLogManager.addApplicationLog("SMAQC Version " + SMAQC_VERSION + " [BUILD DATE: " + SMAQC_BUILD_DATE + "]");

			}
			catch (Exception ex)
			{
				Console.WriteLine("Error initializing log file: " + ex.Message);
				Environment.Exit(6);
			}


			try
			{

				//LOAD CONFIG INTO HASH TABLE
				loadConfig();

				//BUILD MEASUREMENT LIST
				loadMeasurements(measurementsFile);

				//FETCH LIST<STRING> OF ALL MEASUREMENT FUNCTIONS TO RUN
				List<string> measurements_list = measurementsToRun(path_to_scan_files, measurementsFile);

				try
				{

					//CREATE CONNECTIONS
					DBWrapper = new DBWrapper(configtable["dbhost"].ToString(), configtable["dbuser"].ToString(),
										configtable["dbpass"].ToString(), configtable["dbname"].ToString(),
										configtable["dbtype"].ToString(), dbFolderPath);

					DBWrapper.ShowQueryText = false;

					//DB OBJECT
					m_Aggregate = new Aggregate(path_to_scan_files);                                                        //AGGREGATE OBJECT
					m_Measurement = new Measurement(r_id, ref DBWrapper);                                                   //MEASUREMENT LIST OBJECT
					m_MeasurementEngine = new MeasurementEngine(measurements_list, ref m_Measurement, ref m_SystemLogManager);                      //MEASUREMENT ENGINE OBJECT
					m_Filter = new Filter(ref DBWrapper, instrument_id, r_id, ref m_SystemLogManager);                                              //FILTER OBJECT
					m_OutputFileManager = new OutputFileManager(ref DBWrapper, SMAQC_VERSION, SMAQC_BUILD_DATE, fields);    //OUTPUTFILE MANAGER OBJECT

				}
				catch (Exception ex)
				{
					Console.WriteLine("Error initializing database wrapper: " + ex.Message);
					Environment.Exit(7);
				}

			}
			catch (Exception ex)
			{
				Console.WriteLine("Error initializing loading configuration: " + ex.Message);
				Environment.Exit(8);
			}


			try
			{
				ProcessDatasets(path_to_scan_files, outputfile, r_id, instrument_id);
			}
			catch (Exception ex)
			{
				Console.WriteLine("Error finding datasets to process: " + ex.Message);
				Environment.Exit(9);
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
				Console.WriteLine("Error closing the log file: " + ex.Message);
				Environment.Exit(10);
			}

		}

		private static void ProcessDatasets(String path_to_scan_files, String outputfile, int r_id, String instrument_id)
		{

			//FETCH FILE LISTING OF VALID FILES WE NEED TO PARSE
			m_SystemLogManager.addApplicationLog("Searching for Text Files...");

			//DETECT DATA SETS [SCAN + FIND OUT HOW MANY + WHICH ARE THEIR FILE PREFIXES [QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45 == Example]
			List<String> DataPrefix = m_Aggregate.DetectDatasets("*.txt");              //RETURN LIST OF DATA PREFIXES
			int total_datasets = m_Aggregate.numberOfDataSets();                        //RETURN # OF DATA SETS IN FOLDER TO SCAN

			//IF WE DID NOT FIND ONE DATA SET
			if (DataPrefix.Count == 0)
			{
				m_SystemLogManager.addApplicationLog("Unable to find any datasets in {0}" + path_to_scan_files);
				m_SystemLogManager.addApplicationLog("Exiting...");
			}


			//DETERMINE RESULT_ID [USED FOR SCAN_ID]
			int result_id = determine_result_id();

			//LOOP THROUGH EACH DATA SET
			for (int i = 0; i < DataPrefix.Count; i++)
			{
				try
				{
					//SET DATASET
					m_Aggregate.setDataset(DataPrefix[i]);

					List<String> FileList = m_Aggregate.getFileImportList(DataPrefix[i], "*.txt");

					//ENSURE FILES HAVE BEEN FOUND OR EXIT
					if (FileList.Count < 6)
					{
						//NOT ALL FILES HAVE BEEN FOUND! ... EXIT AS CANNOT RUN METRICS WITH INCOMPLETE DATASET
						m_SystemLogManager.addApplicationLog("The 6 required data files not found in " + path_to_scan_files);
						m_SystemLogManager.addApplicationLog("Exiting...");

						//CLOSE THE APPLICATION LOG
						m_SystemLogManager.CloseLogFile();

						Environment.Exit(11);
					}

					//LOOP THROUGH EACH FILE IN FileList, CREATE TEMP FILE, REWRITE TO USE ',' AND BULK INSERT INTO DB
					m_SystemLogManager.addApplicationLog("Parsing and Inserting Data into DB Temp Tables");
					m_Filter.LoadFilesAndInsertIntoDB(FileList, valid_file_tables, DataPrefix[i]);

					//AT THIS POINT OUR DB IS FULL OF ALL DATA FROM OUR TEXT FILES
					m_SystemLogManager.addApplicationLog("Now running Measurements on " + DataPrefix[i]);

					//RUN MEASUREMENT ENGINE
					resultstable = m_MeasurementEngine.run();

					//ADD TO SCAN RESULTS
					m_SystemLogManager.addApplicationLog("Saving Scan Results");
					add_scan_results(instrument_id, r_id, result_id);

					//CLEAR TEMP TABLES
					DBWrapper.clearTempTables(r_id);

					//IF OPTIONAL OUTPUTFILE IS PASSED WE WRITE TO FILE
					if (!string.IsNullOrEmpty(outputfile))
					{
						//WRITE TO FILE
						m_OutputFileManager.SaveData(DataPrefix[i], outputfile, Convert.ToInt32(configtable["scan_id"]), i);

						//SAVE TO LOG
						m_SystemLogManager.addApplicationLog("Scan output has been saved to " + outputfile);
					}
					else
					{
						//DONE SO PRINT END MSG
						m_SystemLogManager.addApplicationLog("Scan result saved to SQLite DB (Scan ID=" + configtable["scan_id"] + ")");
					}

					resultstable.Clear();

				}
				catch (Exception ex)
				{
					m_SystemLogManager.addApplicationLog("Error processing dataset " + DataPrefix[i] + ": " + ex.Message);
					Environment.Exit(5);
				}

			}
		}

		private static bool ParseCommandLine(string[] args, ref String outputfile, ref String instrument_id, ref String dbFolderPath, out String path_to_scan_files, out String measurementsFile)
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

				Environment.Exit(1);
			}

			//SET VARIABLES BASED ON ARGUMENTS
			instrument_id = args[1];
			path_to_scan_files = args[3];
			measurementsFile = args[5];

			if (string.IsNullOrEmpty(instrument_id))
			{
				Console.WriteLine("Instrument_ID parameter is empty; unable to continue");
				Environment.Exit(2);
			}

			if (string.IsNullOrEmpty(path_to_scan_files))
			{
				Console.WriteLine("Path to datasets is empty; unable to continue");
				Environment.Exit(3);
			}

			if (string.IsNullOrEmpty(measurementsFile))
			{
				Console.WriteLine("Measurements file path is empty; unable to continue");
				Environment.Exit(4);
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
			Console.WriteLine("Measurements file: ".PadRight(20) + measurementsFile);
			if (!string.IsNullOrEmpty(outputfile))
				Console.WriteLine("Text results file: ".PadRight(20) + outputfile);
			Console.WriteLine("SQLite DB folder: ".PadRight(20) + dbFolderPath);
			Console.WriteLine();

			return true;

		}

		//FUNCTION INSERTS INTO SCAN_RESULTS
		static void add_scan_results(String instrument_id, int r_id, int result_id)
		{
			//DECLARE VARIABLE
			String scan_results_query = "";

			//REMOVE SCAN ID [NEEDED AS IF USING MULTIPLE DATASETS PROGRAM WILL CRASH DUE TO DUPLICATE HASH KEY]
			configtable.Remove("scan_id");

			//BUILD SCAN RESULTS QUERY
			scan_results_query = build_scan_results_query(instrument_id, r_id, result_id);

			//SET QUERY TO STORE DATA TO SCAN_STATS
			DBWrapper.setQuery(scan_results_query);

			//EXECUTE QUERY
			DBWrapper.QueryNonQuery();

			//SET SCAN_ID
			configtable.Add("scan_id", result_id);
		}

		//BUILD SCAN_RESULTS INSERT QUERY
		static String build_scan_results_query(String instrument_id, int r_id, int result_id)
		{
			//DECLARE VARIABLES
			String scan_results_query = "";

			//HEAD OF RESULTS STRING QUERY
			scan_results_query = "INSERT INTO scan_results ( `scan_id`, `instrument_id`, `random_id`, `scan_date`";

			//BUILD METRICS FIELDS [, `C_1A` ...]
			foreach (string key in measurementsDict.Keys)
			{
				scan_results_query += ", `" + measurementsDict[key] + "`";
			}
			scan_results_query += ") VALUES (";

			//BUILD VALUES FOR SCAN_ID, INSTRUMENT_ID, RANDOM_ID
			scan_results_query += "'" + result_id + "',";
			scan_results_query += "'" + instrument_id + "',";
			scan_results_query += "'" + r_id + "',";

			//BUILD DATE STRINGS
			scan_results_query += DBWrapper.getDateTime();// +",";

			//BUILD METRICS VALUE LIST ["'" + resultstable["C_1A"] + "',"]
			foreach (string key in measurementsDict.Keys)
			{
				scan_results_query += ", '" + resultstable[measurementsDict[key]] + "'";
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
			Hashtable temp_hashtable = new Hashtable();
			int result_id = 0;

			//SET QUERY
			DBWrapper.setQuery("SELECT result_id FROM `scan_results` ORDER BY result_id DESC;");
			//DBWrapper.setQuery("SELECT Auto_increment as result_id FROM information_schema.tables WHERE table_name='scan_results';"); //NOT WORK FOR SQLITE

			//INIT READER
			DBWrapper.initReader();

			//DECLARE FIELDS TO READ FROM
			String[] fields = { "result_id" };

			//READ LINE + STORE RESULT_ID IN OUR temp_hashtable [HASHTABLE]
			DBWrapper.readSingleLine(fields, ref temp_hashtable);

			//SET RESULT_ID
			result_id = Convert.ToInt32(temp_hashtable["result_id"]) + 1;
			//result_id = Convert.ToInt32(temp_hashtable["result_id"]);//NOT WORK FOR SQLITE

			//CLEAR HASH TABLE
			temp_hashtable.Clear();

			//RETURN + INC BY ONE [AS THAT WOULD BE THE NEXT VALUE]
			return result_id;
		}

		//LOAD CONFIG.XML VARIABLES INTO HASH TABLE
		static void loadConfig()
		{
			//DECLARE VARIABLES
			String[] xml_variables = { "dbhost", "dbuser", "dbpass", "dbname", "dbtype" };
			String attribute = "value";
			String configFilePath = System.IO.Path.Combine(appDirectoryPath, "config.xml");
			FileStream configFile = null;

			//OPEN XML DOC + INIT PARSER
			List<String> measurements = new List<String>();

			try
			{
				configFile = File.Open(configFilePath, FileMode.Open);
			}
			catch (FileNotFoundException ex)
			{
				Console.WriteLine("Error:: Could not open config file (" + configFilePath + "): " + ex.Message);
				Environment.Exit(12);
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
					configtable.Add(xml_variables[i], parser.Value);
				}
			}

			//CLOSE
			parser.Close();
		}

		//THIS FUNCTION READS OUR MEASUREMENTS FILE ... THEN ADDS TO A DICTIONARY TO SO KNOW EACH MEASUREMENT THAT IS BEING RUN
		//USED FOR BUILDING SQL QUERIES TO SAVE RESULTS
		static void loadMeasurements(string measurementsFilePath)
		{
			//DECLARE VARIABLES
			FileStream measurementsFile = null;
			System.IO.FileInfo fiMeasurementsFile = new System.IO.FileInfo(measurementsFilePath);

			//OPEN XML DOC + INIT PARSER
			List<String> measurements = new List<String>();

			try
			{
				measurementsFile = File.Open(fiMeasurementsFile.FullName, FileMode.Open);
			}
			catch (FileNotFoundException ex)
			{
				Console.WriteLine("Error:: Could not open measurements file (" + measurementsFilePath + "): " + ex.Message);
				Environment.Exit(13);
			}

			XmlTextReader parser = new XmlTextReader(measurementsFile);

			//LOOP THROUGH ENTIRE XML FILE
			while (parser.ReadToFollowing("measurement"))
			{
				parser.MoveToAttribute("name");
				measurementsDict.Add(parser.Value, parser.Value);
			}

			//CLOSE
			parser.Close();
		}

		//THIS IS OUR XML READER. TAKES TWO ARGUEMENTS, PATH TO DIR WITH FILES (NO TRAILING /) AND THE MEASUREMENTS FILE
		//IT THEN RETURNS A LIST<STRING> OF ALL OUR FUNCTION NAMES TO RUN
		static List<string> measurementsToRun(String path_to_scan_files, String measurementsToRunFile)
		{
			List<String> measurements = new List<String>();
			FileStream configFile = null;                   //SET TO NULL SO VS NOT DETECT ERROR

			//TRY TO OPEN FILE
			try
			{
				configFile = File.Open(measurementsToRunFile, FileMode.Open);
			}
			catch (FileNotFoundException ex)
			{
				Console.WriteLine("Could not find file {0}!", ex.Message);
				Environment.Exit(14);
			}
			XmlTextReader parser = new XmlTextReader(configFile);

			//LOOP THROUGH ENTIRE XML FILE
			while (parser.ReadToFollowing("measurement"))
			{
				parser.MoveToAttribute("name");
				measurements.Add(parser.Value);
			}

			//CLOSE
			parser.Close();

			//RETURN OUR LIST
			return measurements;
		}

		static protected string GetAppPath()
		{
			return System.Reflection.Assembly.GetExecutingAssembly().Location;
		}

	}
}
