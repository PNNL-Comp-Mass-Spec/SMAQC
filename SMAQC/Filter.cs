using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace SMAQC
{
	class Filter
	{
		//DECLARE VARIABLES
		public DBWrapper mDBWrapper;                                                                //CREATE DB INTERFACE OBJECT
		public string instrument_id;                                                                //INSTRUMENT ID
		public int random_id;                                                                       //RANDOM ID
		public DataFileFormatter DFF = new DataFileFormatter();                                     //DFF OBJECT
		SystemLogManager m_SystemLogManager;

		//CONSTRUCTOR
		public Filter(ref DBWrapper DBInterface, string instrument_id, int random_id, ref SystemLogManager systemLogManager)
		{
			this.mDBWrapper = DBInterface;
			this.instrument_id = instrument_id;
			this.random_id = random_id;
			this.m_SystemLogManager = systemLogManager;

			// Attach the event handler
			this.mDBWrapper.ErrorEvent += new DBWrapper.DBErrorEventHandler(DBWrapper_ErrorEvent);
		}

		//DESTRUCTOR
		~Filter()
		{
		}

		//THIS FUNCTION RETURNS WHETHER OR NOT WE ARE CURRENTLY WORKING WITH _SCANSTATSEX.TXT
		public Boolean ScanStatsExBugFixer(string file_to_load)
		{
			int value = file_to_load.IndexOf("_ScanStatsEx.txt", StringComparison.OrdinalIgnoreCase);

			//IF FOUND RETURN TRUE
			if (value >= 0)
			{
				return true;
			}

			//ELSE RETURN FALSE
			return false;
		}

		//CREATE BULK INSERT COMPATIBLE FILE
		public void parse_and_filter(string temp_file, string file_to_load, DBWrapper.eDBTypeConstants dbType)
		{
			//DECLARE VARIABLES
			string line;
			int line_num = 0;
			string query_info = "";

			char newDelimiter;

			if (dbType == DBWrapper.eDBTypeConstants.MySql)
				newDelimiter = ',';
			else
				newDelimiter = '\t';

			//OPEN TEMP srInFile
			System.IO.StreamWriter swOutFile = new System.IO.StreamWriter(temp_file);

			//Console.WriteLine("WRITE TO: {0} ... LOAD FROM: {1}", temp_file, file_to_load);

			StreamReader srInFile = new StreamReader(file_to_load);
			while ((line = srInFile.ReadLine()) != null)
			{
				//NEW LINE SO CLEAR QUERY_INFO
				query_info = "";

				//SPLIT GIVEN DATA FILES BY TAB
				char[] delimiters = new char[] { '\t' };

				//DO SPLIT OPERATION
				string[] parts = line.Split(delimiters, StringSplitOptions.None);

				//ADD INSTRUMENT ID + RANDOM ID
				if (line_num == 0)
				{
					query_info += "instrument_id" + newDelimiter + "random_id" + newDelimiter;
				}
				else
				{
					query_info += instrument_id + newDelimiter + random_id + newDelimiter;
				}

				//LOOP THROUGH ALL FIELDS (FORMATING CORRECTLY ALSO)
				for (int i = 0; i < parts.Length; i++)
				{

					if (parts[i].Equals("[PAD]"))
					{
						query_info += newDelimiter;
					}
					else
					{
						//HERE WE SEE OUR CONTENT TO BE INSERTED
						query_info += parts[i].Replace(newDelimiter, ';') + newDelimiter;
					}

					//IF AT END ... REMOVE + APPEND
					if (i == (parts.Length - 1))
					{
						//REMOVE END "," CHARACTER
						query_info = query_info.Substring(0, query_info.Length - 1);

						//ADD \r\n
						query_info += "\r\n";
					}
				}

				//WRITE RECORD LINE TO srInFile
				swOutFile.Write(query_info);

				//INCREMENT OUR LINE #
				line_num++;
			}

			//CLOSE THE FILE HANDLES
			swOutFile.Close();
			srInFile.Close();
		}

		//THIS FUNCTION:
		//1. LOOPS THROUGH A VALID FILE LIST
		//2. Calls another function that loads that file and rewrites the \tab as ',' separated.
		//3. From the filename, determines the correct table to insert into, appends temp
		//4. Calls our bulk insert function
		public void LoadFilesAndInsertIntoDB(List<string> FileList, string[] valid_file_tables, string dataset)
		{

			//LOOP THROUGH EACH FILE
			for (int i = 0; i < FileList.Count; i++)
			{
				//STORES THE FULL PATH TO FILE + TEMP FILE NAME
				string file_info = String.Copy(FileList[i]);							//FILENAME WE WANT TO LOAD INTO DB
				string temp_file = System.IO.Path.GetTempFileName();	//WRITE TO THIS FILE [TEMP FILE]
				string query_table = "temp";							//USED AS PREFIX PORTION OF TABLE

				//DETERMINE IF WE HAVE A TABLE TO INSERT INTO DEPENDING ON OUR INPUT FILENAME
				int j = return_file_table_position(file_info, valid_file_tables);

				//IF VALID INSERT TABLE
				if (j >= 0)
				{

					//DOES THIS FILE NEED TO BE REFORMATED [VARIABLE COLUMN SUPPORT]
					if (DFF.handleFile(file_info, dataset))
					{
						//YES

						//REBUILD [SAVE TO DFF.TempFilePath BY DEFAULT]
						//DFF.handleRebuild(FileList[i]);

						//SET FILE_INFO TO OUR REBUILT FILE NOW
						file_info = DFF.TempFilePath;
					}

					// PARSE + FORMAT FILE CORRECTLY FOR BULK INSERT QUERIES
					// Will add columns instrument_id and random_id
					// Will change the column delimiter to a comma only if using MySql
					parse_and_filter(temp_file, file_info, mDBWrapper.DBType);


					//WE NOW HAVE A ACCESS TO valid_file_tables[j] which starts with the prefix '_'
					//APPEND temp [DB PREFIX] to this.
					query_table += valid_file_tables[j];
					Console.WriteLine("Populating Table {0}", query_table);

					//INSERT INTO DB
					mDBWrapper.BulkInsert(query_table, temp_file);
				}
				else
				{
					//NOT A VALID .TXT FILE FROM OUR LIST!
					Console.WriteLine("ERROR, unrecognized file " + FileList[i]);
				}
				//DELETE TEMP FILE
				File.Delete(temp_file);
			}

		}

		public bool LoadFilesUsingPHRP(string sInputFolderPath, string sDataset)
		{

			// Look for a valid input file
			string sInputFilePath = PHRPReader.clsPHRPReader.AutoDetermineBestInputFile(sInputFolderPath, sDataset);

			if (string.IsNullOrEmpty(sInputFilePath))
			{
				throw new System.IO.FileNotFoundException("Valid input file not found for dataset " + sDataset + " in folder " + sInputFolderPath);
			}

			try
			{
				bool blnLoadModsAndSeqInfo = true;
				bool blnLoadMSGFResults = true;
				bool blnLoadScanStats = false;

				PHRPReader.clsPHRPReader oPHRPReader;
				oPHRPReader = new PHRPReader.clsPHRPReader(sInputFilePath, PHRPReader.clsPHRPReader.ePeptideHitResultType.Unknown, blnLoadModsAndSeqInfo, blnLoadMSGFResults, blnLoadScanStats);
				oPHRPReader.EchoMessagesToConsole = false;
				oPHRPReader.SkipDuplicatePSMs = true;

				// Attach the error handlers
				oPHRPReader.MessageEvent += new PHRPReader.clsPHRPReader.MessageEventEventHandler(mPHRPReader_MessageEvent);
				oPHRPReader.ErrorEvent += new PHRPReader.clsPHRPReader.ErrorEventEventHandler(mPHRPReader_ErrorEvent);
				oPHRPReader.WarningEvent += new PHRPReader.clsPHRPReader.WarningEventEventHandler(mPHRPReader_WarningEvent);

				// Report any errors cached during instantiation of mPHRPReader
				foreach (string strMessage in oPHRPReader.ErrorMessages.Distinct())
				{
					m_SystemLogManager.addApplicationLog("Error: " + strMessage);
					Console.WriteLine(strMessage);
				}

				// Report any warnings cached during instantiation of mPHRPReader
				foreach (string strMessage in oPHRPReader.WarningMessages.Distinct())
				{
					m_SystemLogManager.addApplicationLog("Warning: " + strMessage);
					Console.WriteLine(strMessage);
				}
				if (oPHRPReader.WarningMessages.Count > 0)
					Console.WriteLine();

				oPHRPReader.ClearErrors();
				oPHRPReader.ClearWarnings();

				System.Data.Common.DbTransaction dbTrans;
				mDBWrapper.InitPHRPInsertCommand(out dbTrans);

				Dictionary<string, string> dctData = new Dictionary<string, string>();

				int line_num = 0;

				Console.WriteLine("Populating database using PHRP");

				while (oPHRPReader.MoveNext())
				{
					PHRPReader.clsPSM objCurrentPSM = oPHRPReader.CurrentPSM;
					line_num += 1;

					dctData.Clear();

					dctData.Add("instrument_id", instrument_id.ToString());
					dctData.Add("random_id", random_id.ToString());
					dctData.Add("Result_ID", objCurrentPSM.ResultID.ToString());
					dctData.Add("Scan", objCurrentPSM.ScanNumberStart.ToString());
					dctData.Add("CollisionMode", objCurrentPSM.CollisionMode);
					dctData.Add("Charge", objCurrentPSM.Charge.ToString());

					dctData.Add("Peptide_MH", PHRPReader.clsPeptideMassCalculator.ConvoluteMass(objCurrentPSM.PeptideMonoisotopicMass, 0, 1).ToString("0.00000"));
					dctData.Add("Peptide_Sequence", objCurrentPSM.Peptide);

					dctData.Add("DelM_Da", objCurrentPSM.MassErrorDa);
					dctData.Add("DelM_PPM", objCurrentPSM.MassErrorPPM);
					
					double msgfSpecProb;
					if (double.TryParse(objCurrentPSM.MSGFSpecProb, out msgfSpecProb))
						dctData.Add("MSGFSpecProb", objCurrentPSM.MSGFSpecProb);
					else
						dctData.Add("MSGFSpecProb", "1");

					dctData.Add("Unique_Seq_ID", objCurrentPSM.SeqID.ToString());
					dctData.Add("Cleavage_State", ((int)objCurrentPSM.CleavageState).ToString());

					mDBWrapper.ExecutePHRPInsertCommand(dctData, line_num);
			

				}

				// Commit the transaction
				dbTrans.Commit();
			}
			catch (Exception ex)
			{
				throw new Exception("Error in LoadFilesUsingPHRP: " + ex.Message, ex);
			}
			return false;
		}

	
		//FUNCTION WILL SEARCH THROUGH A FILE NAME, ENSURING IT IS A VALID TABLE EXTENSION AND RETURNING
		//THE POSITION SO THAT IT CAN BE PASSED TO OUR DBINTERFACE/OTHER CLASSES FOR PROCESSING
		public int return_file_table_position(string filename, string[] valid_file_tables)
		{
			string baseFilenameLCase = System.IO.Path.GetFileNameWithoutExtension(filename).ToLower();

			//LOOP THROUGH ALL VALID FILE/TABLE EXTENSIONS
			for (int i = 0; i < valid_file_tables.Length; i++)
			{
				if (baseFilenameLCase.EndsWith(valid_file_tables[i].ToLower()))
				{
					// Match found
					//RETURN THE POSITION ID IN OUR FILE/TABLE LIST
					return i;
				}

			}
			return -1;
		}

		#region "Error handlers"

		protected void DBWrapper_ErrorEvent(string errorMessage)
		{
			m_SystemLogManager.addApplicationLog(errorMessage);
		}

		void mPHRPReader_ErrorEvent(string strErrorMessage)
		{
			m_SystemLogManager.addApplicationLog("PHRPReader error: " + strErrorMessage);
		}

		void mPHRPReader_MessageEvent(string strMessage)
		{
			m_SystemLogManager.addApplicationLog(strMessage);
		}

		void mPHRPReader_WarningEvent(string strWarningMessage)
		{
			m_SystemLogManager.addApplicationLog("PHRPReader warning: " + strWarningMessage);
		}

		#endregion


	}
}
