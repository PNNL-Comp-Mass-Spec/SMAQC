using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using PHRPReader;

namespace SMAQC
{
	class Filter
	{
		//DECLARE VARIABLES
		public DBWrapper mDBWrapper;                                                                //CREATE DB INTERFACE OBJECT
		public string instrument_id;                                                                //INSTRUMENT ID
		public int random_id;                                                                       //RANDOM ID
		public DataFileFormatter DFF = new DataFileFormatter();                                     //DFF OBJECT
		readonly SystemLogManager m_SystemLogManager;

		//CONSTRUCTOR
		public Filter(ref DBWrapper DBInterface, string instrument_id, int random_id, ref SystemLogManager systemLogManager)
		{
			mDBWrapper = DBInterface;
			this.instrument_id = instrument_id;
			this.random_id = random_id;
			m_SystemLogManager = systemLogManager;

			// Attach the event handler
			mDBWrapper.ErrorEvent += DBWrapper_ErrorEvent;
		}

		//THIS FUNCTION RETURNS WHETHER OR NOT WE ARE CURRENTLY WORKING WITH _SCANSTATSEX.TXT
		public Boolean ScanStatsExBugFixer(string file_to_load)
		{
			var value = file_to_load.IndexOf("_ScanStatsEx.txt", StringComparison.OrdinalIgnoreCase);

			//IF FOUND RETURN TRUE
			if (value >= 0)
			{
				return true;
			}

			//ELSE RETURN FALSE
			return false;
		}

		//CREATE BULK INSERT COMPATIBLE FILE
		public void parse_and_filter(string temp_file, string file_to_load)
		{
			//DECLARE VARIABLES
			var line_num = 0;

			const char newDelimiter = '\t';

			//OPEN TEMP srInFile
			using (var swOutFile = new StreamWriter(temp_file))
			{

				//Console.WriteLine("WRITE TO: {0} ... LOAD FROM: {1}", temp_file, file_to_load);

				using (var srInFile = new StreamReader(file_to_load))
				{
					string line;
					while ((line = srInFile.ReadLine()) != null)
					{
						//NEW LINE SO CLEAR QUERY_INFO
						var query_info = "";

						//SPLIT GIVEN DATA FILES BY TAB
						var delimiters = new[] { '\t' };

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
						for (var i = 0; i < parts.Length; i++)
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
				}
			}

		}

		//THIS FUNCTION:
		//1. LOOPS THROUGH A VALID FILE LIST
		//2. Calls another function that loads that file and rewrites the \tab as ',' separated.
		//3. From the filename, determines the correct table to insert into, appends temp
		//4. Calls our bulk insert function
		public void LoadFilesAndInsertIntoDB(List<string> FileList, string[] valid_file_tables, string dataset)
		{
			//LOOP THROUGH EACH FILE
			foreach (var fileName in FileList)
			{
				//STORES THE FULL PATH TO FILE + TEMP FILE NAME
				var file_info = String.Copy(fileName);				//FILENAME WE WANT TO LOAD INTO DB
				var temp_file = Path.GetTempFileName();	//WRITE TO THIS FILE [TEMP FILE]
				var query_table = "temp";							//USED AS PREFIX PORTION OF TABLE

				//DETERMINE IF WE HAVE A TABLE TO INSERT INTO DEPENDING ON OUR INPUT FILENAME
				var j = return_file_table_position(file_info, valid_file_tables);

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
					parse_and_filter(temp_file, file_info);


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
					Console.WriteLine("ERROR, unrecognized file " + fileName);
				}
				//DELETE TEMP FILE
				File.Delete(temp_file);
			}
		}

		public bool LoadFilesUsingPHRP(string sInputFolderPath, string sDataset)
		{

			// Look for a valid input file
			var sInputFilePath = PHRPReader.clsPHRPReader.AutoDetermineBestInputFile(sInputFolderPath, sDataset);

			if (string.IsNullOrEmpty(sInputFilePath))
			{
				throw new FileNotFoundException("Valid input file not found for dataset " + sDataset + " in folder " + sInputFolderPath);
			}

			try
			{
				const bool blnLoadModsAndSeqInfo = true;
				const bool blnLoadMSGFResults = true;
				const bool blnLoadScanStats = false;

				var oPHRPReader = new PHRPReader.clsPHRPReader(sInputFilePath, PHRPReader.clsPHRPReader.ePeptideHitResultType.Unknown, blnLoadModsAndSeqInfo, blnLoadMSGFResults, blnLoadScanStats)
				{
					EchoMessagesToConsole = false,
					SkipDuplicatePSMs = true
				};

				// Attach the error handlers
				oPHRPReader.MessageEvent += mPHRPReader_MessageEvent;
				oPHRPReader.ErrorEvent += mPHRPReader_ErrorEvent;
				oPHRPReader.WarningEvent += mPHRPReader_WarningEvent;

				// Report any errors cached during instantiation of mPHRPReader
				foreach (var strMessage in oPHRPReader.ErrorMessages.Distinct())
				{
					m_SystemLogManager.addApplicationLog("Error: " + strMessage);
					Console.WriteLine(strMessage);
				}

				// Report any warnings cached during instantiation of mPHRPReader
				foreach (var strMessage in oPHRPReader.WarningMessages.Distinct())
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

                // Dictionary has key/value pairs of information about the best peptide for the scan
				var dctBestPeptide = new Dictionary<string, string>();
				dctBestPeptide.Clear();

                // Dictionary mapping normalized peptide sequences to NormalizedSeqID values
                // The NormalizedSeqID values are custom-assigned by this class to keep track of peptide sequences 
                //   on a basis where modifications are tracked but not mapped to specific residues
                //   This is done so that peptides like PEPT*IDES and PEPTIDES* are counted as the same peptide
			    var normalizedPeptides = new Dictionary<string, int>();

				var intBestPeptideScan = -1;
				var intBestPeptideCharge = -1;
				double dblBestPeptideScore = 100;

				var line_num = 0;
				var prev_scan = 0;
				var prev_charge = 0;
				var prev_peptide = string.Empty;

				Console.WriteLine("Populating database using PHRP");

				// Read the data using PHRP Reader
				// Only store the best scoring peptide for each scan/charge combo
                // Furthermore, normalize peptide sequences so that modifications are not associated with specific residues

				while (oPHRPReader.MoveNext())
				{
					var objCurrentPSM = oPHRPReader.CurrentPSM;
					line_num += 1;

					string strCurrentPeptide;
					string strPrefix;
					string strSuffix;

                    PHRPReader.clsPeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(objCurrentPSM.Peptide, out strCurrentPeptide, out strPrefix, out strSuffix);

					if (prev_scan == objCurrentPSM.ScanNumberStart && prev_charge == objCurrentPSM.Charge && prev_peptide == strCurrentPeptide)
						// Skip this entry (same peptide, different protein)
						continue;

					if (intBestPeptideScan > 0 && !(intBestPeptideScan == objCurrentPSM.ScanNumberStart && intBestPeptideCharge == objCurrentPSM.Charge))
					{
                        // Store the cached peptide
						mDBWrapper.ExecutePHRPInsertCommand(dctBestPeptide, line_num);
						intBestPeptideScan = -1;
						intBestPeptideCharge = -1;
						dblBestPeptideScore = 100;
					}

                    // Dictionary has key/value pairs of information about the peptide
					var dctCurrentPeptide = new Dictionary<string, string>();
					dctCurrentPeptide.Clear();

					dctCurrentPeptide.Add("instrument_id", instrument_id);
					dctCurrentPeptide.Add("random_id", random_id.ToString());
					dctCurrentPeptide.Add("Result_ID", objCurrentPSM.ResultID.ToString());
					dctCurrentPeptide.Add("Scan", objCurrentPSM.ScanNumberStart.ToString());
					dctCurrentPeptide.Add("CollisionMode", objCurrentPSM.CollisionMode);
					dctCurrentPeptide.Add("Charge", objCurrentPSM.Charge.ToString());

					dctCurrentPeptide.Add("Peptide_MH", PHRPReader.clsPeptideMassCalculator.ConvoluteMass(objCurrentPSM.PeptideMonoisotopicMass, 0, 1).ToString("0.00000"));
					dctCurrentPeptide.Add("Peptide_Sequence", objCurrentPSM.Peptide);

					dctCurrentPeptide.Add("DelM_Da", objCurrentPSM.MassErrorDa);
					dctCurrentPeptide.Add("DelM_PPM", objCurrentPSM.MassErrorPPM);

					double msgfSpecProb;
					if (double.TryParse(objCurrentPSM.MSGFSpecProb, out msgfSpecProb))
						dctCurrentPeptide.Add("MSGFSpecProb", objCurrentPSM.MSGFSpecProb);
					else
						dctCurrentPeptide.Add("MSGFSpecProb", "1");

                    var normalizedPeptide = NormalizeSequence(objCurrentPSM.PeptideCleanSequence, objCurrentPSM.ModifiedResidues);

				    int normalizedSeqID;
                    if (!normalizedPeptides.TryGetValue(normalizedPeptide, out normalizedSeqID))
                    {
                        normalizedSeqID = normalizedPeptides.Count + 1;
                        normalizedPeptides.Add(normalizedPeptide, normalizedSeqID);
                    }
                    
					// Note: previously stored objCurrentPSM.SeqID.ToString()
                    // Now storing normalizedSeqID
                    dctCurrentPeptide.Add("Unique_Seq_ID", normalizedSeqID.ToString());

					dctCurrentPeptide.Add("Cleavage_State", ((int)objCurrentPSM.CleavageState).ToString());

				    // Check whether this is a phosphopeptide
					byte phosphoFlag = 0;
					foreach (var modification in objCurrentPSM.ModifiedResidues)
					{
						if (modification.ModDefinition.MassCorrectionTag == "Phosph")
						{
							phosphoFlag = 1;
							break;
						}

						if (Math.Abs(modification.ModDefinition.ModificationMass - 79.966331) < 0.075 &&
							(modification.Residue == 'S' || modification.Residue == 'T' || modification.Residue == 'Y'))
						{
							phosphoFlag = 1;
							break;
						}
					}

					dctCurrentPeptide.Add("Phosphopeptide", phosphoFlag.ToString());

					if (intBestPeptideScan < 0 || msgfSpecProb < dblBestPeptideScore)
					{
						dctBestPeptide = dctCurrentPeptide;

						intBestPeptideScan = objCurrentPSM.ScanNumberStart;
						intBestPeptideCharge = objCurrentPSM.Charge;
						dblBestPeptideScore = msgfSpecProb;
					}

					prev_scan = objCurrentPSM.ScanNumberStart;
					prev_charge = objCurrentPSM.Charge;
					prev_peptide = string.Copy(strCurrentPeptide);

				}

				if (intBestPeptideScan > 0)
				{
                    // Store the cached peptide
					mDBWrapper.ExecutePHRPInsertCommand(dctBestPeptide, line_num);
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

	    private string NormalizeSequence(string peptideCleanSequence, ICollection<clsAminoAcidModInfo> modifiedResidues)
	    {
	        if (modifiedResidues.Count == 0)
                return peptideCleanSequence;

	        var sbModifications = new StringBuilder();

	        foreach (var modEntry in modifiedResidues)
	        {
                if (string.IsNullOrEmpty(modEntry.ModDefinition.MassCorrectionTag))
                    sbModifications.Append(modEntry.ModDefinition.ModificationSymbol);
                else
	                sbModifications.Append(modEntry.ModDefinition.MassCorrectionTag);
	        }

            return peptideCleanSequence + "_" + sbModifications.ToString();
	    }


	    //FUNCTION WILL SEARCH THROUGH A FILE NAME, ENSURING IT IS A VALID TABLE EXTENSION AND RETURNING
		//THE POSITION SO THAT IT CAN BE PASSED TO OUR DBINTERFACE/OTHER CLASSES FOR PROCESSING
		public int return_file_table_position(string filename, string[] valid_file_tables)
		{
		    var baseName = Path.GetFileNameWithoutExtension(filename);
            if (baseName != null)
		    {
                var baseFilenameLCase = baseName.ToLower();

		        //LOOP THROUGH ALL VALID FILE/TABLE EXTENSIONS
		        for (var i = 0; i < valid_file_tables.Length; i++)
		        {
		            if (baseFilenameLCase.EndsWith(valid_file_tables[i].ToLower()))
		            {
		                // Match found
		                //RETURN THE POSITION ID IN OUR FILE/TABLE LIST
		                return i;
		            }

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
