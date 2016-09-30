using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using MSGFResultsSummarizer;
using PHRPReader;

namespace SMAQC
{
	class Filter
	{

		public readonly DBWrapper mDBWrapper;                                                                // Create db interface object
		public readonly string instrument_id;                                                                // Instrument id
		public readonly int random_id;                                                                       // Random id
		public readonly DataFileFormatter DFF = new DataFileFormatter();                                     // Dff object
		private readonly SystemLogManager m_SystemLogManager;

		/// <summary>
        /// Constructor
		/// </summary>
		/// <param name="DBInterface"></param>
		/// <param name="instrument_id"></param>
		/// <param name="random_id"></param>
		/// <param name="systemLogManager"></param>
		public Filter(ref DBWrapper DBInterface, string instrument_id, int random_id, ref SystemLogManager systemLogManager)
		{
			mDBWrapper = DBInterface;
			this.instrument_id = instrument_id;
			this.random_id = random_id;
			m_SystemLogManager = systemLogManager;

			// Attach the event handler
			mDBWrapper.ErrorEvent += DBWrapper_ErrorEvent;
		}

		/// <summary>
        /// Returns true if processing the extended scan stats file (_ScanStatsEx.txt)
		/// </summary>
		/// <param name="file_to_load"></param>
		/// <returns></returns>
		public bool ScanStatsExBugFixer(string file_to_load)
		{
			var value = file_to_load.IndexOf("_ScanStatsEx.txt", StringComparison.OrdinalIgnoreCase);

			if (value >= 0)
			{
				return true;
			}

			return false;
		}

		/// <summary>
		/// Create a bulk-insert compatible file
		/// </summary>
		/// <param name="temp_file"></param>
		/// <param name="file_to_load"></param>
		public void parse_and_filter(string temp_file, string file_to_load)
		{
			var line_num = 0;

			const char newDelimiter = '\t';

            // Split on tab characters
            var delimiters = new[] { '\t' };

			// Create the output file
			using (var swOutFile = new StreamWriter(temp_file))
			{

				//Console.WriteLine("WRITE TO: {0} ... LOAD FROM: {1}", temp_file, file_to_load);

                // Open the input file
				using (var srInFile = new StreamReader(file_to_load))
				{
					
					while (!srInFile.EndOfStream)
					{
					    var line = srInFile.ReadLine();

					    if (string.IsNullOrEmpty(line))
					        continue;

						var query_info = "";

						var parts = line.Split(delimiters, StringSplitOptions.None);
						
						if (line_num == 0)
						{
                            // Prepend the additional headers
							query_info += "instrument_id" + newDelimiter + "random_id" + newDelimiter;
						}
						else
						{
                            // Prepend Instrument_ID and Random_ID
							query_info += instrument_id + newDelimiter + random_id + newDelimiter;
						}

						// Process the fields
						foreach (var dataValue in parts)
						{
                            if (dataValue.Equals("[PAD]"))
						    {
						        query_info += newDelimiter;
						    }
						    else
						    {
						        // Replace any tab characters with semicolons
                                query_info += dataValue.Replace(newDelimiter, ';') + newDelimiter;
						    }
						}

					    if (!string.IsNullOrEmpty(query_info))
					    {
                            // Final column; remove the trailing tab character								
                            query_info = query_info.Substring(0, query_info.Length - 1);

                            // Append a carriage return
                            query_info += Environment.NewLine;

                            // Write out the data line
                            swOutFile.Write(query_info);
					    }
					   						
						line_num++;
					}
				}
			}

		}
	
        /// <summary>
        /// Loop through the file list, processing each file
        /// </summary>
        /// <param name="fileList"></param>
        /// <param name="valid_file_tables"></param>
        /// <param name="dataset"></param>
        /// <remarks>
        /// For each file:
        ///   1. Calls another function that loads that file and rewrites the \tab as ',' separated.
        ///   2. From the filename, determines the correct table to insert into, appends temp
        ///   3. Calls our bulk insert function
        /// </remarks>
		public void LoadFilesAndInsertIntoDB(List<string> fileList, string[] valid_file_tables, string dataset)
		{
			
			foreach (var fileName in fileList)
			{

                var file_info = string.Copy(fileName);

                // Obtain a temp file
				var temp_file = Path.GetTempFileName();

				var query_table = "temp";

                // Determine if we have a table to insert into depending on our input filename
				var j = return_file_table_position(file_info, valid_file_tables);

				if (j >= 0)
				{
                    // Valid table

                    // Does this file need to be reformated [variable column support]
					if (DFF.handleFile(file_info, dataset))
					{
						// Yes

						// Rebuild [SAVE TO DFF.TempFilePath BY DEFAULT]
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

				// Delete the tempfile
				File.Delete(temp_file);
			}
		}

		public bool LoadFilesUsingPHRP(string sInputFolderPath, string sDataset)
		{

			// Look for a valid input file
			var sInputFilePath = clsPHRPReader.AutoDetermineBestInputFile(sInputFolderPath, sDataset);

			if (string.IsNullOrEmpty(sInputFilePath))
			{
				throw new FileNotFoundException("Valid input file not found for dataset " + sDataset + " in folder " + sInputFolderPath);
			}

			try
			{
				const bool blnLoadModsAndSeqInfo = true;
				const bool blnLoadMSGFResults = true;
				const bool blnLoadScanStats = false;

				var oPHRPReader = new clsPHRPReader(sInputFilePath, clsPHRPReader.ePeptideHitResultType.Unknown, blnLoadModsAndSeqInfo, blnLoadMSGFResults, blnLoadScanStats)
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

				DbTransaction dbTrans;
				mDBWrapper.InitPHRPInsertCommand(out dbTrans);

                // Dictionary has key/value pairs of information about the best peptide for the scan
				var dctBestPeptide = new Dictionary<string, string>();
				dctBestPeptide.Clear();

                // Keys in this dictionary are clean sequences (peptide sequence without any mod symbols)
                // Values are lists of modified residue combinations that correspond to the given clean sequence
                // Each combination of residues has a corresponding "best" SeqID associated with it
                //
                // When comparing a new sequence to entries in this dictionary, if the mod locations are all within one residue of an existing normalized sequence,
                //  the new sequence and mods is not added
                // For example, LS*SPATLNSR and LSS*PATLNSR are considered equivalent, and will be tracked as LSSPATLNSR with * at index 1
                // But P#EPT*IDES and PEP#T*IDES and P#EPTIDES* are all different, and are tracked with entries:
                //  PEPTIDES with # at index 0 and * at index 3
                //  PEPTIDES with # at index 2 and * at index 3
                //  PEPTIDES with # at index 0 and * at index 7
                //
                // If sequenceInfoAvailable is True, then instead of using mod symbols we use ModNames from the Mod_Description column in the _SeqInfo.txt file
                //   For example, VGVEASEETPQT with Phosph at index 5
                //
                // The SeqID value tracked by udtNormalizedPeptideType is the SeqID of the first sequence to get normalized to the given entry
                // If sequenceInfoAvailable is False, values are the ResultID value of the first peptide to get normalized to the given entry
                //
                var normalizedPeptides = new Dictionary<string, List<clsNormalizedPeptideInfo>>();

                var intBestPeptideScan = -1;
				var intBestPeptideCharge = -1;
				double dblBestPeptideScore = 100;

				var line_num = 0;
				var prev_scan = 0;
				var prev_charge = 0;
				var prev_peptide = string.Empty;

				Console.WriteLine("Populating database using PHRP");

                // Regex to match keratin proteins
			    var reKeratinProtein = clsMSGFResultsSummarizer.GetKeratinRegEx();

                // Regex to match trypsin proteins
			    var reTrypsinProtein = clsMSGFResultsSummarizer.GetTrypsinRegEx();

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

                    clsPeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(objCurrentPSM.Peptide, out strCurrentPeptide, out strPrefix, out strSuffix);

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

					dctCurrentPeptide.Add("Peptide_MH", clsPeptideMassCalculator.ConvoluteMass(objCurrentPSM.PeptideMonoisotopicMass, 0, 1).ToString("0.00000"));
					dctCurrentPeptide.Add("Peptide_Sequence", objCurrentPSM.Peptide);

					dctCurrentPeptide.Add("DelM_Da", objCurrentPSM.MassErrorDa);
					dctCurrentPeptide.Add("DelM_PPM", objCurrentPSM.MassErrorPPM);

					double msgfSpecProb;
					if (double.TryParse(objCurrentPSM.MSGFSpecProb, out msgfSpecProb))
						dctCurrentPeptide.Add("MSGFSpecProb", objCurrentPSM.MSGFSpecProb);
					else
						dctCurrentPeptide.Add("MSGFSpecProb", "1");

                    var normalizedPeptide = NormalizeSequence(objCurrentPSM.PeptideCleanSequence, objCurrentPSM.ModifiedResidues, objCurrentPSM.SeqID);

				    var normalizedSeqID = clsMSGFResultsSummarizer.FindNormalizedSequence(normalizedPeptides, normalizedPeptide);

				    if (normalizedSeqID == clsPSMInfo.UNKNOWN_SEQID)
                    {
                        // New normalized peptide
                        
                        List<clsNormalizedPeptideInfo> observedNormalizedPeptides;

                        if (!normalizedPeptides.TryGetValue(normalizedPeptide.CleanSequence, out observedNormalizedPeptides))
                        {
                            // This clean sequence is not yet tracked; add it
                            observedNormalizedPeptides = new List<clsNormalizedPeptideInfo>();
                            normalizedPeptides.Add(normalizedPeptide.CleanSequence, observedNormalizedPeptides);
                        }

                        normalizedSeqID = objCurrentPSM.SeqID;
                        if (normalizedSeqID < 0)
                        {
                            normalizedSeqID = objCurrentPSM.ResultID;
                        }

                        // Make a new normalized peptide entry that does not have clean sequence 
                        // (to conserve memory, since keys in dictionary normalizedPeptides are clean sequence)
                        var normalizedPeptideToStore = new clsNormalizedPeptideInfo(string.Empty);
                        normalizedPeptideToStore.StoreModifications(normalizedPeptide.Modifications);
                        normalizedPeptideToStore.SeqID = normalizedSeqID;

                        observedNormalizedPeptides.Add(normalizedPeptideToStore);
                    }
                    
					// Associate the normalized SeqID with the current peptide
                    dctCurrentPeptide.Add("Unique_Seq_ID", normalizedSeqID.ToString());

					dctCurrentPeptide.Add("Cleavage_State", ((int)objCurrentPSM.CleavageState).ToString());

				    // Check whether this is a phosphopeptide
					byte phosphoFlag = 0;
					foreach (var modification in objCurrentPSM.ModifiedResidues)
					{
						if (string.Equals(modification.ModDefinition.MassCorrectionTag, "Phosph", StringComparison.InvariantCultureIgnoreCase))
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

                    // Check whether this is a peptide from Keratin
                    byte keratinFlag = 0;
                    if (objCurrentPSM.Proteins.Any(protein => reKeratinProtein.IsMatch(protein)))
                    {
                        keratinFlag = 1;
                    }
                    dctCurrentPeptide.Add("Keratinpeptide", keratinFlag.ToString());

                    // Store the number of missed cleavages
                    dctCurrentPeptide.Add("MissedCleavages", objCurrentPSM.NumMissedCleavages.ToString());

                    // Check whether this is a peptide from trypsin or trypsinogen
                    byte trypsinFlag = 0;
                    if (objCurrentPSM.Proteins.Any(protein => reTrypsinProtein.IsMatch(protein)))
                    {
                        trypsinFlag = 1;
                    }
                    dctCurrentPeptide.Add("Trypsinpeptide", trypsinFlag.ToString());

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

	    private clsNormalizedPeptideInfo NormalizeSequence(string peptideCleanSequence, IEnumerable<clsAminoAcidModInfo> modifiedResidues, int seqId)
	    {
            var modifications = new List< KeyValuePair<string, int>>();

            foreach (var modEntry in modifiedResidues)
            {
                string modSymbolOrName;
                var residueIndex = modEntry.ResidueLocInPeptide - 1;

                if (string.IsNullOrEmpty(modEntry.ModDefinition.MassCorrectionTag))
                    modSymbolOrName = modEntry.ModDefinition.ModificationSymbol.ToString();
                else
                    modSymbolOrName = modEntry.ModDefinition.MassCorrectionTag;

                modifications.Add(new KeyValuePair<string, int>(modSymbolOrName, residueIndex));
            }

            var normalizedPeptide = clsMSGFResultsSummarizer.GetNormalizedPeptideInfo(peptideCleanSequence, modifications, seqId);
	        return normalizedPeptide;

	    }


	    // Function will search through a file name, ensuring it is a valid table extension and returning
		// The position so that it can be passed to our dbinterface/other classes for processing
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
