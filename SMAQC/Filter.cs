using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using MSGFResultsSummarizer;
using PHRPReader;

namespace SMAQC
{
    internal class Filter
    {
        public readonly DBWrapper mDBWrapper;

        public readonly string mInstrumentId;

        public readonly int mRandomId;

        public readonly DataFileFormatter mDataFileFormatter = new DataFileFormatter();

        private readonly SystemLogManager mSystemLogManager;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="DBInterface"></param>
        /// <param name="instrument_id"></param>
        /// <param name="random_id"></param>
        /// <param name="systemLogManager"></param>
        public Filter(DBWrapper DBInterface, string instrument_id, int random_id, SystemLogManager systemLogManager)
        {
            mDBWrapper = DBInterface;
            mInstrumentId = instrument_id;
            mRandomId = random_id;
            mSystemLogManager = systemLogManager;

            // Attach the event handler
            mDBWrapper.ErrorEvent += DBWrapper_ErrorEvent;
        }

        /// <summary>
        /// Returns true if processing the extended scan stats file (_ScanStatsEx.txt)
        /// </summary>
        /// <param name="fileToLoad"></param>
        /// <returns></returns>
        public bool ScanStatsExBugFixer(string fileToLoad)
        {
            var value = fileToLoad.IndexOf("_ScanStatsEx.txt", StringComparison.OrdinalIgnoreCase);

            return value >= 0;
        }

        /// <summary>
        /// Create a bulk-insert compatible file
        /// </summary>
        /// <param name="filePathToLoad"></param>
        /// <param name="targetFilePath"></param>
        private void CreateBulkInsertDataFile(string filePathToLoad, string targetFilePath)
        {
            var lineNumber = 0;

            const string tabChar = "\t";

            // Split on tab characters
            var delimiters = new[] { '\t' };

            using (var srInFile = new StreamReader(new FileStream(filePathToLoad, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            using (var swOutFile = new StreamWriter(new FileStream(targetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                while (!srInFile.EndOfStream)
                {
                    var line = srInFile.ReadLine();

                    if (string.IsNullOrEmpty(line))
                        continue;

                    var filteredData = new List<string>();

                    var parts = line.Split(delimiters, StringSplitOptions.None);

                    if (lineNumber == 0)
                    {
                        // Prepend the additional headers
                        filteredData.Add("instrument_id");
                        filteredData.Add("random_id");
                    }
                    else
                    {
                        // Prepend Instrument_ID and Random_ID
                        filteredData.Add(mInstrumentId);
                        filteredData.Add(mRandomId.ToString());
                    }

                    // Process the fields
                    foreach (var dataValue in parts)
                    {
                        if (dataValue.Equals("[PAD]"))
                        {
                            filteredData.Add("");
                        }
                        else
                        {
                            // Replace any tab characters with semicolons
                            filteredData.Add(dataValue.Replace(tabChar, ";"));
                        }
                    }

                    if (filteredData.Count > 0)
                    {
                        // Write out the data line
                        swOutFile.WriteLine(string.Join(tabChar, filteredData));
                    }

                    lineNumber++;
                }
            }
        }

        /// <summary>
        /// Loop through the file list, processing each file
        /// </summary>
        /// <param name="fileList">Files to load. Keys are file paths and Values are lists of header column suffixes to ignore, e.g. _SignalToNoise</param>
        /// <param name="validFileExtensions"></param>
        /// <param name="dataset"></param>
        /// <remarks>
        /// For each file:
        ///   1. Calls another function that loads that file and rewrites the \tab as ',' separated.
        ///   2. From the filename, determines the correct table to insert into, appends temp
        ///   3. Calls our bulk insert function
        /// </remarks>
        public void LoadFilesAndInsertIntoDB(Dictionary<string, List<string>> fileList, string[] validFileExtensions, string dataset)
        {
            foreach (var candidateFile in fileList)
            {
                var filePath = string.Copy(candidateFile.Key);

                // Determine if we have a table to insert into depending on our input filename
                var knownFile = IsKnownFileExtension(filePath, validFileExtensions, out var targetTableName);

                if (!knownFile)
                {
                    // Not a recognized file; cannot load it
                    Console.WriteLine("ERROR, unrecognized file " + filePath);
                    continue;
                }

                // Valid table
                // Create a temp file
                var tempFilePath = Path.GetTempFileName();

                var excludedFieldNameSuffixes = candidateFile.Value;

                // Does this file need to be reformatted [variable column support]
                if (mDataFileFormatter.HandleFile(filePath, dataset))
                {
                    // Yes

                    var reformattedFilePath = mDataFileFormatter.TempFilePath;

                    // Parse and format the file for bulk insert queries
                    // Will add columns instrument_id and random_id
                    CreateBulkInsertDataFile(reformattedFilePath, tempFilePath);
                }
                else
                {
                    // Will add columns instrument_id and random_id
                    CreateBulkInsertDataFile(filePath, tempFilePath);
                }

                var targetTable = "temp" + targetTableName;

                Console.WriteLine("Populating Table {0}", targetTable);

                mDBWrapper.BulkInsert(targetTable, tempFilePath, excludedFieldNameSuffixes);

                // Delete the temporary file
                File.Delete(tempFilePath);
            }
        }

        public bool LoadFilesUsingPHRP(string inputFolderPath, string dataset)
        {
            // Look for a valid input file
            var inputFilePath = clsPHRPReader.AutoDetermineBestInputFile(inputFolderPath, dataset);

            if (string.IsNullOrEmpty(inputFilePath))
            {
                throw new FileNotFoundException("Valid input file not found for dataset " + dataset + " in folder " + inputFolderPath);
            }

            try
            {
                const bool loadModsAndSeqInfo = true;
                const bool loadMSGFResults = true;
                const bool loadScanStats = false;

                var peptideMassCalculator = new clsPeptideMassCalculator();

                var reader = new clsPHRPReader(inputFilePath, clsPHRPReader.PeptideHitResultTypes.Unknown, loadModsAndSeqInfo, loadMSGFResults, loadScanStats)
                {
                    EchoMessagesToConsole = false,
                    SkipDuplicatePSMs = true
                };

                // Attach the error handlers
                reader.StatusEvent += mPHRPReader_MessageEvent;
                reader.ErrorEvent += mPHRPReader_ErrorEvent;
                reader.WarningEvent += mPHRPReader_WarningEvent;

                // Report any errors cached during instantiation of mPHRPReader
                foreach (var message in reader.ErrorMessages.Distinct())
                {
                    mSystemLogManager.AddApplicationLogError("Error: " + message);
                }

                // Report any warnings cached during instantiation of mPHRPReader
                foreach (var message in reader.WarningMessages.Distinct())
                {
                    mSystemLogManager.AddApplicationLogWarning("Warning: " + message);
                }
                if (reader.WarningMessages.Count > 0)
                    Console.WriteLine();

                reader.ClearErrors();
                reader.ClearWarnings();

                mDBWrapper.InitPHRPInsertCommand(out var dbTrans);

                // Dictionary has key/value pairs of information about the best peptide for the scan
                var dctBestPeptide = new Dictionary<string, string>();

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
                // The SeqID value tracked by clsNormalizedPeptideInfo is the SeqID of the first sequence to get normalized to the given entry
                // If sequenceInfoAvailable is False, values are the ResultID value of the first peptide to get normalized to the given entry
                //
                var normalizedPeptides = new Dictionary<string, List<clsNormalizedPeptideInfo>>();

                var bestPeptideScan = -1;
                var bestPeptideCharge = -1;
                double bestPeptideScore = 100;

                var lineNumber = 0;
                var prevScan = 0;
                var prevCharge = 0;
                var prevPeptide = string.Empty;

                Console.WriteLine("Populating database using PHRP");

                // Regex to match keratin proteins
                var reKeratinProtein = clsMSGFResultsSummarizer.GetKeratinRegEx();

                // RegEx to match trypsin proteins
                var reTrypsinProtein = clsMSGFResultsSummarizer.GetTrypsinRegEx();

                // Read the data using PHRP Reader
                // Only store the best scoring peptide for each scan/charge combo
                // Furthermore, normalize peptide sequences so that modifications are not associated with specific residues

                while (reader.MoveNext())
                {
                    var currentPSM = reader.CurrentPSM;
                    lineNumber++;

                    clsPeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(currentPSM.Peptide, out var currentPeptide, out _, out _);

                    if (prevScan == currentPSM.ScanNumberStart && prevCharge == currentPSM.Charge && prevPeptide == currentPeptide)
                    {
                        // Skip this entry (same peptide, different protein)
                        continue;
                    }

                    if (bestPeptideScan > 0 && !(bestPeptideScan == currentPSM.ScanNumberStart && bestPeptideCharge == currentPSM.Charge))
                    {
                        // Store the cached peptide
                        mDBWrapper.ExecutePHRPInsertCommand(dctBestPeptide, lineNumber);
                        bestPeptideScan = -1;
                        bestPeptideCharge = -1;
                        bestPeptideScore = 100;
                    }

                    // Dictionary has key/value pairs of information about the peptide
                    var dctCurrentPeptide = new Dictionary<string, string>
                    {
                        {"instrument_id", mInstrumentId},
                        {"random_id", mRandomId.ToString()},
                        {"Result_ID", currentPSM.ResultID.ToString()},
                        {"Scan", currentPSM.ScanNumberStart.ToString()},
                        {"CollisionMode", currentPSM.CollisionMode},
                        {"Charge", currentPSM.Charge.ToString()},
                        {"Peptide_MH", peptideMassCalculator.ConvoluteMass(currentPSM.PeptideMonoisotopicMass, 0).ToString("0.00000")},
                        {"Peptide_Sequence", currentPSM.Peptide},
                        {"DelM_Da", currentPSM.MassErrorDa},
                        {"DelM_PPM", currentPSM.MassErrorPPM}
                    };

                    if (double.TryParse(currentPSM.MSGFSpecEValue, out var msgfSpecProb))
                        dctCurrentPeptide.Add("MSGFSpecProb", currentPSM.MSGFSpecEValue);
                    else
                        dctCurrentPeptide.Add("MSGFSpecProb", "1");

                    var normalizedPeptide = NormalizeSequence(currentPSM.PeptideCleanSequence, currentPSM.ModifiedResidues, currentPSM.SeqID);

                    var normalizedSeqID = clsMSGFResultsSummarizer.FindNormalizedSequence(normalizedPeptides, normalizedPeptide);

                    if (normalizedSeqID == clsPSMInfo.UNKNOWN_SEQID)
                    {
                        // New normalized peptide

                        if (!normalizedPeptides.TryGetValue(normalizedPeptide.CleanSequence, out var observedNormalizedPeptides))
                        {
                            // This clean sequence is not yet tracked; add it
                            observedNormalizedPeptides = new List<clsNormalizedPeptideInfo>();
                            normalizedPeptides.Add(normalizedPeptide.CleanSequence, observedNormalizedPeptides);
                        }

                        normalizedSeqID = currentPSM.SeqID;
                        if (normalizedSeqID < 0)
                        {
                            normalizedSeqID = currentPSM.ResultID;
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

                    dctCurrentPeptide.Add("Cleavage_State", ((int)currentPSM.CleavageState).ToString());

                    // Check whether this is a phosphopeptide
                    byte phosphoFlag = 0;
                    foreach (var modification in currentPSM.ModifiedResidues)
                    {
                        if (string.Equals(modification.ModDefinition.MassCorrectionTag, "Phosph", StringComparison.OrdinalIgnoreCase))
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
                    if (currentPSM.Proteins.Any(protein => reKeratinProtein.IsMatch(protein)))
                    {
                        keratinFlag = 1;
                    }
                    dctCurrentPeptide.Add("KeratinPeptide", keratinFlag.ToString());

                    // Store the number of missed cleavages
                    dctCurrentPeptide.Add("MissedCleavages", currentPSM.NumMissedCleavages.ToString());

                    // Check whether this is a peptide from trypsin or trypsinogen
                    byte trypsinFlag = 0;
                    if (currentPSM.Proteins.Any(protein => reTrypsinProtein.IsMatch(protein)))
                    {
                        trypsinFlag = 1;
                    }
                    dctCurrentPeptide.Add("TrypsinPeptide", trypsinFlag.ToString());

                    if (bestPeptideScan < 0 || msgfSpecProb < bestPeptideScore)
                    {
                        dctBestPeptide = dctCurrentPeptide;

                        bestPeptideScan = currentPSM.ScanNumberStart;
                        bestPeptideCharge = currentPSM.Charge;
                        bestPeptideScore = msgfSpecProb;
                    }

                    prevScan = currentPSM.ScanNumberStart;
                    prevCharge = currentPSM.Charge;
                    prevPeptide = string.Copy(currentPeptide);
                }

                if (bestPeptideScan > 0)
                {
                    // Store the cached peptide
                    mDBWrapper.ExecutePHRPInsertCommand(dctBestPeptide, lineNumber);
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
            var modifications = new List<KeyValuePair<string, int>>();

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

        /// <summary>
        /// Checks whether the file has a known extension and thus should be loaded into the database
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="validFileExtensions"></param>
        /// <param name="targetTableName"></param>
        /// <returns></returns>
        public bool IsKnownFileExtension(string filename, string[] validFileExtensions, out string targetTableName)
        {
            targetTableName = string.Empty;

            var baseName = Path.GetFileNameWithoutExtension(filename);
            if (baseName == null)
            {
                return false;
            }

            foreach (var fileExtension in validFileExtensions)
            {
                if (baseName.EndsWith(fileExtension, StringComparison.OrdinalIgnoreCase))
                {
                    // Match found
                    targetTableName = fileExtension;
                    return true;
                }
            }
            return false;
        }

        #region "Error handlers"

        private void DBWrapper_ErrorEvent(string message)
        {
            mSystemLogManager.AddApplicationLogError(message);
        }

        private void mPHRPReader_ErrorEvent(string message, Exception ex)
        {
            mSystemLogManager.AddApplicationLogError("PHRPReader error: " + message);
        }

        private void mPHRPReader_MessageEvent(string message)
        {
            mSystemLogManager.AddApplicationLog(message);
        }

        private void mPHRPReader_WarningEvent(string message)
        {
            mSystemLogManager.AddApplicationLogWarning("PHRPReader warning: " + message);
        }

        #endregion

    }
}
