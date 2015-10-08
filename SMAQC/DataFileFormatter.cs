using System;
using System.Collections.Generic;
using System.IO;

namespace SMAQC
{
	class DataFileFormatter
	{
		//DECLARE VARIABLES
		private readonly List<string> ValidFilesToReFormat = new List<string>();                     //LIST OF FILES THAT ARE VALID TO REFORMAT
		private readonly string[,] FieldList = new string[10, 30];                                   //LIST OF FIELDS

		private string mTempFilePath = "";

		//CONSTRUCTOR
		public DataFileFormatter()
		{
			//SET VALID FILES TO REFORMAT
			ValidFilesToReFormat.Add("ScanStats");
			ValidFilesToReFormat.Add("ScanStatsEx");
			ValidFilesToReFormat.Add("SICstats");
			ValidFilesToReFormat.Add("xt");
			ValidFilesToReFormat.Add("xt_ResultToSeqMap");
			ValidFilesToReFormat.Add("xt_SeqToProteinMap");

			//ScanStats Fields
			FieldList[0, 0] = "Dataset";
			FieldList[0, 1] = "ScanNumber";
			FieldList[0, 2] = "ScanTime";
			FieldList[0, 3] = "ScanType";
			FieldList[0, 4] = "TotalIonIntensity";
			FieldList[0, 5] = "BasePeakIntensity";
			FieldList[0, 6] = "BasePeakMZ";
			FieldList[0, 7] = "BasePeakSignalToNoiseRatio";
			FieldList[0, 8] = "IonCount";
			FieldList[0, 9] = "IonCountRaw";
			FieldList[0, 10] = "ScanTypeName";

			//ScanStatsEx FIELDS
			FieldList[1, 0] = "Dataset";
			FieldList[1, 1] = "ScanNumber";
			FieldList[1, 2] = "Ion Injection Time (ms)";
			FieldList[1, 3] = "Scan Segment";//
			FieldList[1, 4] = "Scan Event";
			FieldList[1, 5] = "Master Index";
			FieldList[1, 6] = "Elapsed Scan Time (sec)";
			FieldList[1, 7] = "Charge State";
			FieldList[1, 8] = "Monoisotopic M/Z";
			FieldList[1, 9] = "MS2 Isolation Width";
			FieldList[1, 10] = "FT Analyzer Settings";
			FieldList[1, 11] = "FT Analyzer Message";
			FieldList[1, 12] = "FT Resolution";
			FieldList[1, 13] = "Conversion Parameter B";
			FieldList[1, 14] = "Conversion Parameter C";
			FieldList[1, 15] = "Conversion Parameter D";
			FieldList[1, 16] = "Conversion Parameter E";
			FieldList[1, 17] = "Collision Mode";
			FieldList[1, 18] = "Scan Filter Text";
			FieldList[1, 19] = "Source Voltage (kV)";
			FieldList[1, 20] = "Source Current (uA)";

			//SICstats FIELDS
			FieldList[2, 0] = "Dataset";
			FieldList[2, 1] = "ParentIonIndex";
			FieldList[2, 2] = "MZ";
			FieldList[2, 3] = "SurveyScanNumber";
			FieldList[2, 4] = "FragScanNumber";
			FieldList[2, 5] = "OptimalPeakApexScanNumber";
			FieldList[2, 6] = "PeakApexOverrideParentIonIndex";
			FieldList[2, 7] = "CustomSICPeak";
			FieldList[2, 8] = "PeakScanStart";
			FieldList[2, 9] = "PeakScanEnd";
			FieldList[2, 10] = "PeakScanMaxIntensity";
			FieldList[2, 11] = "PeakMaxIntensity";
			FieldList[2, 12] = "PeakSignalToNoiseRatio";
			FieldList[2, 13] = "FWHMInScans";
			FieldList[2, 14] = "PeakArea";
			FieldList[2, 15] = "ParentIonIntensity";
			FieldList[2, 16] = "PeakBaselineNoiseLevel";
			FieldList[2, 17] = "PeakBaselineNoiseStDev";
			FieldList[2, 18] = "PeakBaselinePointsUsed";
			FieldList[2, 19] = "StatMomentsArea";
			FieldList[2, 20] = "CenterOfMassScan";
			FieldList[2, 21] = "PeakStDev";
			FieldList[2, 22] = "PeakSkew";
			FieldList[2, 23] = "PeakKSStat";
			FieldList[2, 24] = "StatMomentsDataCountUsed";

			//xt FIELDS
			FieldList[3, 0] = "Result_ID";
			FieldList[3, 1] = "Group_ID";
			FieldList[3, 2] = "Scan";
			FieldList[3, 3] = "Charge";
			FieldList[3, 4] = "Peptide_MH";
			FieldList[3, 5] = "Peptide_Hyperscore";
			FieldList[3, 6] = "Peptide_Expectation_Value_Log(e)";
			FieldList[3, 7] = "Multiple_Protein_Count";
			FieldList[3, 8] = "Peptide_Sequence";
			FieldList[3, 9] = "DeltaCn2";
			FieldList[3, 10] = "y_score";
			FieldList[3, 11] = "y_ions";
			FieldList[3, 12] = "b_score";
			FieldList[3, 13] = "b_ions";
			FieldList[3, 14] = "Delta_Mass";
			FieldList[3, 15] = "Peptide_Intensity_Log(I)";
			FieldList[3, 16] = "DelM_PPM";

			//xt_ResultToSeqMap FIELDS
			FieldList[4, 0] = "Result_ID";
			FieldList[4, 1] = "Unique_Seq_ID";

			//xt_SeqToProteinMap FIELDS
			FieldList[5, 0] = "Unique_Seq_ID";
			FieldList[5, 1] = "Cleavage_State";
			FieldList[5, 2] = "Terminus_State";
			FieldList[5, 3] = "Protein_Name";
			FieldList[5, 4] = "Protein_Expectation_Value_Log(e)";
			FieldList[5, 5] = "Protein_Intensity_Log(I)";

			//CLEAN FIELDS FROM 2 DIM ARRAY
			FieldList = FieldCleaner2d(FieldList);
		}

		//DESTRUCTOR
		~DataFileFormatter()
		{
			//ENSURE TEMP FILE DOES NOT STILL EXIST
			if (!String.IsNullOrEmpty(mTempFilePath) && File.Exists(mTempFilePath))
				ensure_temp_file_removed(mTempFilePath);
		}

		// TempFilePath property
		public string TempFilePath
		{
			get
			{
				return mTempFilePath;
			}
		}


		//THIS FUNCTION CHECKS EACH FILE TO SEE IF IT SHOULD BE RE-FORMATED AND THEN TAKES CARE OF IT
		//RETURNS FALSE == NO REBUILD || TRUE == REBUILD
		public Boolean handleFile(string filename, string dataset)
		{
			//DECLARE VARIABLES
			var ListFieldID = new List<int>();                    //Maps observed column index to desired column index in DB (-1 means do not store the given column in the DB)

			//CHECK IF IS VALID FILE
			var ValidFilesToReFormat_id = is_valid_file_to_reformat(filename, dataset);

			//IS THIS A FILE THAT NEEDS RE-FORMATING?
			if (ValidFilesToReFormat_id >= 0)
			{
				//PAD HASH TABLE WITH POINTER TO CORRECT VALUES
				var numOfColumns = padHashTable(filename, ref ListFieldID, ValidFilesToReFormat_id);

				// OBTAIN A TEMP FILE PATH
				mTempFilePath = Path.GetTempFileName();

				//CALL INTERNAL REBUILD FUNCTION
				rebuildFile(filename, mTempFilePath, numOfColumns, ListFieldID);

				return true;
			}

			//CLEAR HASH TABLE
			ListFieldID.Clear();

			return false;
		}

		//REBUILD FILENAME USING PADDING TO ENSURE ALL FIELDS MATCH UP
		private void rebuildFile(string filename, string save_to_filename, int numOfColumns, List<int> ListFieldID)
		{
			//DECLARE VARIABLES
			var line_num = 0;

			//OPEN FILES USED FOR R/W
			using (var file_read = new StreamReader(filename))
			{
				using (var file_write = new StreamWriter(save_to_filename))
				{
					//LOOP THROUGH EACH LINE
					string line;
					while ((line = file_read.ReadLine()) != null)
					{
						//DECLARE NEW LINE
						var line_temp = "";

						//SPLIT GIVEN DATA FILES BY TAB
						var delimiters = new[] { '\t' };

						//DO SPLIT OPERATION
						var parts = line.Split(delimiters, StringSplitOptions.None);

						//IF COLUMN AND DATA MISMATCH
						if (parts.Length != numOfColumns)
						{
							// Number of columns is not the expected number
							//THIS NORMALLY HAPPENS ON SCANSTATSEX. NOTHING WE CAN DO DUE TO BAD TOOL. IGNORE LINE.

							//NEXT LINE
							continue;
						}

						//READING THE FIRST LINE ... SKIP IT AS NO LONGER NEEDED
						if (line_num == 0)
						{
							//CLEAN FIELDS TO ENSURE CONSISTENCY
							parts = FieldCleaner(parts);

							//INC
							line_num++;

							//NEXT LINE
							//continue;
						}

						//NOW READING DATA LINES [ASSUMING LINE_NUM > 0]

						//LOOP THROUGH EACH PART
						for (var i = 0; i < parts.Length; i++)
						{
							//IF NOT AN ALLOWED FIELD IGNORE
							if (ListFieldID[i] > -1)
							{
								if (line_temp.Length > 0)
									line_temp += "\t";

								//APPEND TO LINE_TEMP
								line_temp += parts[i];

							}

						}

						//WRITE LINE
						file_write.WriteLine(line_temp);

					}
				}
			}
		}

		// This function defines a mapping between the column index in the file vs. the column index to which the data should be written in the database
		/*
		1. IDENTIFIES USING ValidFilesToReFormat_id THE CORRECT [x][] ARRAY
		2. STORES VALUES IN HashFieldID IN FOLLOWING FORMAT, IF ValidFilesToReFormat_id=1
			ASSUME FILE HAS ONLY: 'FT Analyzer Settings', 'Conversion Parameter C', 'Dataset' IN FILE
			1. Find offset for each:: 'FT Analyzer Settings'=10, 'Conversion Parameter C'=14, 'Dataset'=0
			2. store in HashFieldID as [0]=10, [1]=14, [2]=0, ...all others == -1
		*/
		private int padHashTable(string file_to_load, ref List<int> ListFieldID, int ValidFilesToReFormat_id)
		{
			//DECLARE VARIABLES
			int numOfcolumns;

			//OPEN + READ
			using (var file = new StreamReader(file_to_load))
			{
				var line = file.ReadLine();

				if (string.IsNullOrWhiteSpace(line))
					return 0;

				//SPLIT GIVEN DATA FILES BY TAB
				var delimiters = new[] {'\t'};

				//DO SPLIT OPERATION
				var parts = line.Split(delimiters, StringSplitOptions.None);

				//CLEAN FIELDS TO ENSURE CONSISTENCY
				parts = FieldCleaner(parts);

				//SET numOfcolumns
				numOfcolumns = parts.Length;

				//LOOP THROUGH EACH COLUMN
				foreach (var column in parts)
				{
					//SEARCH FOR COLUMN NAME THAT IS FOUND IN OUR FILE LINE
					var index = findIndexOfColumnName(ValidFilesToReFormat_id, column);

					//IF FOUND [NOT -1]
					if (index != -1)
					{
						//Console.WriteLine("STORE i={0} && index={1}", i, index);
						//STORE INDEX OF OUR PART_ID AS INDEX VALUE AS PER FUNCTION REQUIREMENTS [SEE DETAILS ABOVE FUNC NAME]
						ListFieldID.Add(index);
					}
					else
					{
						//NOT FOUND ... ADD -1 to indicate we will skip this column
						//Console.WriteLine("STORE i={0} && index={1}", i, index);
						ListFieldID.Add(-1);
					}
				}

			}

			return numOfcolumns;
		}

		//SEARCHES [id][x] FOR NAME ... IF FOUND RETURNS INDEX ... ELSE -1
		private int findIndexOfColumnName(int ValidFilesToReFormat_id, string name)
		{
			//GET BOUND OF SECOND DIM
			var bound = FieldList.GetLength(1);

			for (var i = 0; i < bound; i++)
			{
				//CHECK TO ENSURE THAT WE ARE NOT NULL
				if (FieldList[ValidFilesToReFormat_id, i] == null)
					break;

				//IF FOUND NAME ... RETURN INDEX
				if (FieldList[ValidFilesToReFormat_id, i].Equals(name))
				{
					return i;
				}
			}

			return -1;
		}

		//ENSURE FILE HAS BEEN DELETED
		private void ensure_temp_file_removed(string filePath)
		{
			//ENSURE TEMP FILE DOES NOT STILL EXIST
			if (File.Exists(filePath))
			{
				File.Delete(filePath);
			}
		}

		//IS THIS A VALID FILE TO REFORMAT [CHECKS ValidFilesToReFormat LIST]
		private int is_valid_file_to_reformat(string filename, string dataset)
		{
			//STEP # 1 GET FILENAME WITHOUT EXTENSION
			filename = Path.GetFileNameWithoutExtension(filename);

			if (string.IsNullOrWhiteSpace(filename))
				return -1;

			//STEP # 3 NOW DO ACTUAL REMOVING OF DATA PREFIX
			filename = filename.Substring(dataset.Length + 1);                  //RETURNS ScanStats, ScanStatsEx, ...

			//LOOP THROUGH ALL VALID FILES THAT WE REFORMAT
			for (var i = 0; i < ValidFilesToReFormat.Count; i++)
			{
				//IF FOUND A MATCH
				if (filename.Equals(ValidFilesToReFormat[i], StringComparison.OrdinalIgnoreCase))
				{
					return i;
				}
			}

			return -1;
		}

		//FIELD CLEANER TO ENSURE DATABASE CONSISTANCY BY REMOVING SPACES, (,), / AND MORE FROM 2 DIM ARRAYS
		private string[,] FieldCleaner2d(string[,] field_array)
		{
			//DECLARE VARIABLES
			var dim1 = field_array.GetLength(0); //[x][]
			var dim2 = field_array.GetLength(1); //[][x]

			//LOOP THROUGH FIRST DIMENSION
			for (var j = 0; j < dim1; j++)
			{
				//LOOP THROUGH SECOND DIMENSION [STORES ALL FIELDS]
				for (var i = 0; i < dim2; i++)
				{
					//IF NULL SKIP
					if (field_array[j, i] == null)
						continue;

					//STEP #1 REMOVE (...)
					var first_index = field_array[j, i].IndexOf(" (");
					var last_index = field_array[j, i].IndexOf(")");

					//IF THERE IS A (...)
					if (first_index > 0 && last_index > 0)
					{
						field_array[j, i] = field_array[j, i].Remove(first_index, last_index - first_index + 1);
					}

					//STEP #2 REPLACE ALL " " WITH "_"
					field_array[j, i] = field_array[j, i].Replace(" ", "_");

					//STEP #3 REPLACE ALL "/" WITH "" [REQUIRED DUE TO THINGS LIKE SCANSTATSEX HAVING M/Z WHEN IT SHOULD BE MZ]
					field_array[j, i] = field_array[j, i].Replace("/", "");
				}
			}

			return field_array;
		}

		//FIELD CLEANER TO ENSURE DATABASE CONSISTANCY BY REMOVING SPACES, (,), / AND MORE
		private string[] FieldCleaner(string[] field_array)
		{
			//DECLARE VARIABLES

			//LOOP THROUGH EACH FIELD
			for (var i = 0; i < field_array.Length; i++)
			{
				//STEP #1 REMOVE (...)
				var first_index = field_array[i].IndexOf(" (");
				var last_index = field_array[i].IndexOf(")");

				//IF THERE IS A (...)
				if (first_index > 0 && last_index > first_index)
				{
					field_array[i] = field_array[i].Remove(first_index, last_index - first_index + 1);
				}

				//STEP #2 REPLACE ALL " " WITH "_"
				field_array[i] = field_array[i].Replace(" ", "_");

				//STEP #3 REPLACE ALL "/" WITH "" [REQUIRED DUE TO THINGS LIKE SCANSTATSEX HAVING M/Z WHEN IT SHOULD BE MZ]
				field_array[i] = field_array[i].Replace("/", "");
			}

			return field_array;
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
	}
}