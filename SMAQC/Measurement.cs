using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace SMAQC
{
	class Measurement
	{
		//DECLARE VARIABLES
		private DBWrapper m_DBInterface;															// CREATE DB INTERFACE OBJECT
		private Hashtable m_MeasurementHash = new Hashtable();                                      // HASH TABLE FOR MEASUREMENTS
		private int m_Random_ID;                                                                    // RANDOM ID FOR TEMP TABLES
		private Dictionary<string, double> m_ResultsStorage = new Dictionary<string, double>();		// SOME MEASUREMENTS HAVE DATA REQUIRED BY OTHERS ... WILL BE STORED HERE

		// Properties
		public bool UsingPHRP { get; set; }

		//CONSTRUCTOR
		public Measurement(int random_id, ref DBWrapper DBInterface)
		{
			//CREATE CONNECTIONS
			this.m_Random_ID = random_id;
			this.m_DBInterface = DBInterface;
			this.UsingPHRP = false;
		}

		//DESTRUCTOR
		~Measurement()
		{
			//CLEAR HASHTABLE AND DICTIONARY
			clearStorage();
		}

		/// <summary>
		/// Add (or update) entryName in mResultsStorage
		/// </summary>
		/// <param name="name"></param>
		/// <param name="value"></param>
		protected void AddUpdateResultsStorage(string entryName, double value)
		{
			if (m_ResultsStorage.ContainsKey(entryName))
				m_ResultsStorage[entryName] = value;
			else
				m_ResultsStorage.Add(entryName, value);
		}

		//THIS CALLS OUR HASH TABLE CLEARER. WHICH WE NEED BETWEEN DATASETS AS IT IS NO LONGER NEEDED
		public void clearStorage()
		{
			//CLEAR HASHTABLE STORAGE
			m_MeasurementHash.Clear();
			m_ResultsStorage.Clear();
		}

		protected double ComputeMedian(List<double> values)
		{
			if (values.Count == 0)
				return 0;

			if (values.Count == 1)
				return values[0];

			// Assure the list is sorted
			values.Sort();

			//IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NO NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
			if (values.Count % 2 == 1)
			{
				//IF ODD
				int pos = (values.Count / 2);
				return values[pos];
			}
			else
			{
				//IF EVEN
				int pos = (values.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
				return (values[pos] + values[pos + 1]) / 2;
			}
		}
		protected double GetStoredValue(string entryName, double valueIfMissing)
		{
			double value;

			if (m_ResultsStorage.TryGetValue(entryName, out value))
				return value;
			else
				return valueIfMissing;
		}

		protected int GetStoredValueInt(string entryName, int valueIfMissing)
		{
			double value = GetStoredValue(entryName, valueIfMissing);
			return (int)value;
		}

		protected Dictionary<int, int> GetResultIDToSeqIDTable()
		{
			Dictionary<int, int> ResultID_to_Unique_Seq_ID_Table = new Dictionary<int, int>();
			int resultID;
			int seqID;

			string[] fields_n2 = { "Result_ID", "Unique_Seq_ID" };
			Hashtable htValues = new Hashtable();                                        //HASH TABLE FOR MEASUREMENTS

			m_DBInterface.setQuery("SELECT * FROM temp_xt_resulttoseqmap WHERE temp_xt_resulttoseqmap.random_id=" + m_Random_ID + ";");
			m_DBInterface.initReader();
			while ((m_DBInterface.readLines(fields_n2, ref htValues)) && (htValues.Count > 0))
			{
				if (int.TryParse(htValues["Result_ID"].ToString(), out resultID))
				{
					if (!ResultID_to_Unique_Seq_ID_Table.ContainsKey(resultID))
					{
						if (int.TryParse(htValues["Unique_Seq_ID"].ToString(), out seqID))
							ResultID_to_Unique_Seq_ID_Table.Add(resultID, seqID);
					}
				}
			}

			return ResultID_to_Unique_Seq_ID_Table;
		}

		protected Dictionary<int, int> GetSeqIDToCleavageStateTable()
		{
			Dictionary<int, int> Seq_ID_to_Cleavage_State_Table = new Dictionary<int, int>();
			int seqID;
			short cleavageState;

			string[] fields_n1 = { "Unique_Seq_ID", "Cleavage_State" };
			Hashtable htValues = new Hashtable();                                        //HASH TABLE FOR MEASUREMENTS

			m_DBInterface.setQuery("SELECT Unique_Seq_ID, MAX(Cleavage_State) AS Cleavage_State FROM `temp_xt_seqtoproteinmap` WHERE temp_xt_seqtoproteinmap.random_id=" + m_Random_ID + " GROUP BY Unique_Seq_ID;");
			m_DBInterface.initReader();
			while ((m_DBInterface.readLines(fields_n1, ref htValues)) && (htValues.Count > 0))
			{
				if (int.TryParse(htValues["Unique_Seq_ID"].ToString(), out seqID))
				{
					if (!Seq_ID_to_Cleavage_State_Table.ContainsKey(seqID))
					{
						if (short.TryParse(htValues["Cleavage_State"].ToString(), out cleavageState))
							Seq_ID_to_Cleavage_State_Table.Add(seqID, cleavageState);
					}
				}
			}

			return Seq_ID_to_Cleavage_State_Table;
		}

		/// <summary>
		/// C-1A: Fraction of peptides identified more than 4 minutes earlier than the chromatographic peak apex
		/// </summary>
		/// <returns></returns>
		public string C_1A()
		{
			bool countTailingPeptides = false;
			return C_1_Shared(countTailingPeptides);

		}

		/// <summary>
		/// C-1B: Fraction of peptides identified more than 4 minutes later than the chromatographic peak apex
		/// </summary>
		/// <returns></returns>
		public string C_1B()
		{
			bool countTailingPeptides = true;
			return C_1_Shared(countTailingPeptides);
		}

		/// <summary>
		/// Counts the number of peptides identified more than 4 minutes earlier or more than 4 minutes later than the chromatographic peak apex
		/// </summary>
		/// <param name="countTailingPeptides">False means to count early eluting peptides; True means to count late-eluting peptides</param>
		/// <returns></returns>
		protected string C_1_Shared(bool countTailingPeptides)
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.`Peptide_Expectation_Value_Log`, t1.FragScanNumber, t1.OptimalPeakApexScanNumber,"
			+ "temp_scanstats.ScanTime as ScanTime1, t2.ScanTime as ScanTime2 "
			+ "FROM temp_xt, temp_scanstats, temp_sicstats as t1 "
			+ "LEFT JOIN temp_scanstats as t2 on t1.OptimalPeakApexScanNumber=t2.ScanNumber "
			+ "WHERE temp_xt.Scan = t1.FragScanNumber "
			+ "AND temp_xt.Scan = temp_scanstats.ScanNumber "
			+ "AND temp_xt.random_id=" + m_Random_ID + " "
			+ "AND temp_scanstats.random_id=" + m_Random_ID + " "
			+ "AND t1.random_id=" + m_Random_ID + " "
			+ "AND t2.random_id=" + m_Random_ID + " "
			+ "ORDER BY Scan;");

			int difference_sum = 0;                                                             //FOR COLUMN J
			int valid_rows = 0;                                                                 //FOR COLUMN K
			double answer;

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Expectation_Value_Log", "FragScanNumber", "OptimalPeakApexScanNumber", "ScanTime1", "ScanTime2" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//IF LOG(E) <= -2 ... CALCULATE DIFFERENCE [COLUMN C]
				if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					//CALC DIFFERENCE [COLUMN C]
					double temp_difference;
					if (countTailingPeptides)
						temp_difference = (Convert.ToDouble(m_MeasurementHash["ScanTime1"]) - Convert.ToDouble(m_MeasurementHash["ScanTime2"]));
					else
						temp_difference = (Convert.ToDouble(m_MeasurementHash["ScanTime2"]) - Convert.ToDouble(m_MeasurementHash["ScanTime1"]));

					//IF DIFFERENCE >= 4 [COLUMN I]
					if (temp_difference >= 4.00)
					{
						difference_sum += 1;    //ADD 1 TO TOTAL
					}

					//SINCE VALID ROW ... INC [ONLY IF COLUMN C == 1]
					valid_rows++;
				}
			}

			//CALCULATE SOLUTION
			if (valid_rows > 0)
			{
				answer = difference_sum / (double)valid_rows;
				return answer.ToString("0.000000");
			}
			else
				return String.Empty;
		}

		/// <summary>
		/// C-2A: Time period over which 50% of peptides are identified
		/// We also cache various scan numbers associated with filter-passing peptides
		/// </summary>
		/// <returns></returns>
		public string C_2A()
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.`Peptide_Expectation_Value_Log`, t1.FragScanNumber as ScanNumber,"
				+ "temp_scanstats.ScanTime as ScanTime1 "
				+ "FROM temp_xt, temp_scanstats, temp_sicstats as t1 "
				+ "WHERE temp_xt.Scan = t1.FragScanNumber "
				+ "AND temp_xt.Scan = temp_scanstats.ScanNumber "
				+ "AND temp_xt.random_id=" + m_Random_ID + " "
				+ "AND temp_scanstats.random_id=" + m_Random_ID + " "
				+ "AND t1.random_id=" + m_Random_ID + " "
				+ "ORDER BY Scan;");

			// This list stores scan numbers and elution times for filter-passing peptides; duplicate scans are not allowed
			SortedList<int, double> lstFilterPassingPeptides = new SortedList<int, double>();

			int scanNumber;
			double scanTime;

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Expectation_Value_Log", "ScanNumber", "ScanTime1" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					// FILTER-PASSING PEPTIDE; Append to the dictionary
					if (int.TryParse(m_MeasurementHash["ScanNumber"].ToString(), out scanNumber))
					{
						if (double.TryParse(m_MeasurementHash["ScanTime1"].ToString(), out scanTime))
						{

							if (!lstFilterPassingPeptides.ContainsKey(scanNumber))
							{
								lstFilterPassingPeptides.Add(scanNumber, scanTime);
							}
						}
					}

				}
			}

			int index25th = -1;
			int index75th = -1;

			int C2AScanStart = 0;
			int C2AScanEnd = 0;

			double C2AScanTimeStart = 0;
			double C2AScanTimeEnd = 0;

			if (lstFilterPassingPeptides.Count > 0)
			{

				//DETERMINE THE SCAN NUMBERS AT WHICH THE 25TH AND 75TH PERCENTILES ARE LOCATED
				index25th = (int)(lstFilterPassingPeptides.Count * 0.25);
				index75th = (int)(lstFilterPassingPeptides.Count * 0.75);

				if (index25th >= lstFilterPassingPeptides.Count)
					index25th = lstFilterPassingPeptides.Count - 1;

				if (index75th >= lstFilterPassingPeptides.Count)
					index75th = lstFilterPassingPeptides.Count - 1;

				if (index75th < index25th)
					index75th = index25th;
			}

			if (index25th >= 0 && index25th < lstFilterPassingPeptides.Count && index75th < lstFilterPassingPeptides.Count)
			{
				C2AScanStart = lstFilterPassingPeptides.Keys[index25th];
				C2AScanEnd = lstFilterPassingPeptides.Keys[index75th];

				C2AScanTimeStart = lstFilterPassingPeptides.Values[index25th];
				C2AScanTimeEnd = lstFilterPassingPeptides.Values[index75th];
			}

			if (lstFilterPassingPeptides.Count > 0)
			{
				// ADD TO GLOBAL HASH TABLE FOR USE WITH MS_2A/B
				// SCAN_FIRST_FILTER_PASSING_PEPTIDE is the scan number of the first filter-passing peptide
				AddUpdateResultsStorage("SCAN_FIRST_FILTER_PASSING_PEPTIDE", lstFilterPassingPeptides.Keys.Min());
			}

			// CACHE THE SCAN NUMBERS AT THE START AND END OF THE INTEQUARTILE REGION
			AddUpdateResultsStorage("C_2A_REGION_SCAN_START", C2AScanStart);
			AddUpdateResultsStorage("C_2A_REGION_SCAN_END", C2AScanEnd);

			double answer = C2AScanTimeEnd - C2AScanTimeStart;

			//STORE IN GLOBAL HASH TABLE FOR C_2B
			AddUpdateResultsStorage("C_2A_TIME_MINUTES", answer);

			return answer.ToString("0.0000");
		}


		/// <summary>
		/// C-2B: Rate of peptide identification during C-2A
		/// </summary>
		/// <returns></returns>
		public string C_2B()
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.`Peptide_Expectation_Value_Log`, t1.FragScanNumber as ScanNumber,"
				+ "temp_scanstats.ScanTime as ScanTime1 "
				+ "FROM temp_xt, temp_scanstats, temp_sicstats as t1 "
				+ "WHERE temp_xt.Scan = t1.FragScanNumber "
				+ "AND temp_xt.Scan = temp_scanstats.ScanNumber "
				+ "AND temp_xt.random_id=" + m_Random_ID + " "
				+ "AND temp_scanstats.random_id=" + m_Random_ID + " "
				+ "AND t1.random_id=" + m_Random_ID + " "
				+ "ORDER BY Scan;");

			// This list keeps track of the scan numbers already processed so that we can avoid double-counting a scan number
			SortedSet<int> lstScansWithFilterPassingIDs = new SortedSet<int>();

			int scanNumber;

			double timeMinutesC2A = GetStoredValue("C_2A_TIME_MINUTES", 0);
			int scanStartC2A = GetStoredValueInt("C_2A_REGION_SCAN_START", 0);
			int scanEndC2A = GetStoredValueInt("C_2A_REGION_SCAN_END", 0);

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Expectation_Value_Log", "ScanNumber", "ScanTime1" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					// FILTER-PASSING PEPTIDE; Append to the dictionary
					if (int.TryParse(m_MeasurementHash["ScanNumber"].ToString(), out scanNumber))
					{
						if (scanNumber >= scanStartC2A && scanNumber <= scanEndC2A && !lstScansWithFilterPassingIDs.Contains(scanNumber))
						{
							lstScansWithFilterPassingIDs.Add(scanNumber);
						}
					}

				}
			}

			string answerText = String.Empty;

			if (timeMinutesC2A > 0)
			{
				double answer = lstScansWithFilterPassingIDs.Count / timeMinutesC2A;

				//WE NOW HAVE RESULT ... NOW ROUND IT TO 4th DIGIT
				answerText = answer.ToString("0.0000");

			}

			return answerText;
		}

		/// <summary>
		/// C-3A: Median peak width for all peptides
		/// </summary>
		/// <returns></returns>
		public string C_3A()
		{
			double startScanRelative = 0;
			double endScanRelative = 1;

			//COMPUTE RESULT
			return ComputeMedianPeakWidth(startScanRelative, endScanRelative);
		}

		/// <summary>
		/// C-3B: Median peak width during middle 50% of separation
		/// </summary>
		/// <returns></returns>
		public string C_3B()
		{
			double startScanRelative = 0.25;
			double endScanRelative = 0.75;

			//COMPUTE RESULT
			return ComputeMedianPeakWidth(startScanRelative, endScanRelative);
		}

		/// <summary>
		/// C-4A: Median peak width during first 10% of separation
		/// </summary>
		/// <returns></returns>
		public string C_4A()
		{
			double startScanRelative = 0.00;
			double endScanRelative = 0.10;

			//COMPUTE RESULT
			return ComputeMedianPeakWidth(startScanRelative, endScanRelative);
		}

		/// <summary>
		/// C-4B: Median peak width during last 10% of separation
		/// </summary>
		/// <returns></returns>
		public string C_4B()
		{
			double startScanRelative = 0.90;
			double endScanRelative = 1.00;

			//COMPUTE RESULT
			return ComputeMedianPeakWidth(startScanRelative, endScanRelative);
		}

		/// <summary>
		/// C-4C: Median peak width during middle 10% of separation
		/// </summary>
		/// <returns></returns>
		public string C_4C()
		{
			double startScanRelative = 0.45;
			double endScanRelative = 0.55;

			//COMPUTE RESULT
			return ComputeMedianPeakWidth(startScanRelative, endScanRelative);
		}


		protected string ComputeMedianPeakWidth(double startScanRelative, double endScanRelative)
		{
			//DECLARE HASH TABLES
			List<int> bestscan = new List<int>();                           //STORE Best Scan Results
			Hashtable fragscannumber = new Hashtable();                     //STORE FRAG SCAN NUMBERS
			Hashtable fwhminscans = new Hashtable();                        //STORE FWHMIN SCANS
			Hashtable optimalpeakapexscannumber = new Hashtable();          //STORE OPTIMAL PEAK APEX SCAN NUMBERS
			Hashtable scantime = new Hashtable();                           //STORE TIME
			List<double> result = new List<double>();                       //STORE RESULT FOR FINAL CALCULATION
			int i;                                                          //TEMP POSITION VARIABLE
			int running_sum = 1;                                            //STORE RUNNING SUM STARTING AT 1
			string prv_Charge = "";                                         //INIT PREV CHARGE TO BLANK [REQUIRED FOR COMPARISON]
			string prv_Peptide_Sequence = "";                               //INIT PREV PEPTIDE SEQUENCE TO BLANK [REQUIRED FOR COMPARISON]
			string prev_Best_Evalue = "";                                   //INIT PREV BEST EVALUE TO BLANK [REQUIRED FOR COMPARISON]
			double median = 0.00;                                           //INIT MEDIAN

			//SET DB QUERY [REQUIRED TO SORT BY PEPTIDE SEQUENCE]
			m_DBInterface.setQuery("SELECT Scan, Charge, Peptide_Expectation_Value_Log,Peptide_Sequence FROM `temp_xt` "
			+ "WHERE temp_xt.random_id=" + m_Random_ID + " ORDER BY Peptide_Sequence,Charge,Scan,Scan");

			//DECLARE FIELDS TO READ FROM
			string[] fields1 = { "Scan", "Charge", "Peptide_Expectation_Value_Log", "Peptide_Sequence" };

			//INIT READER
			m_DBInterface.initReader();

			//FETCH COLUMNS Q,R NOW

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields1, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//FIND COLUMN Q
				string Best_Evalue = "";

				//IF PREVIOUS PEPTIDE SEQUENCES == EACH OTHER && PREVIOUS CHARGES == EACH OTHER
				if (prv_Peptide_Sequence.Equals(Convert.ToString(m_MeasurementHash["Peptide_Sequence"])) && prv_Charge.Equals(Convert.ToString(m_MeasurementHash["Charge"])))
				{

					//TAKE MIN [EITHER PREVIOUS BEST EVALUE OR CURRENT PEPTIDE EXPECTATION VALUE
					if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) > Convert.ToDouble(prev_Best_Evalue))
					{
						Best_Evalue = prev_Best_Evalue;
					}
					else
					{
						Best_Evalue = Convert.ToString(m_MeasurementHash["Peptide_Expectation_Value_Log"]);
					}
				}
				else
				{
					Best_Evalue = Convert.ToString(m_MeasurementHash["Peptide_Expectation_Value_Log"]);
				}

				//NOW FIND COLUMN R IF COLUMN U IS == TRUE
				if (Best_Evalue.Equals(Convert.ToString(m_MeasurementHash["Peptide_Expectation_Value_Log"])))
				{
					//WE ARE NOW == TRUE FOR THIS COLUMN ... AS TRUE WE ADD THIS TO OUR HASH TABLE/WHATEVER

					//[ADD HERE]
					bestscan.Add(Convert.ToInt32(m_MeasurementHash["Scan"]));
				}

				//UPDATE PREVIOUS VALUES FOR NEXT LOOP
				prv_Charge = Convert.ToString(m_MeasurementHash["Charge"]);
				prv_Peptide_Sequence = Convert.ToString(m_MeasurementHash["Peptide_Sequence"]);
				prev_Best_Evalue = Best_Evalue;
			}

			//NOW SORT OUR VALUES SO THEY ARE IN THE CORRECT ORDER-
			bestscan.Sort();

			//SET DB QUERY
			m_DBInterface.setQuery("SELECT FragScanNumber, FWHMInScans, OptimalPeakApexScanNumber FROM `temp_sicstats` WHERE temp_sicstats.random_id=" + m_Random_ID + "");

			//DECLARE FIELDS TO READ FROM
			string[] fields2 = { "FragScanNumber", "FWHMInScans", "OptimalPeakApexScanNumber" };

			//INIT READER
			m_DBInterface.initReader();

			//FETCH COLUMNS D-F

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields2, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//ADD VALUES TO OUR TEMP HASH TABLES
				fragscannumber.Add(m_MeasurementHash["FragScanNumber"], m_MeasurementHash["FragScanNumber"]);
				fwhminscans.Add(m_MeasurementHash["FragScanNumber"], m_MeasurementHash["FWHMInScans"]);
				optimalpeakapexscannumber.Add(m_MeasurementHash["FragScanNumber"], m_MeasurementHash["OptimalPeakApexScanNumber"]);
			}

			//SET DB QUERY
			m_DBInterface.setQuery("SELECT temp_scanstats.ScanNumber, temp_scanstats.ScanTime FROM `temp_scanstats` WHERE temp_scanstats.random_id=" + m_Random_ID + "");

			//DECLARE FIELDS TO READ FROM
			string[] fields3 = { "ScanNumber", "ScanTime" };

			//INIT READER
			m_DBInterface.initReader();

			//FETCH COLUMNS H-I
			i = 1;
			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields3, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//ADD TO SCANTIME HASH TABLE
				scantime.Add(Convert.ToString(i), m_MeasurementHash["ScanTime"]);

				//INCREMENT I POSITION
				i++;
			}

			//NOW START THE ACTUAL MEASUREMENT CALCULATION

			//LOOP THROUGH BESTSCAN
			for (i = 0; i < bestscan.Count; i++)
			{
				//FIND INDEX + OPTIMAL PEAK APEX SCAN +- FWHMIN FOR EACH RESULT [COLUMNS: M,O]
				string index = Convert.ToString(bestscan[i]);
				int OptimalPeakApexScanMinusFWHMIN = Convert.ToInt32(optimalpeakapexscannumber[index]) - Convert.ToInt32(Math.Ceiling(Convert.ToDouble(fwhminscans[index]) / 2));
				int OptimalPeakApexScanPlusFWHMIN = Convert.ToInt32(optimalpeakapexscannumber[index]) + Convert.ToInt32(Math.Ceiling(Convert.ToDouble(fwhminscans[index]) / 2));

				//FIND OTHER COLUMNS [N,P, Q,R,T]
				double start_time = Convert.ToDouble(scantime[Convert.ToString(OptimalPeakApexScanMinusFWHMIN)]);
				double end_time = Convert.ToDouble(scantime[Convert.ToString(OptimalPeakApexScanPlusFWHMIN)]);
				double end_minus_start = end_time - start_time;
				double end_minus_start_in_secs = end_minus_start * 60;
				double running_percent = (double)running_sum / (double)bestscan.Count;

				//CHECK FOR VALID RANGE DATA THEN ADD TO OUR RESULTS
				if (running_percent >= startScanRelative && running_percent <= endScanRelative)
				{
					//WE ARE WITHING OUR VALID RANGE ... SO ADD end_minus_start_in_secs TO OUR LIST [COLUMN U]
					result.Add(end_minus_start_in_secs);
				}

				//INCREMENT RUNNING SUM [COLUMN S]
				running_sum++;
			}

			string resultText = String.Empty;

			if (result.Count > 0)
			{
				//CALCULATE MEDIAN
				median = ComputeMedian(result);

				//WE NOW HAVE RESULT ... NOW ROUND IT TO 2ND DIGIT
				resultText = median.ToString("0.00");

				//IMPLEMENTATION NOTES
				/*
				 * result.Count == # OF U COLUMN VALID RESULTS
				 * Console.WriteLine("MEDIAN={0} -- {1} [POS={2}]", result[pos], result[pos + 1], pos); == HELPFUL FOR DEBUGGING
				*/
			}

			//RETURN RESULT
			return resultText;
		}

		/// <summary>
		/// DS-1A: Count of peptides with one spectrum / count of peptides with two spectra
		/// </summary>
		/// <returns></returns>
		public string DS_1A()
		{
			int num_of_1_peptides;	                                                            //RUNNING COUNT FOR COLUMN J
			int num_of_2_peptides;                                                              //RUNNING COUNT FOR COLUMN K
			int num_of_3_peptides;                                                              //RUNNING COUNT FOR COLUMN L
			double result = 0;																	//SOLUTION

			DS_1_Shared(out num_of_1_peptides, out num_of_2_peptides, out num_of_3_peptides);

			//NOW CALCULATE DS_1A
			//RETURN 0 IF NUM_OF_2 EQUALS 0
			if (num_of_2_peptides > 0)
			{
				result = num_of_1_peptides / (double)num_of_2_peptides;
			}

			return result.ToString("0.000");
		}

		/// <summary>
		/// DS-1B: Count of peptides with two spectra / count of peptides with three spectra
		/// </summary>
		/// <returns></returns>
		public string DS_1B()
		{
			int num_of_1_peptides;	                                                            //RUNNING COUNT FOR COLUMN J
			int num_of_2_peptides;                                                              //RUNNING COUNT FOR COLUMN K
			int num_of_3_peptides;                                                              //RUNNING COUNT FOR COLUMN L
			double result = 0;																		//SOLUTION

			DS_1_Shared(out num_of_1_peptides, out num_of_2_peptides, out num_of_3_peptides);

			//NOW CALCULATE DS_1B
			//RETURN 0 IF NUM_OF_3 EQUALS 0
			if (num_of_3_peptides > 0)
			{
				result = num_of_2_peptides / (double)num_of_3_peptides;
			}

			return result.ToString("0.000");

		}

		protected void DS_1_Shared(out int num_of_1_peptides, out int num_of_2_peptides, out int num_of_3_peptides)
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT Peptide_Expectation_Value_Log,Peptide_Sequence,Scan "
				+ "FROM `temp_xt` "
				+ "WHERE temp_xt.random_id=" + m_Random_ID + " "
				+ "ORDER BY Peptide_Sequence,Scan;");

			int running_count = 0;                                                                  //RUNNING COUNT FOR COLUMN F
			num_of_1_peptides = 0;																	//RUNNING COUNT FOR COLUMN J
			num_of_2_peptides = 0;																	//RUNNING COUNT FOR COLUMN K
			num_of_3_peptides = 0;																	//RUNNING COUNT FOR COLUMN L
			Boolean FILTER;                                                                         //FILTER STATUS FOR COLUMN E
			int i = 0;	                                                                            //TEMP POSITION
			Hashtable Peptide_Exp_Value_Log = new Hashtable();                                      //STORE Peptide_Expectation_Value_Log NUMBERS
			Hashtable Peptide_Sequence = new Hashtable();                                           //STORE Peptide Sequence NUMBERS
			Hashtable Scan = new Hashtable();                                                       //STORE SCAN NUMBERS
			Hashtable RunningCountTable = new Hashtable();                                          //STORE RUNNING COUNT'S IN A HASH TABLE FOR LATER ACCES
			string prv_Peptide_Sequence = "";                                                       //INIT PREV PEPTIDE SEQUENCE TO BLANK [REQUIRED FOR COMPARISON]
			int prv_running_count = 0;                                                              //INIT PREV RUNNING COUNT TO 0 [REQUIRED FOR COMPARISON]
			string prv_highest_filtered_log = "";                                                   //INIT PREV HIGHEST FILTERED LOG TO BLANK [REQUIRED FOR COMPARISON]
			int prv_peptide_count = 1;                                                              //INIT PREV PEPTIDE_COUNT TO BE 1

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Peptide_Expectation_Value_Log", "Peptide_Sequence", "Scan" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//ADD TO HASH TABLES
				Peptide_Exp_Value_Log.Add(i, m_MeasurementHash["Peptide_Expectation_Value_Log"]);
				Peptide_Sequence.Add(i, m_MeasurementHash["Peptide_Sequence"]);
				Scan.Add(i, m_MeasurementHash["Scan"]);

				//INCREMENT i
				i++;
			}

			//BUILD RUNNING COUNT TABLE
			for (i = 0; i < Peptide_Exp_Value_Log.Count; i++)
			{
				//RESET FILTER STATUS
				FILTER = false;

				//CALCULATE COLUMN E [TRUE/FALSE]
				if (Convert.ToDouble(Peptide_Exp_Value_Log[i]) < -2)
				{
					//Console.WriteLine("A");
					FILTER = true;
				}

				//CALCULATE RUNNING COUNT + ADD TO RUNNING COUNT TABLE
				if (prv_Peptide_Sequence.Equals(Convert.ToString(Peptide_Sequence[i])))
				{
					//PREVIOUS RUNNING COUNT IS USED
					RunningCountTable.Add(i, running_count);
				}
				else
				{
					//IF FILTER == FALSE
					if (FILTER == false)
					{
						running_count++;//INCREMENT BY ONE

						//ADD CURRENT RUNNING COUNT
						RunningCountTable.Add(i, running_count);
					}
					else
					{
						//ADD CURRENT RUNNING COUNT
						RunningCountTable.Add(i, running_count);
					}
				}

				//UPDATE PREVIOUS RESULT VARIABLES
				prv_Peptide_Sequence = Convert.ToString(Peptide_Sequence[i]);
			}

			//RESETS PREV PEPTIDE SEQUENCE
			prv_Peptide_Sequence = "";

			//CALCULATE EVERYTHING ELSE
			for (i = 0; i < Peptide_Exp_Value_Log.Count; i++)
			{
				//RESET FILTER STATUS
				FILTER = false;
				string highest_filtered_log = "";
				string filtered_log = "";
				int current_peptide_count = 0;

				//CALCULATE COLUMN E [TRUE/FALSE]
				if (Convert.ToDouble(Peptide_Exp_Value_Log[i]) < -2)
				{
					FILTER = true;
				}

				//CALCULATE HIGHEST FILTERED LOG
				if (FILTER == true)
				{
					//Console.WriteLine("TRUE");
					if (prv_highest_filtered_log.Equals(""))
					{
						//GO WITH CURRENT RESULT
						highest_filtered_log = Convert.ToString(Peptide_Exp_Value_Log[i]);
					}
					else if (Convert.ToDouble(prv_highest_filtered_log) > Convert.ToDouble(Peptide_Exp_Value_Log[i]))
					{
						//GO WITH LOWER
						highest_filtered_log = Convert.ToString(Peptide_Exp_Value_Log[i]);
					}
					else
					{
						//GO WITH HIGHER
						highest_filtered_log = Convert.ToString(prv_highest_filtered_log);
					}
				}
				else
				{
					//IF PREV RUNNING COUNT == CURRENT RUNNING COUNT
					if (prv_running_count == Convert.ToInt32(RunningCountTable[i]))
					{
						//Console.WriteLine("D");
						//GO WITH PREVIOUS HIGHEST FILTERED LOG
						highest_filtered_log = Convert.ToString(prv_highest_filtered_log);
					}
					else
					{
						//Console.WriteLine("E");
						//SET TO BLANK
						highest_filtered_log = "";
					}

				}

				//NOW CALCULATE FILTERED LOG
				if (Convert.ToInt32(RunningCountTable[i]) == Convert.ToInt32(RunningCountTable[i + 1]))
				{
					//SET FILTERED LOG TO ""
					filtered_log = "";
				}
				else
				{
					//RUNNING TABLE COUNT IS NOT EQUAL SO USE HIGHEST FILTERED LOG
					filtered_log = highest_filtered_log;
				}

				//NOW COUNT # OF PEPTIDES
				if ((Convert.ToInt32(RunningCountTable[i]) == Convert.ToInt32(RunningCountTable[i - 1])) && FILTER == true)
				{
					//SET CURRENT PEPTIDE COUNT == PREV COUNT + 1
					current_peptide_count = prv_peptide_count + 1;
				}
				else
				{
					//OTHERWISE
					if (Convert.ToInt32(RunningCountTable[i]) == Convert.ToInt32(RunningCountTable[i - 1]))
					{
						if (FILTER == true)
						{
							//SET CURRENT PEPTIDE COUNT == PREV COUNT + 1
							current_peptide_count = prv_peptide_count + 1;
						}
						else
						{
							//SET CURRENT PEPTIDE COUNT == PREV COUNT
							current_peptide_count = prv_peptide_count;
						}
					}
					else
					{
						if (FILTER == true)
						{
							//SET CURRENT PEPTIDE COUNT TO 1
							current_peptide_count = 1;
						}
						else
						{
							//SET CURRENT PEPTIDE COUNT TO 0
							current_peptide_count = 0;
						}
					}
				}

				//CALCULATE # {1,2,3} PEPTIDE COUNTS
				if (current_peptide_count == 1 && !filtered_log.Equals(""))
				{
					num_of_1_peptides++;
				}
				else if (current_peptide_count == 2 && !filtered_log.Equals(""))
				{
					num_of_2_peptides++;
				}
				else if (current_peptide_count == 3 && !filtered_log.Equals(""))
				{
					num_of_3_peptides++;
				}

				//UPDATE PREVIOUS RESULT VARIABLES
				prv_Peptide_Sequence = Convert.ToString(Peptide_Sequence[i]);
				prv_running_count = Convert.ToInt32(RunningCountTable[i]);
				prv_highest_filtered_log = Convert.ToString(highest_filtered_log);
				prv_peptide_count = current_peptide_count;
			}

		}

		/// <summary>
		/// DS-2A: Number of MS1 scans taken over middle 50% of separation
		/// </summary>
		/// <returns></returns>
		public string DS_2A()
		{
			int msLevel = 1;
			return DS_2_Shared(msLevel).ToString();
		}

		/// <summary>
		/// DS-2B: Number of MS2 scans taken over middle 50% of separation
		/// </summary>
		/// <returns></returns>
		public string DS_2B()
		{
			int msLevel = 2;
			return DS_2_Shared(msLevel).ToString();
		}

		protected int DS_2_Shared(int msLevel)
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT ScanNumber, ScanType "
				+ "FROM `temp_scanstats` "
				+ "WHERE temp_scanstats.random_id=" + m_Random_ID + " "
				+ "ORDER BY ScanNumber;");

			//DECLARE VARIABLES
			int scanStartC2A = GetStoredValueInt("C_2A_REGION_SCAN_START", 0);
			int scanEndC2A = GetStoredValueInt("C_2A_REGION_SCAN_END", 0);

			int intScanCount = 0;
			int scanNumber;
			int scanType;

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "ScanNumber", "ScanType" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				scanNumber = Convert.ToInt32(m_MeasurementHash["ScanNumber"]);
				scanType = Convert.ToInt32(m_MeasurementHash["ScanType"]);

				//IF IS WITHIN RANGE
				if (scanType == msLevel && scanNumber >= scanStartC2A && scanNumber <= scanEndC2A)
				{
					intScanCount++;
				}
			}

			return intScanCount;
		}

		/// <summary>
		/// IS-2: Median precursor m/z for all peptides
		/// </summary>
		/// <returns></returns>
		public string IS_2()
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT Scan,Peptide_Expectation_Value_Log,Peptide_MH,Charge "
				+ "FROM `temp_xt` "
				+ "WHERE temp_xt.random_id=" + m_Random_ID + " "
				+ ";");

			//DECLARE VARIABLES
			double MINUS_CONSTANT = 1.00727649;                                                 //REQUIRED CONSTANT TO SUBTRACT BY
			List<double> MZ_List = new List<double>();                                          //MZ LIST
			List<double> MZ_Final;																//MZ Final List
			Dictionary<double, int> tempDict = new Dictionary<double, int>();                   //TEMP ... TO REMOVE DUPLICATES
			double median = 0.00;                                                               //STORE MEDIAN

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Peptide_MH", "Charge" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//IF PEPTIDE EXP VALUE LOG <= -2 SET FILTER TO TRUE
				if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					//COMPUTE MZ VALUE
					double temp_mz = (Convert.ToDouble(m_MeasurementHash["Peptide_MH"]) - MINUS_CONSTANT) / (Convert.ToDouble(m_MeasurementHash["Charge"]));

					//ADD TO MZ_LIST
					MZ_List.Add(temp_mz);

				}
			}

			//REMOVE DUPLICATES IN OUR TEMP DICT
			foreach (double i in MZ_List)
				tempDict[i] = 1;

			//TURN TEMP DICT INTO NEW LIST
			MZ_Final = new List<double>(tempDict.Keys);

			//SORT FROM LOW->HIGH AS REQUIRED FOR COLUMN F
			median = ComputeMedian(MZ_Final);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 4TH DIGIT       
			return median.ToString("0.0000");
		}

		/// <summary>
		/// IS-3A: Count of 1+ peptides / count of 2+ peptides
		/// </summary>
		/// <returns></returns>
		public string IS_3A()
		{
			//DECLARE VARIABLES
			int count_ones = 0;                                                             //TOTAL # OF 1's
			int count_twos = 0;                                                             //TOTAL # OF 2's
			int count_threes = 0;                                                           //TOTAL # OF 3's
			int count_fours = 0;                                                            //TOTAL # OF 4's
			double result = 0;																//RESULT OF MEASUREMENT

			IS3_Shared(out count_ones, out count_twos, out count_threes, out count_fours);

			//CALC MEASUREMENT
			if (count_twos > 0)
				result = count_ones / (double)count_twos;

			//ROUND
			return result.ToString("0.000000");
		}

		/// <summary>
		/// IS-3B: Count of 3+ peptides / count of 2+ peptides
		/// </summary>
		/// <returns></returns>
		public string IS_3B()
		{
			//DECLARE VARIABLES
			int count_ones = 0;                                                             //TOTAL # OF 1's
			int count_twos = 0;                                                             //TOTAL # OF 2's
			int count_threes = 0;                                                           //TOTAL # OF 3's
			int count_fours = 0;                                                            //TOTAL # OF 4's
			double result = 0;																//RESULT OF MEASUREMENT

			IS3_Shared(out count_ones, out count_twos, out count_threes, out count_fours);

			//CALC MEASUREMENT
			if (count_twos > 0)
				result = count_threes / (double)count_twos;

			//ROUND
			return result.ToString("0.000000");
		}

		/// <summary>
		/// IS-3C: Count of 4+ peptides / count of 2+ peptides
		/// </summary>
		/// <returns></returns>
		public string IS_3C()
		{
			//DECLARE VARIABLES
			int count_ones = 0;                                                             //TOTAL # OF 1's
			int count_twos = 0;                                                             //TOTAL # OF 2's
			int count_threes = 0;                                                           //TOTAL # OF 3's
			int count_fours = 0;                                                            //TOTAL # OF 4's
			double result = 0;																//RESULT OF MEASUREMENT

			IS3_Shared(out count_ones, out count_twos, out count_threes, out count_fours);

			//CALC MEASUREMENT
			if (count_twos > 0)
				result = count_fours / (double)count_twos;

			//ROUND
			return result.ToString("0.000000");
		}

		protected void IS3_Shared(out int count_ones, out int count_twos, out int count_threes, out int count_fours)
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT Scan,Peptide_Expectation_Value_Log,Charge "
				+ "FROM `temp_xt` "
				+ "WHERE temp_xt.random_id=" + m_Random_ID + " "
				+ "Order by Scan;");

			//DECLARE VARIABLES
			count_ones = 0;                                                             //TOTAL # OF 1's
			count_twos = 0;                                                             //TOTAL # OF 2's
			count_threes = 0;                                                           //TOTAL # OF 3's
			count_fours = 0;                                                            //TOTAL # OF 4's

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Charge" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//IF PEPTIDE EXP VALUE LOG <= -2 SET FILTER TO TRUE
				if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					//CONVERT CHARGE TO INT FOR SWITCH()
					int charge;
					if (int.TryParse(m_MeasurementHash["Charge"].ToString(), out charge))
					{
						//ADD TO CORRECT COUNT
						switch (charge)
						{
							case 1:
								count_ones++;
								break;

							case 2:
								count_twos++;
								break;

							case 3:
								count_threes++;
								break;

							case 4:
								count_fours++;
								break;

							default:
								break;
						}
					}
				}
			}

		}

		/// <summary>
		/// MS1_1: Median MS1 ion injection time
		/// </summary>
		/// <returns></returns>
		public string MS1_1()
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT temp_scanstats.ScanNumber, temp_scanstats.ScanType, temp_scanstatsex.Ion_Injection_Time "
				+ "FROM `temp_scanstats`, `temp_scanstatsex` "
				+ "WHERE temp_scanstats.ScanNumber=temp_scanstatsex.ScanNumber "
				+ "AND temp_scanstatsex.random_id=" + m_Random_ID + " "
				+ "AND temp_scanstats.random_id=" + m_Random_ID + " "
				+ "ORDER BY temp_scanstats.ScanNumber;");

			//DECLARE VARIABLES
			List<double> Filter = new List<double>();                               //FILTER LIST
			double median = 0.00;                                                   //RESULT

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "ScanNumber", "ScanType", "Ion_Injection_Time" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//SCAN TYPE == 1
				if (Convert.ToDouble(m_MeasurementHash["ScanType"]) == 1)
				{
					//ADD TO FILTER LIST
					Filter.Add(Convert.ToDouble(m_MeasurementHash["Ion_Injection_Time"]));
				}
			}

			median = ComputeMedian(Filter);

			return Convert.ToString(median);
		}

		/// <summary>
		/// MS1_2A: Median S/N value for MS1 spectra from run start through middle 50% of separation
		/// </summary>
		/// <returns></returns>
		public string MS1_2A()
		{
			//DECLARE VARIABLES
			List<double> List_BPSTNR_C_2A;
			List<double> List_TII_C_2A;
			double median = 0.00;                                               //RESULT

			MS1_2_Shared(out List_BPSTNR_C_2A, out List_TII_C_2A);

			if (List_BPSTNR_C_2A.Count > 0)
			{
				//CALC MEDIAN OF COLUMN J
				median = ComputeMedian(List_BPSTNR_C_2A);
			}

			return Convert.ToString(median);
		}

		/// <summary>
		/// MS1_2B: Median TIC value for identified peptides from run start through middle 50% of separation
		/// </summary>
		/// <returns></returns>
		public string MS1_2B()
		{
			//DECLARE VARIABLES
			List<double> List_BPSTNR_C_2A;
			List<double> List_TII_C_2A;
			double median = 0.00;                                               //RESULT

			MS1_2_Shared(out List_BPSTNR_C_2A, out List_TII_C_2A);

			//CALC MEDIAN OF COLUMN K
			median = ComputeMedian(List_TII_C_2A);

			//DIVIDE BY 1000
			median = median / 1000;

			return Convert.ToString(median);
		}

		protected void MS1_2_Shared(out List<double> List_BPSTNR_C_2A, out List<double> List_TII_C_2A)
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT ScanNumber, ScanType, BasePeakSignalToNoiseRatio, TotalIonIntensity "
				+ "FROM `temp_scanstats` "
				+ "WHERE temp_scanstats.random_id=" + m_Random_ID + " "
				+ ";");

			//DECLARE VARIABLES
			List<double> List_BPSTNR = new List<double>();                      //FILTERED LIST
			List<double> List_TII = new List<double>();                         //FILTERED LIST
			List_BPSTNR_C_2A = new List<double>();								//FILTERED LIST
			List_TII_C_2A = new List<double>();									//FILTERED LIST

			List<int> List_ScanNumber = new List<int>();                        //ScanNumber List
			List<int> List_ScanType = new List<int>();                          //ScanType List
			List<double> List_BasePeakSignalToNoiseRatio = new List<double>();  //BPSTNR List
			List<double> List_TotalIonIntensity = new List<double>();           //TII List

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "ScanNumber", "ScanType", "BasePeakSignalToNoiseRatio", "TotalIonIntensity" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//ADD TO HASH TABLES
				List_ScanNumber.Add(Convert.ToInt32(m_MeasurementHash["ScanNumber"]));
				List_ScanType.Add(Convert.ToInt32(m_MeasurementHash["ScanType"]));
				List_BasePeakSignalToNoiseRatio.Add(Convert.ToDouble(m_MeasurementHash["BasePeakSignalToNoiseRatio"]));
				List_TotalIonIntensity.Add(Convert.ToDouble(m_MeasurementHash["TotalIonIntensity"]));
			}

			int max_scannumber = List_ScanNumber.Max();

			int scanFirstPeptide = GetStoredValueInt("SCAN_FIRST_FILTER_PASSING_PEPTIDE", 0);
			int scanEndC2A = GetStoredValueInt("C_2A_REGION_SCAN_END", 0);

			//LOOP THROUGH ALL
			for (int i = 0; i < List_ScanNumber.Count; i++)
			{
				//SCAN TYPE == 1 && List_ScanNumber[i]>=STORAGE["SCAN_FIRST_FILTER_PASSING_PEPTIDE"] && List_ScanNumber[i]<=STORAGE["C_2A_REGION_SCAN_END"]
				if ((List_ScanType[i] == 1) && (List_ScanNumber[i] >= scanFirstPeptide) && (List_ScanNumber[i] <= scanEndC2A))
				{
					//ADD TO FILTER LISTS
					List_BPSTNR_C_2A.Add(List_BasePeakSignalToNoiseRatio[i]);
					List_TII_C_2A.Add(List_TotalIonIntensity[i]);
				}
			}

			//FILTER
			List_BPSTNR_C_2A.Sort();
			List_TII_C_2A.Sort();

		}

		/// <summary>
		/// MS1_3A: Dynamic range estimate using 95th percentile peptide peak apex intensity / 5th percentile
		/// </summary>
		/// <returns></returns>
		public string MS1_3A()
		{
			double PMI_5PC;
			double PMI_95PC;
			List<double> result;
			double final = 0;

			MS1_3_Shared(out PMI_5PC, out PMI_95PC, out result);

			//CALCULATE FINAL MEASUREMENT VALUE
			if (PMI_5PC > 0)
				final = PMI_95PC / PMI_5PC;

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 3RD DIGIT
			return final.ToString("0.000");
		}

		/// <summary>
		/// MS1_3B: Median peak apex intensity for all peptides
		/// </summary>
		/// <returns></returns>
		public string MS1_3B()
		{
			double PMI_5PC;
			double PMI_95PC;
			List<double> result;
			double median = 0.00;

			MS1_3_Shared(out PMI_5PC, out PMI_95PC, out result);

			//NOW CALCULATE MEDIAN
			median = ComputeMedian(result);

			//WE NOW HAVE RESULT
			if (median > 100)
				return median.ToString("0");
			else
				return median.ToString("0.0");
		}

		protected void MS1_3_Shared(out double PMI_5PC, out double PMI_95PC, out List<double> result)
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT Result_ID, FragScanNumber, PeakMaxIntensity, Peptide_Expectation_Value_Log  "
				+ "FROM `temp_sicstats`, `temp_xt` "
				+ "WHERE `temp_sicstats`.FragScanNumber=`temp_xt`.Scan "
				+ "AND temp_sicstats.random_id=" + m_Random_ID + " "
				+ "AND temp_xt.random_id=" + m_Random_ID + " "
				+ "ORDER BY PeakMaxIntensity, Result_ID DESC;");

			//DECLARE VARIABLES
			result = new List<double>();																		//STORES FILTER LIST [COLUMN D]
			List<double> MPI_list = new List<double>();                                                         //STORES MAX PEAK INTENSITY FOR 5-95%
			List<double> temp_list_mpi = new List<double>();                                                    //STORES PeakMaxIntensity FOR FUTURE CALCULATIONS
			List<int> temp_list_running_sum = new List<int>();                                                  //STORES RUNNING SUM FOR FUTURE CALCULATIONS
			int max_running_sum = 0;                                                                            //STORES THE LARGEST/MAX RUNNING SUM OF COLUMN E


			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Result_ID", "FragScanNumber", "PeakMaxIntensity", "Peptide_Expectation_Value_Log" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//IF PEPTIDE EXP VALUE LOG <= -2 SET FILTER TO TRUE
				if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					//INC TOTAL RUNNING SUM
					max_running_sum++;

					//ADD TO FILTER LIST
					result.Add(Convert.ToDouble(m_MeasurementHash["PeakMaxIntensity"]));

					//ADD TO TEMP LIST TO PROCESS LATER AS WE FIRST NEED TO FIND THE MAX RUNNING SUM WHICH IS DONE AT THE END
					temp_list_mpi.Add(Convert.ToDouble(m_MeasurementHash["PeakMaxIntensity"]));                   //ADD MPI
					temp_list_running_sum.Add(max_running_sum);                                                 //ADD CURRENT RUNNING SUM
				}
			}

			if (max_running_sum > 0)
			{
				//LOOP THROUGH OUR TEMP LIST
				for (int i = 0; i < temp_list_mpi.Count; i++)
				{

					//CHECK IF BETWEEN 5-95%
					double percent = Convert.ToDouble(temp_list_running_sum[i]) / Convert.ToDouble(max_running_sum);
					if (percent >= 0.05 && percent <= 0.95)
					{
						//ADD TO MPI LIST
						MPI_list.Add(temp_list_mpi[i]);
					}

				}
			}

			if (MPI_list.Count > 0)
			{
				//CALCULATE FINAL VALUES
				PMI_5PC = MPI_list.Min();                                                                //COLUMN O3
				PMI_95PC = MPI_list.Max();                                                               //COLUMN O4
			}
			else
			{
				PMI_5PC = 0;
				PMI_95PC = 0;
			}

		}

		/// <summary>
		/// DS_3A: Median of MS1 max / MS1 sampled abundance
		/// </summary>
		/// <returns></returns>
		public string DS_3A()
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT Result_ID, FragScanNumber, ParentIonIntensity, PeakMaxIntensity, Peptide_Expectation_Value_Log  "
				+ "FROM `temp_sicstats`, `temp_xt` "
				+ "WHERE `temp_sicstats`.FragScanNumber=`temp_xt`.Scan "
				+ "AND temp_sicstats.random_id=" + m_Random_ID + " "
				+ "AND temp_xt.random_id=" + m_Random_ID + " "
				+ "ORDER BY FragScanNumber, Result_ID DESC;");

			//DECLARE VARIABLES
			Hashtable Lookup_Table = new Hashtable();                                                           //STORES [RESULT_ID, FragScanNumber] SO WE CAN HAVE DUP FSN'S
			Hashtable Filter_Result = new Hashtable();                                                          //STORES [RESULT_ID, 1/0] SO WE CAN DETERMINE IF WE PASSED THE FILTER
			Dictionary<int, double> Lookup_Table_KV = new Dictionary<int, double>();                            //STORES [RESULT_ID, VALUE] SO WE CAN SORT BY VALUE
			List<double> result_PMIPII_Filtered = new List<double>();                                           //STORES TABLE VALUES COLUMN K
			List<double> result_VBPMIPII_Filtered = new List<double>();                                         //STORES TABLE VALUES COLUMN N
			int max_running_sum = 0;                                                                            //STORES THE LARGEST/MAX RUNNING SUM OF COLUMN L
			int running_sum = 0;                                                                                //STORES THE CURRENT RUNNING SUM OF COLUMN L
			double median = 0.00;                                                                               //INIT MEDIAN
			double parentIonIntensity = 0;
			double ratioPeakMaxToParentIonIntensity = 0;

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Result_ID", "FragScanNumber", "ParentIonIntensity", "PeakMaxIntensity", "Peptide_Expectation_Value_Log" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//STORE RESULT_ID, SCAN [ALLOWS FOR US TO HAVE DUPLICATES WITHOUT CRASHING]
				Lookup_Table.Add(Convert.ToInt32(m_MeasurementHash["Result_ID"]), Convert.ToInt32(m_MeasurementHash["FragScanNumber"]));

				//STORE RESULT_ID, VALUE [SO WE CAN THEN SORT BY VALUE]
				parentIonIntensity = Convert.ToDouble(m_MeasurementHash["ParentIonIntensity"]);
				if (parentIonIntensity > 0)
					ratioPeakMaxToParentIonIntensity = Convert.ToDouble(m_MeasurementHash["PeakMaxIntensity"]) / parentIonIntensity;
				else
					ratioPeakMaxToParentIonIntensity = 0;

				Lookup_Table_KV.Add(Convert.ToInt32(m_MeasurementHash["Result_ID"]), ratioPeakMaxToParentIonIntensity);

				//IF PEPTIDE EXP VALUE LOG <= -2 SET FILTER TO TRUE
				if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					//SET FILTER TO 1
					Filter_Result.Add(Convert.ToInt32(m_MeasurementHash["Result_ID"]), 1);

					//INCREMENT MAX RUNNING SUM
					max_running_sum++;
				}
				else
				{
					//SET FILTER TO 1
					Filter_Result.Add(Convert.ToInt32(m_MeasurementHash["Result_ID"]), 0);
				}
			}

			//USE LINQ TO SORT VALUES IN ASC ORDER
			var items = from i in Lookup_Table_KV.Keys
						orderby Lookup_Table_KV[i] ascending
						select i;

			//LOOP THROUGH ALL KEYS
			foreach (int key in items)
			{
				//KEY==RESULT_ID WHICH IS UNIQUE
				int Scan = Convert.ToInt32(Lookup_Table[key]);
				//Console.WriteLine("Scan={0} && Value={1} && Filter={2}", Scan, Lookup_Table_KV[key], Filter_Result[key]);

				//IF VALID FILTER
				if (Convert.ToInt32(Filter_Result[key]) == 1)
				{
					//INC RUNNING SUM
					running_sum++;

					//ADD TO FILTERED LIST FOR COLUMN K
					result_PMIPII_Filtered.Add(Convert.ToDouble(Lookup_Table_KV[key]));

					//IF IN VALID BOTTOM 50%
					if (max_running_sum > 0)
					{
						if (running_sum / (double)max_running_sum <= 0.5)
						{
							//ADD TO FILTERED LIST FOR COLUMN N
							result_VBPMIPII_Filtered.Add(Convert.ToDouble(Lookup_Table_KV[key]));
						}
					}
				}
			}

			//NOW CALCULATE MEDIAN
			median = ComputeMedian(result_PMIPII_Filtered);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 3RD DIGIT
			return median.ToString("0.000");
		}

		/// <summary>
		/// DS_3B: Median of MS1 max / MS1 sampled abundance; limit to bottom 50% of peptides by abundance
		/// </summary>
		/// <returns></returns>
		public string DS_3B()
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT Result_ID, FragScanNumber, ParentIonIntensity, PeakMaxIntensity, Peptide_Expectation_Value_Log  "
				+ "FROM `temp_sicstats`, `temp_xt` "
				+ "WHERE `temp_sicstats`.FragScanNumber=`temp_xt`.Scan "
				+ "AND temp_sicstats.random_id=" + m_Random_ID + " "
				+ "AND temp_xt.random_id=" + m_Random_ID + " "
				+ "ORDER BY FragScanNumber, Result_ID DESC;");

			//DECLARE VARIABLES
			Hashtable Lookup_Table = new Hashtable();                                                           //STORES [RESULT_ID, FragScanNumber] SO WE CAN HAVE DUP FSN'S
			Hashtable Filter_Result = new Hashtable();                                                          //STORES [RESULT_ID, 1/0] SO WE CAN DETERMINE IF WE PASSED THE FILTER
			Dictionary<int, double> Lookup_Table_KV = new Dictionary<int, double>();                            //STORES [RESULT_ID, VALUE] SO WE CAN SORT BY VALUE
			List<double> result_PMIPII_Filtered = new List<double>();                                           //STORES TABLE VALUES COLUMN K
			List<double> result_VBPMIPII_Filtered = new List<double>();                                         //STORES TABLE VALUES COLUMN N
			int max_running_sum = 0;                                                                            //STORES THE LARGEST/MAX RUNNING SUM OF COLUMN L
			int running_sum = 0;                                                                                //STORES THE CURRENT RUNNING SUM OF COLUMN L
			double median = 0.00;                                                                               //INIT MEDIAN
			double parentIonIntensity = 0;
			double ratioPeakMaxToParentIonIntensity = 0;

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Result_ID", "FragScanNumber", "ParentIonIntensity", "PeakMaxIntensity", "Peptide_Expectation_Value_Log" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//STORE RESULT_ID, SCAN [ALLOWS FOR US TO HAVE DUPLICATES WITHOUT CRASHING]
				Lookup_Table.Add(Convert.ToInt32(m_MeasurementHash["Result_ID"]), Convert.ToInt32(m_MeasurementHash["FragScanNumber"]));

				//STORE RESULT_ID, VALUE [SO WE CAN THEN SORT BY VALUE]
				parentIonIntensity = Convert.ToDouble(m_MeasurementHash["ParentIonIntensity"]);
				if (parentIonIntensity > 0)
					ratioPeakMaxToParentIonIntensity = Convert.ToDouble(m_MeasurementHash["PeakMaxIntensity"]) / parentIonIntensity;
				else
					ratioPeakMaxToParentIonIntensity = 0;

				Lookup_Table_KV.Add(Convert.ToInt32(m_MeasurementHash["Result_ID"]), ratioPeakMaxToParentIonIntensity);

				//IF PEPTIDE EXP VALUE LOG <= -2 SET FILTER TO TRUE
				if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					//SET FILTER TO 1
					Filter_Result.Add(Convert.ToInt32(m_MeasurementHash["Result_ID"]), 1);

					//INCREMENT MAX RUNNING SUM
					max_running_sum++;
				}
				else
				{
					//SET FILTER TO 1
					Filter_Result.Add(Convert.ToInt32(m_MeasurementHash["Result_ID"]), 0);
				}
			}

			//USE LINQ TO SORT VALUES IN ASC ORDER
			var items = from i in Lookup_Table_KV.Keys
						orderby Lookup_Table_KV[i] ascending
						select i;

			//LOOP THROUGH ALL KEYS
			foreach (int key in items)
			{
				//KEY==RESULT_ID WHICH IS UNIQUE
				int Scan = Convert.ToInt32(Lookup_Table[key]);
				//Console.WriteLine("Scan={0} && Value={1} && Filter={2}", Scan, Lookup_Table_KV[key], Filter_Result[key]);

				//IF VALID FILTER
				if (Convert.ToInt32(Filter_Result[key]) == 1)
				{
					//INC RUNNING SUM
					running_sum++;

					//ADD TO FILTERED LIST FOR COLUMN K
					result_PMIPII_Filtered.Add(Convert.ToDouble(Lookup_Table_KV[key]));

					//IF IN VALID BOTTOM 50%
					if (max_running_sum > 0)
					{
						if (running_sum / (double)max_running_sum <= 0.5)
						{
							//ADD TO FILTERED LIST FOR COLUMN N
							result_VBPMIPII_Filtered.Add(Convert.ToDouble(Lookup_Table_KV[key]));
						}
					}
				}
			}

			//NOW CALCULATE MEDIAN
			median = ComputeMedian(result_VBPMIPII_Filtered);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 3RD DIGIT
			return median.ToString("0.000");
		}

		/// <summary>
		/// IS_1A: Occurrences of MS1 jumping >10x
		/// </summary>
		/// <returns></returns>
		public string IS_1A()
		{
			int countMS1Jump10x;
			int countMS1Fall10x;
			int foldThreshold = 10;

			IS_1_Shared(foldThreshold, out countMS1Jump10x, out countMS1Fall10x);
			return Convert.ToString(countMS1Jump10x);
		}

		/// <summary>
		/// IS_1B: Occurrences of MS1 falling >10x
		/// </summary>
		/// <returns></returns>
		public string IS_1B()
		{
			int countMS1Jump10x;
			int countMS1Fall10x;
			int foldThreshold = 10;

			IS_1_Shared(foldThreshold, out countMS1Jump10x, out countMS1Fall10x);
			return Convert.ToString(countMS1Fall10x);
		}

		protected void IS_1_Shared(int foldThreshold, out int countMS1Jump10x, out int countMS1Fall10x)
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT ScanNumber, ScanType, BasePeakIntensity  "
				+ "FROM `temp_scanstats` "
				+ "WHERE temp_scanstats.random_id=" + m_Random_ID + " "
				+ ";");

			//DECLARE VARIABLES
			double bpiPrevious = -1;
			double bpiCurrent = 0;
			countMS1Jump10x = 0;                                                          //STORE COUNT FOR IS_1A
			countMS1Fall10x = 0;                                                          //STORE COUNT FOR IS_1B

			//VALIDATE foldThreshold
			if (foldThreshold < 2)
				foldThreshold = 2;

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "ScanNumber", "ScanType", "BasePeakIntensity" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//DETERMINE Compare_Only_MS1
				if (Convert.ToInt32(m_MeasurementHash["ScanType"]) == 1)
				{
					//SET COMPARE ONLY MS1 TO BPI
					bpiCurrent = Convert.ToDouble(m_MeasurementHash["BasePeakIntensity"]);

					if (bpiPrevious != -1)
					{
						if (bpiCurrent > 0 && bpiPrevious / bpiCurrent > foldThreshold)
						{
							countMS1Fall10x += 1;

						}

						if (bpiPrevious > 0 && bpiCurrent / bpiPrevious > foldThreshold)
						{
							countMS1Jump10x += 1;
						}
					}

					bpiPrevious = bpiCurrent;
				}

			}

		}

		/// <summary>
		/// MS1_5A: Median of precursor mass error (Th)
		/// </summary>
		/// <returns></returns>
		public string MS1_5A()
		{
			//DECLARE VARIABLES
			List<double> FilteredDelM;                                    //STORE FILTERED VALUES [COLUMN G]
			List<double> AbsFilteredDelM;                                 //STORE FILTERED VALUES [COLUMN H]
			List<double> DelMPPM;                                          //STORE FILTERED VALUES [COLUMN I]

			MS1_5_Shared(out FilteredDelM, out AbsFilteredDelM, out DelMPPM);

			//NOW CALCULATE MEDIAN
			double median;
			median = ComputeMedian(FilteredDelM);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 6th DIGIT
			return median.ToString("0.000000");
		}

		/// <summary>
		/// MS1_5B: Median of absolute value of precursor mass error (Th)
		/// </summary>
		/// <returns></returns>
		public string MS1_5B()
		{
			//DECLARE VARIABLES
			List<double> FilteredDelM;                                    //STORE FILTERED VALUES [COLUMN G]
			List<double> AbsFilteredDelM;                                 //STORE FILTERED VALUES [COLUMN H]
			List<double> DelMPPM;                                          //STORE FILTERED VALUES [COLUMN I]

			MS1_5_Shared(out FilteredDelM, out AbsFilteredDelM, out DelMPPM);

			//CALCULATE AVERAGE
			double average;
			average = AbsFilteredDelM.Average();

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 6th DIGIT
			return average.ToString("0.000000");
		}

		/// <summary>
		/// MS1_5C: Median of precursor mass error (ppm)
		/// </summary>
		/// <returns></returns>
		public string MS1_5C()
		{
			//DECLARE VARIABLES
			List<double> FilteredDelM;                                    //STORE FILTERED VALUES [COLUMN G]
			List<double> AbsFilteredDelM;                                 //STORE FILTERED VALUES [COLUMN H]
			List<double> DelMPPM;                                          //STORE FILTERED VALUES [COLUMN I]

			MS1_5_Shared(out FilteredDelM, out AbsFilteredDelM, out DelMPPM);

			//NOW CALCULATE MEDIAN
			double median;
			median = ComputeMedian(DelMPPM);

			return median.ToString("0.000");
		}

		/// <summary>
		/// MS1_5D: Interquartile distance in ppm-based precursor mass error
		/// </summary>
		/// <returns></returns>
		public string MS1_5D()
		{
			//DECLARE VARIABLES
			List<double> FilteredDelM;                                    //STORE FILTERED VALUES [COLUMN G]
			List<double> AbsFilteredDelM;                                 //STORE FILTERED VALUES [COLUMN H]
			List<double> DelMPPM;                                          //STORE FILTERED VALUES [COLUMN I]

			MS1_5_Shared(out FilteredDelM, out AbsFilteredDelM, out DelMPPM);

			//NOW FILTER PPM PASSED VALUES [COLUMN K] + START COUNT
			DelMPPM.Sort();

			List<double> InterquartilePPMErrors = new List<double>();                           //STORE ERRORS FROM PPMList [COLUMN M]
			double median = 0.00;                                                               //INIT MEDIAN
			int INTER_QUARTILE_START = 0;                                                       //REQUIRED FOR MEASUREMENT
			int INTER_QUARTILE_END = 0;                                                         //REQUIRED FOR MEASUREMENT
			int count = 0;

			//CALCULATE INTER_QUARTILE_START AND INTER_QUARTILE_END
			INTER_QUARTILE_START = Convert.ToInt32(Math.Round(0.25 * Convert.ToDouble(DelMPPM.Count)));
			INTER_QUARTILE_END = Convert.ToInt32(Math.Round(0.75 * Convert.ToDouble(DelMPPM.Count)));

			//LOOP THROUGH EACH ITEM IN LIST
			foreach (double item in DelMPPM)
			{
				//INC count
				count++;

				if ((count >= INTER_QUARTILE_START) && (count <= INTER_QUARTILE_END))
				{
					//ADD TO LIST [COLUMN M]
					InterquartilePPMErrors.Add(item);
				}
			}

			//NOW CALCULATE MEDIAN
			median = ComputeMedian(InterquartilePPMErrors);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 3 DIGITS
			return median.ToString("0.000");
		}

		protected void MS1_5_Shared(out List<double> FilteredDelM, out List<double> AbsFilteredDelM, out List<double> DelMPPM)
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT temp_xt.Scan,temp_xt.Peptide_Expectation_Value_Log,temp_xt.Peptide_MH,temp_xt.Charge, temp_sicstats.MZ "
				+ "FROM `temp_xt`,`temp_sicstats` "
				+ "WHERE temp_sicstats.FragScanNumber=temp_xt.Scan AND temp_sicstats.random_id=" + m_Random_ID + " AND temp_xt.random_id=" + m_Random_ID + " "
				+ "ORDER BY temp_xt.Scan;");

			//DECLARE VARIABLES
			double massHydrogen = 1.00727649;                                      //REQUIRED BY MEASUREMENT
			double massC13 = 1.00335483;
			FilteredDelM = new List<double>();                                    //STORE FILTERED VALUES [COLUMN G]
			AbsFilteredDelM = new List<double>();                                 //STORE FILTERED VALUES [COLUMN H]
			DelMPPM = new List<double>();                                          //STORE FILTERED VALUES [COLUMN I]

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Peptide_MH", "Charge", "MZ" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//CALC THEORETICAL VALUE [COLUMN F], which would be theoretical m/z of the peptide
				// Instead, calculate theoretical monoisotopic mass of the peptide

				double theoMonoMass = Convert.ToDouble(m_MeasurementHash["Peptide_MH"]) - massHydrogen;

				//IF LOG(E) <= -2 ... CALC FILTERED AND ABS FILTERED
				if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					// Compute observed precursor mass, as monoisotopic mass					
					int observedCharge = Convert.ToInt16(m_MeasurementHash["Charge"]);
					double observedMonoMass = Convert.ToDouble(m_MeasurementHash["MZ"]) * observedCharge - massHydrogen * observedCharge;

					double delm = observedMonoMass - theoMonoMass;

					// Correct the delm value by assuring that it is between -0.5 and5 0.5
					// This corrects for the instrument choosing the 2nd or 3rd isotope of an isotopic distribution as the parent ion
					while (delm < -0.5)
						delm += massC13;

					while (delm > 0.5)
						delm -= massC13;

					double delMppm = 0;

					if (theoMonoMass > 0)
					{
						delMppm = delm / (theoMonoMass / 1000000);
					}

					if (Math.Abs(delMppm) > 200)
					{
						Console.WriteLine("Large DelM_PPM: " + delMppm);
					}

					//CALC FILTERED ARRAY
					FilteredDelM.Add(delm);

					//NOW TAKE THE ABS VALUE OF OUR FILTERED ARRAY
					AbsFilteredDelM.Add(Math.Abs(delm));

					//ADD TO PPM LIST
					DelMPPM.Add(delMppm);
				}

			}

		}

		/// <summary>
		/// MS2_1: Median MS2 ion injection time
		/// </summary>
		/// <returns></returns>
		public string MS2_1()
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.Peptide_Expectation_Value_Log, temp_scanstatsex.Ion_Injection_Time "
				+ "FROM `temp_xt`, `temp_scanstatsex` "
				+ "WHERE temp_xt.Scan=temp_scanstatsex.ScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_scanstatsex.random_id=" + m_Random_ID + " "
				+ "ORDER BY temp_xt.Scan;");

			//DECLARE VARIABLES
			List<double> FilterList = new List<double>();                                       //FILTERED LIST [COLUMN P]
			double median = 0.00;                                                               //STORE MEDIAN

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Ion_Injection_Time" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//CALCULATE COLUMN P
				if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					//ADD TO FILTERED LIST
					FilterList.Add(Convert.ToDouble(m_MeasurementHash["Ion_Injection_Time"]));
				}
			}

			//NOW CALCULATE MEDIAN
			median = ComputeMedian(FilterList);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 3rd DIGIT
			return median.ToString("0.000");
		}

		/// <summary>
		/// MS2_2: Median S/N value for identified MS2 spectra
		/// </summary>
		/// <returns></returns>
		public string MS2_2()
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.Peptide_Expectation_Value_Log, temp_scanstats.BasePeakSignalToNoiseRatio "
				+ "FROM `temp_xt`, `temp_scanstats` "
				+ "WHERE temp_xt.Scan=temp_scanstats.ScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_scanstats.random_id=" + m_Random_ID + " "
				+ "ORDER BY temp_xt.Scan;");

			//DECLARE VARIABLES
			List<double> FilterList = new List<double>();                                       //FILTERED LIST [COLUMN H]
			List<double> FinishedList = new List<double>();                                     //FINISHED LIST [COLUMN J]
			double median = 0.00;                                                               //STORE MEDIAN
			int current_count = 0;                                                              //CURRENT COUNTER

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Expectation_Value_Log", "BasePeakSignalToNoiseRatio" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//CALCULATE COLUMN P
				if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					//ADD TO FILTERED LIST
					FilterList.Add(Convert.ToDouble(m_MeasurementHash["BasePeakSignalToNoiseRatio"]));
				}
			}

			//LOOP THROUGH FILTERED LIST
			for (int i = 0; i < FilterList.Count; i++)
			{
				//INC COUNTER
				current_count++;

				//CALCULATE IF <= 0.75
				if (current_count <= (FilterList.Count * 0.75))
				{
					//ADD TO FINISHED FILTERED LIST
					FinishedList.Add(FilterList[i]);
				}
			}

			//NOW CALCULATE MEDIAN
			median = ComputeMedian(FinishedList);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 3 DIGITS
			return median.ToString("0.000");
		}

		/// <summary>
		/// MS2_3: Median number of peaks in all MS2 spectra
		/// </summary>
		/// <returns></returns>
		public string MS2_3()
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.Peptide_Expectation_Value_Log, temp_scanstats.IonCountRaw "
				+ "FROM `temp_xt`, `temp_scanstats` "
				+ "WHERE temp_xt.Scan=temp_scanstats.ScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_scanstats.random_id=" + m_Random_ID + " "
				+ "ORDER BY temp_xt.Scan;");

			//DECLARE VARIABLES
			List<double> FilterList = new List<double>();                                       //FILTERED LIST [COLUMN M]
			double median = 0.00;                                                               //STORE MEDIAN

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Expectation_Value_Log", "IonCountRaw" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//CALCULATE COLUMN M
				if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					//ADD TO FILTERED LIST
					FilterList.Add(Convert.ToDouble(m_MeasurementHash["IonCountRaw"]));
				}
			}

			//NOW CALCULATE MEDIAN
			median = ComputeMedian(FilterList);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO THE NEAREST INTEGER
			return median.ToString("0");
		}

		/// <summary>
		/// MS2_4A: Fraction of all MS2 spectra identified; high abundance quartile (determined using MS1 intensity of identified peptides)
		/// </summary>
		/// <returns></returns>
		public string MS2_4A()
		{
			int ScanCountQ1 = 0;
			int ScanCountQ2 = 0;
			int ScanCountQ3 = 0;
			int ScanCountQ4 = 0;
			int Q1PassFilt = 0;
			int Q2PassFilt = 0;
			int Q3PassFilt = 0;
			int Q4PassFilt = 0;

			MS2_4_Shared(out ScanCountQ1, out ScanCountQ2, out ScanCountQ3, out ScanCountQ4, out Q1PassFilt, out Q2PassFilt, out Q3PassFilt, out Q4PassFilt);

			//COMPUTE THE RESULT
			double result = 0;
			if (ScanCountQ1 > 0)
				result = Q1PassFilt / (double)ScanCountQ1;

			return result.ToString("0.0000");
		}

		/// <summary>
		/// MS2_4B: Fraction of all MS2 spectra identified; second quartile (determined using MS1 intensity of identified peptides)
		/// </summary>
		/// <returns></returns>
		public string MS2_4B()
		{
			int ScanCountQ1 = 0;
			int ScanCountQ2 = 0;
			int ScanCountQ3 = 0;
			int ScanCountQ4 = 0;
			int Q1PassFilt = 0;
			int Q2PassFilt = 0;
			int Q3PassFilt = 0;
			int Q4PassFilt = 0;

			MS2_4_Shared(out ScanCountQ1, out ScanCountQ2, out ScanCountQ3, out ScanCountQ4, out Q1PassFilt, out Q2PassFilt, out Q3PassFilt, out Q4PassFilt);

			//COMPUTE THE RESULT
			double result = 0;
			if (ScanCountQ2 > 0)
				result = Q2PassFilt / (double)ScanCountQ2;

			return result.ToString("0.0000");

		}

		/// <summary>
		/// MS2_4C: Fraction of all MS2 spectra identified; third quartile (determined using MS1 intensity of identified peptides)
		/// </summary>
		/// <returns></returns>
		public string MS2_4C()
		{
			int ScanCountQ1 = 0;
			int ScanCountQ2 = 0;
			int ScanCountQ3 = 0;
			int ScanCountQ4 = 0;
			int Q1PassFilt = 0;
			int Q2PassFilt = 0;
			int Q3PassFilt = 0;
			int Q4PassFilt = 0;

			MS2_4_Shared(out ScanCountQ1, out ScanCountQ2, out ScanCountQ3, out ScanCountQ4, out Q1PassFilt, out Q2PassFilt, out Q3PassFilt, out Q4PassFilt);

			//COMPUTE THE RESULT
			double result = 0;
			if (ScanCountQ3 > 0)
				result = Q3PassFilt / (double)ScanCountQ3;

			return result.ToString("0.0000");

		}

		/// <summary>
		/// MS2_4D: Fraction of all MS2 spectra identified; low abundance quartile (determined using MS1 intensity of identified peptides)
		/// </summary>
		/// <returns></returns>
		public string MS2_4D()
		{
			int ScanCountQ1 = 0;
			int ScanCountQ2 = 0;
			int ScanCountQ3 = 0;
			int ScanCountQ4 = 0;
			int Q1PassFilt = 0;
			int Q2PassFilt = 0;
			int Q3PassFilt = 0;
			int Q4PassFilt = 0;

			MS2_4_Shared(out ScanCountQ1, out ScanCountQ2, out ScanCountQ3, out ScanCountQ4, out Q1PassFilt, out Q2PassFilt, out Q3PassFilt, out Q4PassFilt);

			//COMPUTE THE RESULT
			double result = 0;
			if (ScanCountQ4 > 0)
				result = Q4PassFilt / (double)ScanCountQ4;

			return result.ToString("0.0000");

		}

		protected void MS2_4_Shared(out int ScanCountQ1, out int ScanCountQ2, out int ScanCountQ3, out int ScanCountQ4, 
			                        out int Q1PassFilt, out int Q2PassFilt, out int Q3PassFilt, out int Q4PassFilt)
		{
			//SET DB QUERY [TO FIND MAX NUMBER OF ROWS]
			m_DBInterface.setQuery("SELECT COUNT(*) as MS2ScanCount "
				+ "FROM `temp_xt`, `temp_sicstats` "
				+ "WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_sicstats.random_id=" + m_Random_ID + " "
				+ ";");

			//DECLARE FIELDS TO READ FROM
			string[] fields_temp = { "MS2ScanCount" };

			//INIT READER
			m_DBInterface.initReader();

			//READ LINE
			m_DBInterface.readSingleLine(fields_temp, ref m_MeasurementHash);
			int scanCountMS2 = Convert.ToInt32(m_MeasurementHash["MS2ScanCount"]);                         //STORE TOTAL MS2 SCAN COUNT

			//SET DB QUERY
			//NOTE THAT WE SORT BY ASCENDING PeakMaxIntensity
			m_DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.Peptide_Expectation_Value_Log, temp_sicstats.PeakMaxIntensity "
				+ "FROM `temp_xt`, `temp_sicstats` "
				+ "WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_sicstats.random_id=" + m_Random_ID + " "
				+ "ORDER BY temp_sicstats.PeakMaxIntensity;");

			//DECLARE VARIABLES
			ScanCountQ1 = 0;														//FOUND FOR FIRST QUARTILE LIST [COLUMN I]
			ScanCountQ2 = 0;														//FOUND FOR SECOND QUARTILE LIST [COLUMN K]
			ScanCountQ3 = 0;														//FOUND FOR THIRD QUARTILE LIST [COLUMN M]
			ScanCountQ4 = 0;														//FOUND FOR FOURTH QUARTILE LIST [COLUMN O]
			Q1PassFilt = 0;															//Identified FOR FIRST QUARTILE LIST [COLUMN J]
			Q2PassFilt = 0;															//Identified FOR SECOND QUARTILE LIST [COLUMN L]
			Q3PassFilt = 0;															//Identified FOR THIRD QUARTILE LIST [COLUMN N]
			Q4PassFilt = 0;															//Identified FOR FOURTH QUARTILE LIST [COLUMN P]
			int scan_count = 1;                                                     //RUNNING SCAN COUNT [COLUMN H]

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Expectation_Value_Log", "PeakMaxIntensity" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//DID IT PASS OUR FILTER?
				bool passed_filter = false;

				//CALCULATE COLUMN G
				if (Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					passed_filter = true;
				}

				//IF SCAN IN FIRST QUARTILE
				if (scan_count < (scanCountMS2 * 0.25))
				{
					//FOUND SO ADD AS 1
					ScanCountQ1++;

					if (passed_filter)
						Q1PassFilt++;
				}

				//IF SCAN IN SECOND QUARTILE
				if (scan_count >= (scanCountMS2 * 0.25) && scan_count < (scanCountMS2 * 0.5))
				{
					//FOUND SO ADD AS 1
					ScanCountQ2++;

					if (passed_filter)
						Q2PassFilt++;
				}

				//IF SCAN IN THIRD QUARTILE
				if (scan_count >= (scanCountMS2 * 0.5) && scan_count < (scanCountMS2 * 0.75))
				{
					//FOUND SO ADD AS 1
					ScanCountQ3++;

					if (passed_filter)
						Q3PassFilt++;
				}

				//IF SCAN IN FOURTH QUARTILE
				if (scan_count >= (scanCountMS2 * 0.75))
				{
					//FOUND SO ADD AS 1
					ScanCountQ4++;

					if (passed_filter)
						Q4PassFilt++;
				}

				//INC
				scan_count++;
			}

		}

		/// <summary>
		/// P_1A: Median peptide ID score (X!Tandem hyperscore)
		/// </summary>
		/// <returns></returns>
		public string P_1A()
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT Scan, Peptide_Hyperscore, Peptide_Expectation_Value_Log "
				+ "FROM `temp_xt` "
				+ "WHERE temp_xt.random_id=" + m_Random_ID + " "
				+ "ORDER BY Scan;");

			//DECLARE VARIABLES
			List<double> Peptide_Hyperscore_List = new List<double>();                          //STORE PEPTIDE HYPERSCORE LIST
			double median = 0.00;                                                               //INIT MEDIAN VALUE

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Hyperscore", "Peptide_Expectation_Value_Log" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//CALCULATE COLUMN B + ADD TO LIST
				Peptide_Hyperscore_List.Add(Convert.ToDouble(m_MeasurementHash["Peptide_Hyperscore"]));
			}

			//NOW CALCULATE MEDIAN
			median = ComputeMedian(Peptide_Hyperscore_List);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 3rd DIGIT
			return median.ToString("0.00");
		}

		/// <summary>
		/// P_1B: Median peptide ID score (X!Tandem Peptide_Expectation_Value_Log(e))
		/// </summary>
		/// <returns></returns>
		public string P_1B()
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT Scan, Peptide_Hyperscore, Peptide_Expectation_Value_Log "
				+ "FROM `temp_xt` "
				+ "WHERE temp_xt.random_id=" + m_Random_ID + " "
				+ "ORDER BY Scan;");

			//DECLARE VARIABLES
			List<double> Peptide_Expectation_List = new List<double>();                         //STORE PEPTIDE EXP LIST
			double median = 0.00;                                                               //INIT MEDIAN VALUE

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Hyperscore", "Peptide_Expectation_Value_Log" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				//CALCULATE COLUMN C + ADD TO LIST
				Peptide_Expectation_List.Add(Convert.ToDouble(m_MeasurementHash["Peptide_Expectation_Value_Log"]));
			}

			//NOW CALCULATE MEDIAN
			median = ComputeMedian(Peptide_Expectation_List);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 3rd DIGIT           
			return median.ToString("0.000");
		}

		/// <summary>
		/// P_2A: Number of tryptic peptides; total spectra count
		/// </summary>
		/// <returns></returns>
		public string P_2A()
		{
			//BUILD RESULT_ID, UNIQUE_SEQ_TABLE
			Dictionary<int, int> ResultID_to_Unique_Seq_ID_Table;
			ResultID_to_Unique_Seq_ID_Table = GetResultIDToSeqIDTable();

			// BUILD UNIQUE_SEQ_TABLE, CLEAVAGE STATE TABLE            
			// Populate a dictionary object via a single query to the database
			Dictionary<int, int> Seq_ID_to_Cleavage_State_Table;
			Seq_ID_to_Cleavage_State_Table = GetSeqIDToCleavageStateTable();

			//SET DB QUERY
			m_DBInterface.setQuery("SELECT Scan,Peptide_Expectation_Value_Log, Charge, temp_xt.Result_ID, Peptide_Sequence, temp_xt_seqtoproteinmap.Cleavage_State "
				+ "FROM `temp_xt`, `temp_xt_resulttoseqmap` "
				+ "JOIN `temp_xt_seqtoproteinmap` ON `temp_xt_resulttoseqmap`.Unique_Seq_ID=`temp_xt_seqtoproteinmap`.Unique_Seq_ID "
				+ "WHERE temp_xt.Peptide_Expectation_Value_Log <= -2.00 "
				+ "AND temp_xt.Result_ID=temp_xt_resulttoseqmap.Result_ID "
				+ "AND temp_xt.random_id=" + m_Random_ID + " "
				+ "AND temp_xt_resulttoseqmap.random_id=" + m_Random_ID + " "
				+ "GROUP BY temp_xt_resulttoseqmap.Result_ID "
				+ "ORDER BY temp_xt.Peptide_Sequence, temp_xt.Group_ID;");

			//DECLARE VARIABLES
			int cleavage_state_1_count = 0;                                                         //COLUMN H
			int cleavage_state_2_count = 0;                                                         //COLUMN I
			int unique_cleavage_state_2_count = 0;                                                  //COLUMN J
			int cleavage_state_2_charge_1 = 0;                                                      //COLUMN K
			int line_count = 0;                                                                     //LINE COUNTER
			string prv_peptide_sequence = "";                                                       //PRV PEPTIDE SEQUENCE

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Charge", "Result_ID", "Peptide_Sequence", "Cleavage_State" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				int resultID;
				int seqID;
				int cleavage_state = 0;

				// DETERMINE THE CLEAVAGE STATE
				if (int.TryParse(m_MeasurementHash["Result_ID"].ToString(), out resultID))
				{
					if (ResultID_to_Unique_Seq_ID_Table.TryGetValue(resultID, out seqID))
					{
						Seq_ID_to_Cleavage_State_Table.TryGetValue(seqID, out cleavage_state);   //CONVERT UNIQUE_SEQUENCE_ID TO CLEAVAGE_STATE
					}
				}

				//FOR cleavage_state_1_count [COLUMN H]
				if (cleavage_state == 1)
				{
					//COLUMN H
					cleavage_state_1_count += 1;
				}

				//FOR cleavage_state_2_count [COLUMN I]
				if (cleavage_state == 2)
				{
					//COLUMN I
					cleavage_state_2_count += 1;
				}

				//IF THIS IS THE FIRST LINE
				if (line_count == 0)
				{
					//INC LINE COUNT
					line_count++;

					//FOR unique_cleavage_state_2_count [COLUMN J]
					if (cleavage_state == 2)
					{
						unique_cleavage_state_2_count = 1;
					}

					//FOR cleavage_state_2_charge_1 [COLUMN K]
					if (Convert.ToInt32(m_MeasurementHash["Charge"]) == 1)
					{
						cleavage_state_2_charge_1 = 1;
					}

				}
				else
				{
					//LINE COUNT IS NOT THE FIRST LINE

					//FOR unique_cleavage_state_2_count IF CURRENT PEPTIDE SEQUENCE != PRV PEPTIDE SEQUENCE [COLUMN J]
					if (cleavage_state == 2 && !Convert.ToString(m_MeasurementHash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
					{
						unique_cleavage_state_2_count += 1;
					}

					//FOR cleavage_state_2_charge_1 IF CURRENT PEPTIDE SEQUENCE != PRV PEPTIDE SEQUENCE [COLUMN K]
					if (Convert.ToInt32(m_MeasurementHash["Charge"]) == 1 && !Convert.ToString(m_MeasurementHash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
					{
						cleavage_state_2_charge_1 += 1;
					}

				}

				//UPDATE PREVIOUS VALUES FOR NEXT LOOP
				prv_peptide_sequence = Convert.ToString(m_MeasurementHash["Peptide_Sequence"]);
			}

			//SET ANSWER
			return cleavage_state_2_count.ToString();
		}

		/// <summary>
		/// P_2B: Number of tryptic peptides; unique peptide & charge count
		/// </summary>
		/// <returns></returns>
		public string P_2B()
		{
			//BUILD RESULT_ID, UNIQUE_SEQ_TABLE
			Dictionary<int, int> ResultID_to_Unique_Seq_ID_Table;
			ResultID_to_Unique_Seq_ID_Table = GetResultIDToSeqIDTable();

			// BUILD UNIQUE_SEQ_TABLE, CLEAVAGE STATE TABLE            
			// Populate a dictionary object via a single query to the database
			Dictionary<int, int> Seq_ID_to_Cleavage_State_Table;
			Seq_ID_to_Cleavage_State_Table = GetSeqIDToCleavageStateTable();

			//SET DB QUERY
			m_DBInterface.setQuery("SELECT temp_xt.Result_ID, temp_xt.Scan, temp_xt.Peptide_Expectation_Value_Log, temp_xt.Charge, temp_xt.Peptide_Sequence, temp_xt_seqtoproteinmap.Cleavage_State, temp_xt_seqtoproteinmap.Unique_Seq_ID "
				+ "FROM `temp_xt`, `temp_xt_resulttoseqmap` "
				+ "INNER JOIN `temp_xt_seqtoproteinmap` ON `temp_xt_resulttoseqmap`.Unique_Seq_ID=`temp_xt_seqtoproteinmap`.Unique_Seq_ID "
				+ "WHERE Peptide_Expectation_Value_Log <= -2.00 "
				+ "AND temp_xt.Result_ID=temp_xt_resulttoseqmap.Result_ID "
				+ "AND temp_xt.random_id=" + m_Random_ID + " "
				+ "AND temp_xt_resulttoseqmap.random_id=" + m_Random_ID + " "
				+ "GROUP BY temp_xt_resulttoseqmap.Result_ID "
				+ "ORDER BY Charge, Peptide_Sequence, Scan;");

			//DECLARE VARIABLES
			string prv_peptide_sequence = "";                                                       //STORE PREVIOUS PEPTIDE SEQUENCE
			string prv_prv_peptide_sequence = "";                                                   //STORE PREVIOUS PREVIOUS PEPTIDE SEQUENCE
			int prv_cleavage_state = 0;                                                             //STORE PREVIOUS CLEAVAGE STATE
			int count_with_different_charges = 0;                                                   //COUNTS WITH DIFFERENT CHARGES [COLUMN F]
			Boolean is_first_line = true;                                                           //IS FIRST LINE 

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Result_ID", "Scan", "Peptide_Expectation_Value_Log", "Charge", "Peptide_Sequence", "Cleavage_State", "Unique_Seq_ID" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				int resultID;
				int seqID;
				int cleavage_state = 0;

				// DETERMINE THE CLEAVAGE STATE
				if (int.TryParse(m_MeasurementHash["Result_ID"].ToString(), out resultID))
				{
					if (ResultID_to_Unique_Seq_ID_Table.TryGetValue(resultID, out seqID))
					{
						Seq_ID_to_Cleavage_State_Table.TryGetValue(seqID, out cleavage_state);   //CONVERT UNIQUE_SEQUENCE_ID TO CLEAVAGE_STATE
					}
				}

				//IS FIRST LINE?
				if (is_first_line)
				{
					//SET TO FALSE
					is_first_line = false;

					//IF CURRENT CLEAVAGE STATE == 2 && PREV + CURRENT PEPTIDE SEQUENCE VALUES ARE DIFFERENT [ONLY FOR FIRST LINE]
					if (cleavage_state == 2 && !Convert.ToString(m_MeasurementHash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
					{
						//INC
						count_with_different_charges++;
					}
				}
				else
				{
					//IF PREV CLEAVAGE STATE == 2 && PREV + PREV PREV PEPTIDE SEQUENCE VALUES ARE DIFFERENT
					if (prv_cleavage_state == 2 && !prv_peptide_sequence.Equals(prv_prv_peptide_sequence))
					{
						//INC
						count_with_different_charges++;
					}
				}

				//UPDATE PREV VALUES
				prv_prv_peptide_sequence = prv_peptide_sequence;
				prv_peptide_sequence = Convert.ToString(m_MeasurementHash["Peptide_Sequence"]);
				prv_cleavage_state = cleavage_state;
			}

			//SET ANSWER
			return count_with_different_charges.ToString();
		}

		/// <summary>
		/// P_2C: Number of tryptic peptides; unique peptide count
		/// </summary>
		/// <returns></returns>
		public string P_2C()
		{
			//BUILD RESULT_ID, UNIQUE_SEQ_TABLE
			Dictionary<int, int> ResultID_to_Unique_Seq_ID_Table;
			ResultID_to_Unique_Seq_ID_Table = GetResultIDToSeqIDTable();

			// BUILD UNIQUE_SEQ_TABLE, CLEAVAGE STATE TABLE            
			// Populate a dictionary object via a single query to the database
			Dictionary<int, int> Seq_ID_to_Cleavage_State_Table;
			Seq_ID_to_Cleavage_State_Table = GetSeqIDToCleavageStateTable();

			//SET DB QUERY
			m_DBInterface.setQuery("SELECT Scan,Peptide_Expectation_Value_Log, Charge, temp_xt.Result_ID, Peptide_Sequence, temp_xt_seqtoproteinmap.Cleavage_State, temp_xt_seqtoproteinmap.Unique_Seq_ID "
				+ "FROM `temp_xt`, `temp_xt_resulttoseqmap` "
				+ "JOIN `temp_xt_seqtoproteinmap` ON `temp_xt_resulttoseqmap`.Unique_Seq_ID=`temp_xt_seqtoproteinmap`.Unique_Seq_ID "
				+ "WHERE temp_xt.Peptide_Expectation_Value_Log <= -2.00 "
				+ "AND temp_xt.Result_ID=temp_xt_resulttoseqmap.Result_ID "
				+ "AND temp_xt.random_id=" + m_Random_ID + " "
				+ "AND temp_xt_resulttoseqmap.random_id=" + m_Random_ID + " "
				+ "GROUP BY temp_xt_resulttoseqmap.Result_ID "
				+ "ORDER BY temp_xt.Peptide_Sequence, temp_xt.Group_ID;");

			//DECLARE VARIABLES
			int cleavage_state_1_count = 0;                                                         //COLUMN H
			int cleavage_state_2_count = 0;                                                         //COLUMN I
			int unique_cleavage_state_2_count = 0;                                                  //COLUMN J
			int cleavage_state_2_charge_1 = 0;                                                      //COLUMN K
			int line_count = 0;                                                                     //LINE COUNTER
			string prv_peptide_sequence = "";                                                       //PRV PEPTIDE SEQUENCE

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Charge", "Result_ID", "Peptide_Sequence", "Cleavage_State", "Unique_Seq_ID" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				int resultID;
				int seqID;
				int cleavage_state = 0;

				// DETERMINE THE CLEAVAGE STATE
				if (int.TryParse(m_MeasurementHash["Result_ID"].ToString(), out resultID))
				{
					if (ResultID_to_Unique_Seq_ID_Table.TryGetValue(resultID, out seqID))
					{
						Seq_ID_to_Cleavage_State_Table.TryGetValue(seqID, out cleavage_state);   //CONVERT UNIQUE_SEQUENCE_ID TO CLEAVAGE_STATE
					}
				}

				//FOR cleavage_state_1_count [COLUMN H]
				if (cleavage_state == 1)
				{
					//COLUMN H
					cleavage_state_1_count += 1;
				}

				//FOR cleavage_state_2_count [COLUMN I]
				if (cleavage_state == 2)
				{
					//COLUMN I
					cleavage_state_2_count += 1;
				}

				//IF THIS IS THE FIRST LINE
				if (line_count == 0)
				{
					//INC LINE COUNT
					line_count++;

					//FOR unique_cleavage_state_2_count [COLUMN J]
					if (cleavage_state == 2)
					{
						unique_cleavage_state_2_count = 1;
					}

					//FOR cleavage_state_2_charge_1 [COLUMN K]
					if (Convert.ToInt32(m_MeasurementHash["Charge"]) == 1)
					{
						cleavage_state_2_charge_1 = 1;
					}

				}
				else
				{
					//LINE COUNT IS NOT THE FIRST LINE

					//FOR unique_cleavage_state_2_count IF CURRENT PEPTIDE SEQUENCE != PRV PEPTIDE SEQUENCE [COLUMN J]
					if (cleavage_state == 2 && !Convert.ToString(m_MeasurementHash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
					{
						unique_cleavage_state_2_count += 1;
					}

					//FOR cleavage_state_2_charge_1 IF CURRENT PEPTIDE SEQUENCE != PRV PEPTIDE SEQUENCE [COLUMN K]
					if (Convert.ToInt32(m_MeasurementHash["Charge"]) == 1 && !Convert.ToString(m_MeasurementHash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
					{
						cleavage_state_2_charge_1 += 1;
					}

				}

				//UPDATE PREVIOUS VALUES FOR NEXT LOOP
				prv_peptide_sequence = Convert.ToString(m_MeasurementHash["Peptide_Sequence"]);
			}

			//SET ANSWER
			return unique_cleavage_state_2_count.ToString();
		}

		/// <summary>
		/// P_3: Ratio of semi-tryptic / fully tryptic peptides
		/// </summary>
		/// <returns></returns>
		public string P_3()
		{
			//BUILD RESULT_ID, UNIQUE_SEQ_TABLE
			Dictionary<int, int> ResultID_to_Unique_Seq_ID_Table;
			ResultID_to_Unique_Seq_ID_Table = GetResultIDToSeqIDTable();

			// BUILD UNIQUE_SEQ_TABLE, CLEAVAGE STATE TABLE            
			// Populate a dictionary object via a single query to the database
			Dictionary<int, int> Seq_ID_to_Cleavage_State_Table;
			Seq_ID_to_Cleavage_State_Table = GetSeqIDToCleavageStateTable();

			//SET DB QUERY
			m_DBInterface.setQuery("SELECT Scan,Peptide_Expectation_Value_Log, Charge, temp_xt.Result_ID, Peptide_Sequence, temp_xt_seqtoproteinmap.Cleavage_State "
				+ "FROM `temp_xt`, `temp_xt_resulttoseqmap` "
				+ "JOIN `temp_xt_seqtoproteinmap` ON `temp_xt_resulttoseqmap`.Unique_Seq_ID=`temp_xt_seqtoproteinmap`.Unique_Seq_ID "
				+ "WHERE temp_xt.Peptide_Expectation_Value_Log <= -2.00 "
				+ "AND temp_xt.Result_ID=temp_xt_resulttoseqmap.Result_ID "
				+ "AND temp_xt.random_id=" + m_Random_ID + " "
				+ "AND temp_xt_resulttoseqmap.random_id=" + m_Random_ID + " "
				+ "GROUP BY temp_xt_resulttoseqmap.Result_ID "
				+ "ORDER BY temp_xt.Peptide_Sequence, temp_xt.Group_ID;");

			//DECLARE VARIABLES
			int cleavage_state_1_count = 0;                                                         //COLUMN H
			int cleavage_state_2_count = 0;                                                         //COLUMN I
			int unique_cleavage_state_2_count = 0;                                                  //COLUMN J
			int cleavage_state_2_charge_1 = 0;                                                      //COLUMN K
			int line_count = 0;                                                                     //LINE COUNTER
			string prv_peptide_sequence = "";                                                       //PRV PEPTIDE SEQUENCE

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Charge", "Result_ID", "Peptide_Sequence", "Cleavage_State" };

			//INIT READER
			m_DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((m_DBInterface.readLines(fields, ref m_MeasurementHash)) && (m_MeasurementHash.Count > 0))
			{
				int resultID;
				int seqID;
				int cleavage_state = 0;

				// DETERMINE THE CLEAVAGE STATE
				if (int.TryParse(m_MeasurementHash["Result_ID"].ToString(), out resultID))
				{
					if (ResultID_to_Unique_Seq_ID_Table.TryGetValue(resultID, out seqID))
					{
						Seq_ID_to_Cleavage_State_Table.TryGetValue(seqID, out cleavage_state);   //CONVERT UNIQUE_SEQUENCE_ID TO CLEAVAGE_STATE
					}
				}

				//FOR cleavage_state_1_count [COLUMN H]
				if (cleavage_state == 1)
				{
					//COLUMN H
					cleavage_state_1_count += 1;
				}

				//FOR cleavage_state_2_count [COLUMN I]
				if (cleavage_state == 2)
				{
					//COLUMN I
					cleavage_state_2_count += 1;
				}

				//IF THIS IS THE FIRST LINE
				if (line_count == 0)
				{
					//INC LINE COUNT
					line_count++;

					//FOR unique_cleavage_state_2_count [COLUMN J]
					if (cleavage_state == 2)
					{
						unique_cleavage_state_2_count = 1;
					}

					//FOR cleavage_state_2_charge_1 [COLUMN K]
					if (Convert.ToInt32(m_MeasurementHash["Charge"]) == 1)
					{
						cleavage_state_2_charge_1 = 1;
					}

				}
				else
				{
					//LINE COUNT IS NOT THE FIRST LINE

					//FOR unique_cleavage_state_2_count IF CURRENT PEPTIDE SEQUENCE != PRV PEPTIDE SEQUENCE [COLUMN J]
					if (cleavage_state == 2 && !Convert.ToString(m_MeasurementHash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
					{
						unique_cleavage_state_2_count += 1;
					}

					//FOR cleavage_state_2_charge_1 IF CURRENT PEPTIDE SEQUENCE != PRV PEPTIDE SEQUENCE [COLUMN K]
					if (Convert.ToInt32(m_MeasurementHash["Charge"]) == 1 && !Convert.ToString(m_MeasurementHash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
					{
						cleavage_state_2_charge_1 += 1;
					}

				}

				//UPDATE PREVIOUS VALUES FOR NEXT LOOP
				prv_peptide_sequence = Convert.ToString(m_MeasurementHash["Peptide_Sequence"]);

			}

			//SET ANSWER
			double answer = 0;
			if (cleavage_state_2_count > 0)
				answer = cleavage_state_1_count / (double)cleavage_state_2_count;

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 6th DIGIT
			return answer.ToString("0.000000");
		}

	}
}
