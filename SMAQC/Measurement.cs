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
		// CONSTANTS
		int XTANDEM_LOG_EVALUE_THRESHOLD = -2;
		double MSGF_SPECPROB_THRESHOLD = 1e-12;

		// CREATE DB INTERFACE OBJECT
		private DBWrapper m_DBInterface;

		// MEASUREMENT RESULTS
		private Dictionary<string, string> m_MeasurementResults = new Dictionary<string, string>();

		// RANDOM ID FOR TEMP TABLES
		private int m_Random_ID;

		// SOME MEASUREMENTS HAVE DATA REQUIRED BY OTHERS ... WILL BE STORED HERE
		private Dictionary<string, double> m_ResultsStorage = new Dictionary<string, double>();

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
			//CLEAR Dictionaries
			ClearStorage();
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

		/// <summary>
		/// Clear storage
		/// </summary>
		public void ClearStorage()
		{
			m_MeasurementResults.Clear();
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

		protected bool PeptideScorePassesFilter(double peptideScore)
		{
			if (UsingPHRP && peptideScore <= MSGF_SPECPROB_THRESHOLD)
				return true;

			if (!UsingPHRP && peptideScore <= XTANDEM_LOG_EVALUE_THRESHOLD)
				return true;

			return false;
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
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT temp_PSMs.Scan, t1.FragScanNumber, t1.OptimalPeakApexScanNumber,"
										+ "    temp_scanstats.ScanTime as ScanTime1, t2.ScanTime as ScanTime2 "
										+ " FROM temp_PSMs, temp_scanstats, temp_sicstats as t1 "
										+ " LEFT JOIN temp_scanstats as t2 on t1.OptimalPeakApexScanNumber=t2.ScanNumber "
										+ " WHERE temp_PSMs.Scan = t1.FragScanNumber "
										+ "  AND temp_PSMs.Scan = temp_scanstats.ScanNumber "
										+ "  AND temp_PSMs.random_id=" + m_Random_ID 
										+ "  AND temp_scanstats.random_id=" + m_Random_ID 
										+ "  AND t1.random_id=" + m_Random_ID
										+ "  AND t2.random_id=" + m_Random_ID
										+ "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
										+ " ORDER BY Scan;");
			else
				m_DBInterface.setQuery("SELECT temp_xt.Scan, t1.FragScanNumber, t1.OptimalPeakApexScanNumber,"
										+ "    temp_scanstats.ScanTime as ScanTime1, t2.ScanTime as ScanTime2 "
										+ " FROM temp_xt, temp_scanstats, temp_sicstats as t1 "
										+ " LEFT JOIN temp_scanstats as t2 on t1.OptimalPeakApexScanNumber=t2.ScanNumber "
										+ " WHERE temp_xt.Scan = t1.FragScanNumber "
										+ "  AND temp_xt.Scan = temp_scanstats.ScanNumber "
										+ "  AND temp_xt.random_id=" + m_Random_ID 
										+ "  AND temp_scanstats.random_id=" + m_Random_ID 
										+ "  AND t1.random_id=" + m_Random_ID 
										+ "  AND t2.random_id=" + m_Random_ID
										+ "  AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
										+ " ORDER BY Scan;");

			int difference_sum = 0;                                                             //FOR COLUMN J
			int valid_rows = 0;                                                                 //FOR COLUMN K
			double answer;

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "FragScanNumber", "OptimalPeakApexScanNumber", "ScanTime1", "ScanTime2" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//CALC DIFFERENCE [COLUMN C]
				double temp_difference;
				if (countTailingPeptides)
					temp_difference = (Convert.ToDouble(m_MeasurementResults["ScanTime1"]) - Convert.ToDouble(m_MeasurementResults["ScanTime2"]));
				else
					temp_difference = (Convert.ToDouble(m_MeasurementResults["ScanTime2"]) - Convert.ToDouble(m_MeasurementResults["ScanTime1"]));

				//IF DIFFERENCE >= 4 [COLUMN I]
				if (temp_difference >= 4.00)
				{
					difference_sum += 1;    //ADD 1 TO TOTAL
				}

				//SINCE VALID ROW ... INC [ONLY IF COLUMN C == 1]
				valid_rows++;
			}

			//CALCULATE SOLUTION
			if (valid_rows > 0)
			{
				answer = difference_sum / (double)valid_rows;
				return answer.ToString("0.000000");
			}
			else
				return string.Empty;
		}

		/// <summary>
		/// C-2A: Time period over which 50% of peptides are identified
		/// We also cache various scan numbers associated with filter-passing peptides
		/// </summary>
		/// <returns></returns>
		public string C_2A()
		{
			//SET DB QUERY
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT temp_PSMs.Scan, t1.FragScanNumber as ScanNumber,"
										+ "    temp_scanstats.ScanTime as ScanTime1 "
										+ " FROM temp_PSMs, temp_scanstats, temp_sicstats as t1 "
										+ " WHERE temp_PSMs.Scan = t1.FragScanNumber "
										+ "  AND temp_PSMs.Scan = temp_scanstats.ScanNumber "
										+ "  AND temp_PSMs.random_id=" + m_Random_ID
										+ "  AND temp_scanstats.random_id=" + m_Random_ID
										+ "  AND t1.random_id=" + m_Random_ID + " "
										+ "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
										+ " ORDER BY Scan;");
			else
				m_DBInterface.setQuery("SELECT temp_xt.Scan, t1.FragScanNumber as ScanNumber,"
										+ "    temp_scanstats.ScanTime as ScanTime1 "
										+ " FROM temp_xt, temp_scanstats, temp_sicstats as t1 "
										+ " WHERE temp_xt.Scan = t1.FragScanNumber "
										+ "  AND temp_xt.Scan = temp_scanstats.ScanNumber "
										+ "  AND temp_xt.random_id=" + m_Random_ID 
										+ "  AND temp_scanstats.random_id=" + m_Random_ID
										+ "  AND t1.random_id=" + m_Random_ID + " "
										+ "  AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
										+ " ORDER BY Scan;");

			// This list stores scan numbers and elution times for filter-passing peptides; duplicate scans are not allowed
			SortedList<int, double> lstFilterPassingPeptides = new SortedList<int, double>();

			int scanNumber;
			double scanTime;

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "ScanNumber", "ScanTime1" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{

				// FILTER-PASSING PEPTIDE; Append to the dictionary
				if (int.TryParse(m_MeasurementResults["ScanNumber"], out scanNumber))
				{
					if (double.TryParse(m_MeasurementResults["ScanTime1"], out scanTime))
					{

						if (!lstFilterPassingPeptides.ContainsKey(scanNumber))
						{
							lstFilterPassingPeptides.Add(scanNumber, scanTime);
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
				// ADD TO GLOBAL LIST FOR USE WITH MS_2A/B
				// SCAN_FIRST_FILTER_PASSING_PEPTIDE is the scan number of the first filter-passing peptide
				AddUpdateResultsStorage("SCAN_FIRST_FILTER_PASSING_PEPTIDE", lstFilterPassingPeptides.Keys.Min());
			}

			// CACHE THE SCAN NUMBERS AT THE START AND END OF THE INTEQUARTILE REGION
			AddUpdateResultsStorage("C_2A_REGION_SCAN_START", C2AScanStart);
			AddUpdateResultsStorage("C_2A_REGION_SCAN_END", C2AScanEnd);

			double answer = C2AScanTimeEnd - C2AScanTimeStart;

			//STORE IN GLOBAL RESULTS FOR C_2B
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
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT temp_PSMs.Scan, t1.FragScanNumber as ScanNumber,"
										+ "    temp_scanstats.ScanTime as ScanTime1 "
										+ " FROM temp_PSMs, temp_scanstats, temp_sicstats as t1 "
										+ " WHERE temp_PSMs.Scan = t1.FragScanNumber "
										+ "  AND temp_PSMs.Scan = temp_scanstats.ScanNumber "
										+ "  AND temp_PSMs.random_id=" + m_Random_ID
										+ "  AND temp_scanstats.random_id=" + m_Random_ID
										+ "  AND t1.random_id=" + m_Random_ID
										+ "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
										+ " ORDER BY Scan;");
			else
				m_DBInterface.setQuery("SELECT temp_xt.Scan, t1.FragScanNumber as ScanNumber,"
										+ "    temp_scanstats.ScanTime as ScanTime1 "
										+ " FROM temp_xt, temp_scanstats, temp_sicstats as t1 "
										+ " WHERE temp_xt.Scan = t1.FragScanNumber "
										+ "  AND temp_xt.Scan = temp_scanstats.ScanNumber "
										+ "  AND temp_xt.random_id=" + m_Random_ID
										+ "  AND temp_scanstats.random_id=" + m_Random_ID 
										+ "  AND t1.random_id=" + m_Random_ID 
										+ "  AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
										+ " ORDER BY Scan;");

			// This list keeps track of the scan numbers already processed so that we can avoid double-counting a scan number
			SortedSet<int> lstScansWithFilterPassingIDs = new SortedSet<int>();

			int scanNumber;

			double timeMinutesC2A = GetStoredValue("C_2A_TIME_MINUTES", 0);
			int scanStartC2A = GetStoredValueInt("C_2A_REGION_SCAN_START", 0);
			int scanEndC2A = GetStoredValueInt("C_2A_REGION_SCAN_END", 0);

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "ScanNumber", "ScanTime1" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{

				// FILTER-PASSING PEPTIDE; Append to the dictionary
				if (int.TryParse(m_MeasurementResults["ScanNumber"], out scanNumber))
				{
					if (scanNumber >= scanStartC2A && scanNumber <= scanEndC2A && !lstScansWithFilterPassingIDs.Contains(scanNumber))
					{
						lstScansWithFilterPassingIDs.Add(scanNumber);
					}
				}

			}

			string answerText = string.Empty;

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

			List<int> lstBestScan = new List<int>();												//STORE Best Scan Results
			Dictionary<int, double> dctFWHMinScans = new Dictionary<int, double>();                 //STORE FWHMIN SCANS
			Dictionary<int, int> dctOptimalPeakApexScanNumber = new Dictionary<int, int>();         //STORE OPTIMAL PEAK APEX SCAN NUMBERS
			Dictionary<int, double> dctScanTime = new Dictionary<int, double>();					//STORE TIME
			List<double> result = new List<double>();												//STORE RESULT FOR FINAL CALCULATION

			int running_sum = 1;                                            //STORE RUNNING SUM STARTING AT 1
			string prv_Charge = string.Empty;                                         //INIT PREV CHARGE TO BLANK [REQUIRED FOR COMPARISON]
			string prv_Peptide_Sequence = string.Empty;                               //INIT PREV PEPTIDE SEQUENCE TO BLANK [REQUIRED FOR COMPARISON]
			string prev_Best_Score = string.Empty;                                    //INIT PREV BEST SCORE TO BLANK [REQUIRED FOR COMPARISON]
			double median = 0.00;                                           //INIT MEDIAN

			//SET DB QUERY [REQUIRED TO SORT BY PEPTIDE SEQUENCE]
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT Scan, Charge, Peptide_Sequence, MSGFSpecProb AS Peptide_Score"
									+ " FROM temp_PSMs "
									+ " WHERE random_id=" + m_Random_ID
									+ " ORDER BY Peptide_Sequence, Charge, Scan");
			else
				m_DBInterface.setQuery("SELECT Scan, Charge, Peptide_Sequence, Peptide_Expectation_Value_Log AS Peptide_Score"
									+ " FROM temp_xt "
									+ " WHERE random_id=" + m_Random_ID
									+ " ORDER BY Peptide_Sequence, Charge ,Scan");

			//DECLARE FIELDS TO READ FROM
			string[] fields1 = { "Scan", "Charge", "Peptide_Sequence", "Peptide_Score" };

			//INIT READER
			m_DBInterface.initReader();

			//FETCH COLUMNS Q,R NOW

			//READ ROWS
			while ((m_DBInterface.readLines(fields1, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//FIND COLUMN Q
				string Best_Score = string.Empty;

				//IF PREVIOUS PEPTIDE SEQUENCES == EACH OTHER && PREVIOUS CHARGES == EACH OTHER
				if (prv_Peptide_Sequence.Equals(Convert.ToString(m_MeasurementResults["Peptide_Sequence"])) && prv_Charge.Equals(Convert.ToString(m_MeasurementResults["Charge"])))
				{

					//TAKE MIN [EITHER PREVIOUS BEST EVALUE OR CURRENT PEPTIDE EXPECTATION VALUE
					if (Convert.ToDouble(m_MeasurementResults["Peptide_Score"]) > Convert.ToDouble(prev_Best_Score))
					{
						Best_Score = prev_Best_Score;
					}
					else
					{
						Best_Score = Convert.ToString(m_MeasurementResults["Peptide_Score"]);
					}
				}
				else
				{
					Best_Score = Convert.ToString(m_MeasurementResults["Peptide_Score"]);
				}

				//NOW FIND COLUMN R IF COLUMN U IS == TRUE
				if (Best_Score.Equals(Convert.ToString(m_MeasurementResults["Peptide_Score"])))
				{
					lstBestScan.Add(Convert.ToInt32(m_MeasurementResults["Scan"]));
				}

				//UPDATE PREVIOUS VALUES FOR NEXT LOOP
				prv_Charge = Convert.ToString(m_MeasurementResults["Charge"]);
				prv_Peptide_Sequence = Convert.ToString(m_MeasurementResults["Peptide_Sequence"]);
				prev_Best_Score = Best_Score;
			}

			//NOW SORT OUR VALUES SO THEY ARE IN THE CORRECT ORDER-
			lstBestScan.Sort();

			//SET DB QUERY
			m_DBInterface.setQuery("SELECT FragScanNumber, FWHMInScans, OptimalPeakApexScanNumber FROM temp_sicstats WHERE temp_sicstats.random_id=" + m_Random_ID);

			//DECLARE FIELDS TO READ FROM
			string[] fields2 = { "FragScanNumber", "FWHMInScans", "OptimalPeakApexScanNumber" };

			//INIT READER
			m_DBInterface.initReader();

			//FETCH COLUMNS D-F

			//READ ROWS
			while ((m_DBInterface.readLines(fields2, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				int fragScanNumber = Convert.ToInt32(m_MeasurementResults["FragScanNumber"]);

				dctFWHMinScans.Add(fragScanNumber, Convert.ToDouble(m_MeasurementResults["FWHMInScans"]));
				dctOptimalPeakApexScanNumber.Add(fragScanNumber, Convert.ToInt32(m_MeasurementResults["OptimalPeakApexScanNumber"]));
			}

			//SET DB QUERY
			m_DBInterface.setQuery("SELECT temp_scanstats.ScanNumber, temp_scanstats.ScanTime FROM temp_scanstats WHERE temp_scanstats.random_id=" + m_Random_ID);

			//DECLARE FIELDS TO READ FROM
			string[] fields3 = { "ScanNumber", "ScanTime" };

			//INIT READER
			m_DBInterface.initReader();

			//FETCH COLUMNS H-I		
			//READ ROWS
			while ((m_DBInterface.readLines(fields3, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				dctScanTime.Add(Convert.ToInt32(m_MeasurementResults["ScanNumber"]), Convert.ToDouble(m_MeasurementResults["ScanTime"]));
			}

			//NOW START THE ACTUAL MEASUREMENT CALCULATION

			//LOOP THROUGH BESTSCAN
			for (int i = 0; i < lstBestScan.Count; i++)
			{
				//FIND INDEX + OPTIMAL PEAK APEX SCAN +- FWHMIN FOR EACH RESULT [COLUMNS: M,O]				
				int OptimalPeakApexScanMinusFWHM = dctOptimalPeakApexScanNumber[lstBestScan[i]] - Convert.ToInt32(Math.Ceiling(dctFWHMinScans[lstBestScan[i]] / 2));
				int OptimalPeakApexScanPlusFWHM = dctOptimalPeakApexScanNumber[lstBestScan[i]] + Convert.ToInt32(Math.Ceiling(dctFWHMinScans[lstBestScan[i]] / 2));

				//FIND OTHER COLUMNS [N,P, Q,R,T]

				double start_time;
				double end_time;

				if (dctScanTime.TryGetValue(OptimalPeakApexScanMinusFWHM, out start_time))
				{
					if (dctScanTime.TryGetValue(OptimalPeakApexScanPlusFWHM, out end_time))
					{
							
						double end_minus_start = end_time - start_time;
						double end_minus_start_in_secs = end_minus_start * 60;
						double running_percent = (double)running_sum / (double)lstBestScan.Count;

						//CHECK FOR VALID RANGE DATA THEN ADD TO OUR RESULTS
						if (running_percent >= startScanRelative && running_percent <= endScanRelative)
						{
							//WE ARE WITHING OUR VALID RANGE ... SO ADD end_minus_start_in_secs TO OUR LIST [COLUMN U]
							result.Add(end_minus_start_in_secs);
						}

					}
				}						

				//INCREMENT RUNNING SUM [COLUMN S]
				running_sum++;
			}

			string resultText = string.Empty;

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
			// Keys are the number of spectra that a peptide was observed in (passing filters) and values are the number of peptides identified by Key spectra</param>
			Dictionary<int, int> dctPeptideSamplingStats;
			double result = 0;																		//SOLUTION

			DS_1_Shared(out dctPeptideSamplingStats);

			//NOW CALCULATE DS_1B
			//Return 0 if number of peptides identified with 3 spectra is 0

			int numPeptidesWithOneSpectrum;
			int numPeptidesWithTwoSpectra;

			if (dctPeptideSamplingStats.TryGetValue(2, out numPeptidesWithTwoSpectra) && numPeptidesWithTwoSpectra > 0)
			{
				if (!dctPeptideSamplingStats.TryGetValue(1, out numPeptidesWithOneSpectrum))
					numPeptidesWithOneSpectrum = 0;

				result = numPeptidesWithOneSpectrum / (double)numPeptidesWithTwoSpectra;
			}

			return result.ToString("0.000");
		}

		/// <summary>
		/// DS-1B: Count of peptides with two spectra / count of peptides with three spectra
		/// </summary>
		/// <returns></returns>
		public string DS_1B()
		{
			// Keys are the number of spectra that a peptide was observed in (passing filters) and values are the number of peptides identified by Key spectra</param>
			Dictionary<int, int> dctPeptideSamplingStats;
			double result = 0;																		//SOLUTION

			DS_1_Shared(out dctPeptideSamplingStats);

			//NOW CALCULATE DS_1B
			//Return 0 if number of peptides identified with 3 spectra is 0

			int numPeptidesWithTwoSpectra;
			int numPeptidesWithThreeSpectra;

			if (dctPeptideSamplingStats.TryGetValue(3, out numPeptidesWithThreeSpectra) && numPeptidesWithThreeSpectra > 0)
			{
				if (!dctPeptideSamplingStats.TryGetValue(2, out numPeptidesWithTwoSpectra))
					numPeptidesWithTwoSpectra = 0;

				result = numPeptidesWithTwoSpectra / (double)numPeptidesWithThreeSpectra;
			}
		
			return result.ToString("0.000");

		}

		/// <summary>
		/// Computes stats on the number of spectra by which each peptide was identified
		/// </summary>
		/// <param name="dctPeptideSamplingStats">Dictionary where keys are the number of spectra that a peptide was observed in (passing filters) and values are the number of peptides identified by Key spectra</param>
		protected void DS_1_Shared(out Dictionary<int, int> dctPeptideSamplingStats)
		{
			//SET DB QUERY
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT Spectra, Count(*) AS Peptides "
									+ " FROM ( SELECT Unique_Seq_ID, COUNT(*) AS Spectra "
									+ "        FROM ( SELECT Unique_Seq_ID, Scan "
									+ "               FROM temp_PSMs "
									+ "               WHERE random_id=" + m_Random_ID
									+ "                 AND MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
									+ "               GROUP BY Unique_Seq_ID, Scan ) DistinctQ "
									+ "        GROUP BY Unique_Seq_ID ) CountQ "
									+ " GROUP BY Spectra;");
			else
				m_DBInterface.setQuery("SELECT Spectra, Count(*) AS Peptides "
									+ " FROM ( SELECT Unique_Seq_ID, COUNT(*) AS Spectra "
									+ "        FROM ( SELECT Unique_Seq_ID, Scan "
									+ "               FROM temp_xt JOIN temp_xt_resulttoseqmap "
									+ "                      ON temp_xt.result_id = temp_xt_resulttoseqmap.result_id "
									+ "               WHERE temp_xt.random_id=" + m_Random_ID
									+ "                 AND temp_xt_resulttoseqmap.random_id=" + m_Random_ID
			                        + "                 AND Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
									+ "               GROUP BY Unique_Seq_ID, Scan ) DistinctQ "
									+ "        GROUP BY Unique_Seq_ID ) CountQ "
									+ " GROUP BY Spectra;");



			dctPeptideSamplingStats = new Dictionary<int, int>();

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Spectra", "Peptides" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				int spectra = Convert.ToInt32(m_MeasurementResults["Spectra"]);
				int peptides = Convert.ToInt32(m_MeasurementResults["Peptides"]);

				dctPeptideSamplingStats.Add(spectra, peptides);
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
				+ " FROM temp_scanstats "
				+ " WHERE temp_scanstats.random_id=" + m_Random_ID
				+ " ORDER BY ScanNumber;");

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

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				scanNumber = Convert.ToInt32(m_MeasurementResults["ScanNumber"]);
				scanType = Convert.ToInt32(m_MeasurementResults["ScanType"]);

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
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT Scan, MSGFSpecProb AS Peptide_Score, Peptide_MH, Charge "
									+ " FROM temp_PSMs "
									+ " WHERE random_id=" + m_Random_ID + ";");
			else
				m_DBInterface.setQuery("SELECT Scan, Peptide_Expectation_Value_Log AS Peptide_Score, Peptide_MH, Charge "
									+ " FROM temp_xt "
									+ " WHERE random_id=" + m_Random_ID + ";");

			//DECLARE VARIABLES
			List<double> MZ_List = new List<double>();                                          //MZ LIST
			List<double> MZ_Final;																//MZ Final List
			Dictionary<double, int> tempDict = new Dictionary<double, int>();                   //TEMP ... TO REMOVE DUPLICATES
			double median = 0.00;                                                               //STORE MEDIAN

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Score", "Peptide_MH", "Charge" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{

				double peptideScore;
				if (double.TryParse(m_MeasurementResults["Peptide_Score"], out peptideScore))
				{
					if(PeptideScorePassesFilter(peptideScore))
					{
						//COMPUTE MZ VALUE
						double temp_mz = PHRPReader.clsPeptideMassCalculator.ConvoluteMass(Convert.ToDouble(m_MeasurementResults["Peptide_MH"]), 1, Convert.ToInt32(m_MeasurementResults["Charge"]));					

						//ADD TO MZ_LIST
						MZ_List.Add(temp_mz);
					}
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
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT Scan, Charge "
						+ " FROM temp_PSMs "
						+ " WHERE random_id=" + m_Random_ID
						+   " AND MSGFSpecProb < " + MSGF_SPECPROB_THRESHOLD
						+ " Order by Scan;");
			else
				m_DBInterface.setQuery("SELECT Scan, Charge "
						+ " FROM temp_xt "
						+ " WHERE random_id=" + m_Random_ID
						+   " AND Peptide_Expectation_Value_Log < " + XTANDEM_LOG_EVALUE_THRESHOLD
						+ " Order by Scan;");

			//DECLARE VARIABLES
			count_ones = 0;                                                             //TOTAL # OF 1's
			count_twos = 0;                                                             //TOTAL # OF 2's
			count_threes = 0;                                                           //TOTAL # OF 3's
			count_fours = 0;                                                            //TOTAL # OF 4's

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Charge" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//CONVERT CHARGE TO INT FOR SWITCH()
				int charge;
				if (int.TryParse(m_MeasurementResults["Charge"], out charge))
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

		/// <summary>
		/// MS1_1: Median MS1 ion injection time
		/// </summary>
		/// <returns></returns>
		public string MS1_1()
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT temp_scanstats.ScanNumber, temp_scanstats.ScanType, temp_scanstatsex.Ion_Injection_Time "
				+ " FROM temp_scanstats, temp_scanstatsex "
				+ " WHERE temp_scanstats.ScanNumber = temp_scanstatsex.ScanNumber "
				+  "  AND temp_scanstatsex.random_id = " + m_Random_ID 
				+  "  AND temp_scanstats.random_id = " + m_Random_ID 
				+ " ORDER BY temp_scanstats.ScanNumber;");

			//DECLARE VARIABLES
			List<double> Filter = new List<double>();                               //FILTER LIST
			double median = 0.00;                                                   //RESULT

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "ScanNumber", "ScanType", "Ion_Injection_Time" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//SCAN TYPE == 1
				if (Convert.ToDouble(m_MeasurementResults["ScanType"]) == 1)
				{
					//ADD TO FILTER LIST
					Filter.Add(Convert.ToDouble(m_MeasurementResults["Ion_Injection_Time"]));
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
				+ " FROM temp_scanstats"
				+ " WHERE temp_scanstats.random_id=" + m_Random_ID + ";");

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

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{

				List_ScanNumber.Add(Convert.ToInt32(m_MeasurementResults["ScanNumber"]));
				List_ScanType.Add(Convert.ToInt32(m_MeasurementResults["ScanType"]));
				List_BasePeakSignalToNoiseRatio.Add(Convert.ToDouble(m_MeasurementResults["BasePeakSignalToNoiseRatio"]));
				List_TotalIonIntensity.Add(Convert.ToDouble(m_MeasurementResults["TotalIonIntensity"]));
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
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT temp_PSMs.Result_ID, temp_sicstats.FragScanNumber, temp_sicstats.PeakMaxIntensity"
					+ " FROM temp_sicstats, temp_PSMs"
					+ " WHERE temp_sicstats.FragScanNumber=temp_PSMs.Scan"
					+ "   AND temp_sicstats.random_id=" + m_Random_ID
					+ "   AND temp_PSMs.random_id=" + m_Random_ID
					+ "   AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
					+ " ORDER BY temp_sicstats.PeakMaxIntensity, temp_PSMs.Result_ID DESC;");
			else
				m_DBInterface.setQuery("SELECT temp_xt.Result_ID, temp_sicstats.FragScanNumber, temp_sicstats.PeakMaxIntensity"
					+ " FROM temp_sicstats, temp_xt"
					+ " WHERE temp_sicstats.FragScanNumber=temp_xt.Scan"
					+ "   AND temp_sicstats.random_id=" + m_Random_ID
					+ "   AND temp_xt.random_id=" + m_Random_ID
					+ "   AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
					+ " ORDER BY temp_sicstats.PeakMaxIntensity, temp_xt.Result_ID DESC;");

			//DECLARE VARIABLES
			result = new List<double>();																		//STORES FILTER LIST [COLUMN D]
			List<double> MPI_list = new List<double>();                                                         //STORES MAX PEAK INTENSITY FOR 5-95%
			List<double> temp_list_mpi = new List<double>();                                                    //STORES PeakMaxIntensity FOR FUTURE CALCULATIONS
			List<int> temp_list_running_sum = new List<int>();                                                  //STORES RUNNING SUM FOR FUTURE CALCULATIONS
			int max_running_sum = 0;                                                                            //STORES THE LARGEST/MAX RUNNING SUM OF COLUMN E


			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Result_ID", "FragScanNumber", "PeakMaxIntensity" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{	
				//INC TOTAL RUNNING SUM
				max_running_sum++;

				//ADD TO FILTER LIST
				result.Add(Convert.ToDouble(m_MeasurementResults["PeakMaxIntensity"]));

				//ADD TO TEMP LIST TO PROCESS LATER AS WE FIRST NEED TO FIND THE MAX RUNNING SUM WHICH IS DONE AT THE END
				temp_list_mpi.Add(Convert.ToDouble(m_MeasurementResults["PeakMaxIntensity"]));                   //ADD MPI
				temp_list_running_sum.Add(max_running_sum);                                                 //ADD CURRENT RUNNING SUM
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
			bool bottom50Pct = false;
			return DS_3_Shared(bottom50Pct);
		}

			/// <summary>
		/// DS_3B: Median of MS1 max / MS1 sampled abundance; limit to bottom 50% of peptides by abundance
		/// </summary>
		/// <returns></returns>
		public string DS_3B()
		{
			bool bottom50Pct = true;
			return DS_3_Shared(bottom50Pct);
		}

		/// <summary>
		/// Median of MS1 max / MS1 sampled abundance
		/// </summary>
		/// <param name="bottom50Pct">Set to true to limit to the bottom 50% of peptides by abundance</param>
		/// <returns></returns>
		protected string DS_3_Shared(bool bottom50Pct)
		{
			//SET DB QUERY
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT temp_PSMs.Result_ID, temp_sicstats.FragScanNumber, ParentIonIntensity, PeakMaxIntensity, temp_PSMs.MSGFSpecProb AS Peptide_Score"
					+ " FROM temp_sicstats, temp_PSMs "
					+ " WHERE temp_sicstats.FragScanNumber=temp_PSMs.Scan"
					+ "   AND temp_sicstats.random_id=" + m_Random_ID
					+ "   AND temp_PSMs.random_id=" + m_Random_ID
					+ " ORDER BY temp_sicstats.FragScanNumber, temp_PSMs.Result_ID DESC;");
			else
				m_DBInterface.setQuery("SELECT temp_xt.Result_ID, temp_sicstats.FragScanNumber, ParentIonIntensity, PeakMaxIntensity, temp_xt.Peptide_Expectation_Value_Log AS Peptide_Score"
					+ " FROM temp_sicstats, temp_xt "
					+ " WHERE temp_sicstats.FragScanNumber=temp_xt.Scan"
					+ "   AND temp_sicstats.random_id=" + m_Random_ID
					+ "   AND temp_xt.random_id=" + m_Random_ID
					+ " ORDER BY temp_sicstats.FragScanNumber, temp_xt.Result_ID DESC;");

			//DECLARE VARIABLES
			Dictionary<int, int> dctResultIDFragScanMap = new Dictionary<int, int>();                           //STORES [RESULT_ID, FragScanNumber] SO WE CAN HAVE DUP FSN'S
			Dictionary<int, bool> dctResultFilterFlag = new Dictionary<int, bool>();                            //STORES [RESULT_ID, true/false] SO WE CAN DETERMINE IF WE PASSED THE FILTER
			Dictionary<int, double> Lookup_Table_KV = new Dictionary<int, double>();                            //STORES [RESULT_ID, VALUE] SO WE CAN SORT BY VALUE
			List<double> result_PMIPII_Filtered = new List<double>();                                           //STORES TABLE VALUES COLUMN K
			List<double> result_VBPMIPII_Filtered = new List<double>();                                         //STORES TABLE VALUES COLUMN N
			int max_running_sum = 0;                                                                            //STORES THE LARGEST/MAX RUNNING SUM OF COLUMN L
			int running_sum = 0;                                                                                //STORES THE CURRENT RUNNING SUM OF COLUMN L
			double median = 0.00;                                                                               //INIT MEDIAN
			double parentIonIntensity = 0;
			double ratioPeakMaxToParentIonIntensity = 0;

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Result_ID", "FragScanNumber", "ParentIonIntensity", "PeakMaxIntensity", "Peptide_Score" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				int intResultID = Convert.ToInt32(m_MeasurementResults["Result_ID"]);

				//STORE RESULT_ID, SCAN [ALLOWS FOR US TO HAVE DUPLICATES WITHOUT CRASHING]
				dctResultIDFragScanMap.Add(intResultID, Convert.ToInt32(m_MeasurementResults["FragScanNumber"]));

				//STORE RESULT_ID, VALUE [SO WE CAN THEN SORT BY VALUE]
				parentIonIntensity = Convert.ToDouble(m_MeasurementResults["ParentIonIntensity"]);
				if (parentIonIntensity > 0)
					ratioPeakMaxToParentIonIntensity = Convert.ToDouble(m_MeasurementResults["PeakMaxIntensity"]) / parentIonIntensity;
				else
					ratioPeakMaxToParentIonIntensity = 0;

				Lookup_Table_KV.Add(intResultID, ratioPeakMaxToParentIonIntensity);


				// Compare the peptide_score vs. the threshold
				double peptideScore;
				if (double.TryParse(m_MeasurementResults["Peptide_Score"], out peptideScore) && PeptideScorePassesFilter(peptideScore))
				{
					//SET FILTER TO 1
					dctResultFilterFlag.Add(intResultID, true);

					//INCREMENT MAX RUNNING SUM
					max_running_sum++;
				}
				else
				{
					//SET FILTER TO 0
					dctResultFilterFlag.Add(intResultID, false);
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
				int Scan = dctResultIDFragScanMap[key];
				//Console.WriteLine("Scan={0} && Value={1} && Filter={2}", Scan, Lookup_Table_KV[key], Filter_Result[key]);

				//IF VALID FILTER
				if (dctResultFilterFlag[key])
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
			if (bottom50Pct)
				median = ComputeMedian(result_VBPMIPII_Filtered);
			else
				median = ComputeMedian(result_PMIPII_Filtered);

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
				+ " FROM temp_scanstats "
				+ " WHERE temp_scanstats.random_id=" + m_Random_ID + ";");

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

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//DETERMINE Compare_Only_MS1
				if (Convert.ToInt32(m_MeasurementResults["ScanType"]) == 1)
				{
					//SET COMPARE ONLY MS1 TO BPI
					bpiCurrent = Convert.ToDouble(m_MeasurementResults["BasePeakIntensity"]);

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
			List<double> DelMPPM;                                         //STORE FILTERED VALUES [COLUMN I]

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
			List<double> DelMPPM;                                         //STORE FILTERED VALUES [COLUMN I]

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
			List<double> DelMPPM;                                         //STORE FILTERED VALUES [COLUMN I]

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
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT temp_PSMs.Scan, temp_PSMs.Peptide_MH, temp_PSMs.Charge, temp_sicstats.MZ, temp_PSMs.DelM_Da, temp_PSMs.DelM_PPM"
						+ " FROM temp_PSMs, temp_sicstats"
						+ " WHERE temp_sicstats.FragScanNumber=temp_PSMs.Scan AND temp_sicstats.random_id=" + m_Random_ID + " AND temp_PSMs.random_id=" + m_Random_ID
						+   " AND temp_PSMs.MSGFSpecProb < " + MSGF_SPECPROB_THRESHOLD
						+ " ORDER BY temp_PSMs.Scan;");
			else
				m_DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.Peptide_MH, temp_xt.Charge, temp_sicstats.MZ, 0 AS DelM_Da, temp_xt.DelM_PPM "
						+ " FROM temp_xt, temp_sicstats"
						+ " WHERE temp_sicstats.FragScanNumber=temp_xt.Scan AND temp_sicstats.random_id=" + m_Random_ID + " AND temp_xt.random_id=" + m_Random_ID
						+   " AND temp_xt.Peptide_Expectation_Value_Log < " + XTANDEM_LOG_EVALUE_THRESHOLD
						+ " ORDER BY temp_xt.Scan;");

			//DECLARE VARIABLES
			double massC13 = 1.00335483;
			FilteredDelM = new List<double>();                                    //STORE FILTERED VALUES [COLUMN G]
			AbsFilteredDelM = new List<double>();                                 //STORE FILTERED VALUES [COLUMN H]
			DelMPPM = new List<double>();                                          //STORE FILTERED VALUES [COLUMN I]

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_MH", "Charge", "MZ", "DelM_Da", "DelM_PPM" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//CALC THEORETICAL VALUE [COLUMN F], which would be theoretical m/z of the peptide
				// Instead, calculate theoretical monoisotopic mass of the peptide

				double theoMonoMass = PHRPReader.clsPeptideMassCalculator.ConvoluteMass(Convert.ToDouble(m_MeasurementResults["Peptide_MH"]), 1, 0);

				// Compute observed precursor mass, as monoisotopic mass					
				int observedCharge = Convert.ToInt16(m_MeasurementResults["Charge"]);
				double observedMonoMass = PHRPReader.clsPeptideMassCalculator.ConvoluteMass(Convert.ToDouble(m_MeasurementResults["MZ"]), observedCharge, 0);

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

		/// <summary>
		/// MS2_1: Median MS2 ion injection time
		/// </summary>
		/// <returns></returns>
		public string MS2_1()
		{
			//SET DB QUERY
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT temp_PSMs.Scan, temp_scanstatsex.Ion_Injection_Time "
					+ " FROM temp_PSMs, temp_scanstatsex "
					+ " WHERE temp_PSMs.Scan=temp_scanstatsex.ScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_scanstatsex.random_id=" + m_Random_ID
					+ "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
					+ " ORDER BY temp_PSMs.Scan;");
			else
				m_DBInterface.setQuery("SELECT temp_xt.Scan, temp_scanstatsex.Ion_Injection_Time "
					+ " FROM temp_xt, temp_scanstatsex "
					+ " WHERE temp_xt.Scan=temp_scanstatsex.ScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_scanstatsex.random_id=" + m_Random_ID
					+ "  AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
					+ " ORDER BY temp_xt.Scan;");

			//DECLARE VARIABLES
			List<double> FilterList = new List<double>();                                       //FILTERED LIST [COLUMN P]
			double median = 0.00;                                                               //STORE MEDIAN

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Ion_Injection_Time" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//ADD TO FILTERED LIST
				FilterList.Add(Convert.ToDouble(m_MeasurementResults["Ion_Injection_Time"]));
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
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT temp_PSMs.Scan, temp_scanstats.BasePeakSignalToNoiseRatio "
					+ " FROM temp_PSMs, temp_scanstats "
					+ " WHERE temp_PSMs.Scan=temp_scanstats.ScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_scanstats.random_id=" + m_Random_ID
					+ "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
					+ " ORDER BY temp_PSMs.Scan;");
			else
				m_DBInterface.setQuery("SELECT temp_xt.Scan, temp_scanstats.BasePeakSignalToNoiseRatio "
					+ " FROM temp_xt, temp_scanstats "
					+ " WHERE temp_xt.Scan=temp_scanstats.ScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_scanstats.random_id=" + m_Random_ID
					+ "  AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
					+ " ORDER BY temp_xt.Scan;");

			//DECLARE VARIABLES
			List<double> FilterList = new List<double>();                                       //FILTERED LIST [COLUMN H]
			List<double> FinishedList = new List<double>();                                     //FINISHED LIST [COLUMN J]
			double median = 0.00;                                                               //STORE MEDIAN
			int current_count = 0;                                                              //CURRENT COUNTER

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "BasePeakSignalToNoiseRatio" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//ADD TO FILTERED LIST
				FilterList.Add(Convert.ToDouble(m_MeasurementResults["BasePeakSignalToNoiseRatio"]));
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
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT temp_PSMs.Scan, temp_scanstats.IonCountRaw "
					+ " FROM temp_PSMs, temp_scanstats "
					+ " WHERE temp_PSMs.Scan=temp_scanstats.ScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_scanstats.random_id=" + m_Random_ID
					+ "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
					+ " ORDER BY temp_PSMs.Scan;");
			else
				m_DBInterface.setQuery("SELECT temp_xt.Scan, temp_scanstats.IonCountRaw "
					+ " FROM temp_xt, temp_scanstats "
					+ " WHERE temp_xt.Scan=temp_scanstats.ScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_scanstats.random_id=" + m_Random_ID
					+ "  AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
					+ " ORDER BY temp_xt.Scan;");

			//DECLARE VARIABLES
			List<double> FilterList = new List<double>();                                       //FILTERED LIST [COLUMN M]
			double median = 0.00;                                                               //STORE MEDIAN

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "IonCountRaw" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//ADD TO FILTERED LIST
				FilterList.Add(Convert.ToDouble(m_MeasurementResults["IonCountRaw"]));
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
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT COUNT(*) as MS2ScanCount "
					+ " FROM temp_PSMs, temp_sicstats "
					+ " WHERE temp_PSMs.Scan=temp_sicstats.FragScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_sicstats.random_id=" + m_Random_ID + " "
					+ ";");
			else
				m_DBInterface.setQuery("SELECT COUNT(*) as MS2ScanCount "
					+ " FROM temp_xt, temp_sicstats "
					+ " WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_sicstats.random_id=" + m_Random_ID + " "
					+ ";");

			//DECLARE FIELDS TO READ FROM
			string[] fields_temp = { "MS2ScanCount" };

			//INIT READER
			m_DBInterface.initReader();

			//READ LINE
			m_DBInterface.readSingleLine(fields_temp, ref m_MeasurementResults);
			int scanCountMS2 = Convert.ToInt32(m_MeasurementResults["MS2ScanCount"]);                         //STORE TOTAL MS2 SCAN COUNT

			//SET DB QUERY
			//NOTE THAT WE SORT BY ASCENDING PeakMaxIntensity
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT temp_PSMs.Scan, temp_sicstats.PeakMaxIntensity,temp_PSMs.MSGFSpecProb AS Peptide_Score "
					+ " FROM temp_PSMs, temp_sicstats"
					+ " WHERE temp_PSMs.Scan=temp_sicstats.FragScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_sicstats.random_id=" + m_Random_ID
					+ " ORDER BY temp_sicstats.PeakMaxIntensity;");
			else
				m_DBInterface.setQuery("SELECT temp_xt.Scan, temp_sicstats.PeakMaxIntensity, temp_xt.Peptide_Expectation_Value_Log AS Peptide_Score"
					+ " FROM temp_xt, temp_sicstats"
					+ " WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_sicstats.random_id=" + m_Random_ID
					+ " ORDER BY temp_sicstats.PeakMaxIntensity;");

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
			string[] fields = { "Scan", "Peptide_Score", "PeakMaxIntensity" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//DID IT PASS OUR FILTER?
				bool passed_filter = false;

				//CALCULATE COLUMN G

				// Compare the peptide_score vs. the threshold
				double peptideScore;
				if (double.TryParse(m_MeasurementResults["Peptide_Score"], out peptideScore) && PeptideScorePassesFilter(peptideScore))
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
		/// P_1A: Median peptide ID score (X!Tandem hyperscore or -Log10(MSGF_SpecProb))
		/// </summary>
		/// <returns></returns>
		public string P_1A()
		{
			//SET DB QUERY
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT Scan, MSGFSpecProb AS Peptide_Score"
					+ " FROM temp_PSMs "
					+ " WHERE random_id=" + m_Random_ID
					+ " ORDER BY Scan;");
			else
				m_DBInterface.setQuery("SELECT Scan, Peptide_Hyperscore AS Peptide_Score"
					+ " FROM temp_xt "
					+ " WHERE random_id=" + m_Random_ID
					+ " ORDER BY Scan;");

			//DECLARE VARIABLES
			List<double> Peptide_score_List = new List<double>();                          // Track X!Tandem Hyperscore or -Log10(MSGFSpecProb)
			double median = 0.00;                                                          // INIT MEDIAN VALUE

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Score" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//CALCULATE COLUMN B + ADD TO LIST
				double peptideScore;
				if (double.TryParse(m_MeasurementResults["Peptide_Score"], out peptideScore))
				{
					if (UsingPHRP)
					{
						// Take Minus Log10 of the score to give a value between 0 and roughly 30 (where larger values are better)
						peptideScore = -Math.Log10(peptideScore);
					}
					Peptide_score_List.Add(peptideScore);
				}
			}

			//NOW CALCULATE MEDIAN
			median = ComputeMedian(Peptide_score_List);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 3rd DIGIT
			return median.ToString("0.00");
		}

		/// <summary>
		/// P_1B: Median peptide ID score (X!Tandem Peptide_Expectation_Value_Log(e) or Log10(MSGF_SpecProb)
		/// </summary>
		/// <returns></returns>
		public string P_1B()
		{
			//SET DB QUERY
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT Scan, MSGFSpecProb AS Peptide_Score"
					+ " FROM temp_PSMs "
					+ " WHERE random_id=" + m_Random_ID
					+ " ORDER BY Scan;");
			else
				m_DBInterface.setQuery("SELECT Scan, Peptide_Expectation_Value_Log AS Peptide_Score"
					+ " FROM temp_xt "
					+ " WHERE random_id=" + m_Random_ID
					+ " ORDER BY Scan;");

			//DECLARE VARIABLES
			List<double> Peptide_score_List = new List<double>();                          // Track X!Tandem Peptide_Expectation_Value_Log or Log10(MSGFSpecProb)
			double median = 0.00;                                                          // INIT MEDIAN VALUE

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Score" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//CALCULATE COLUMN C + ADD TO LIST
				double peptideScore;
				if (double.TryParse(m_MeasurementResults["Peptide_Score"], out peptideScore))
				{
					if (UsingPHRP)
					{
						// Take Log10 of the score to give a value between roughly -30 and 0 (where more negative values are better)
						peptideScore = Math.Log10(peptideScore);
					}
					Peptide_score_List.Add(peptideScore);
				}
			}

			//NOW CALCULATE MEDIAN
			median = ComputeMedian(Peptide_score_List);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 3rd DIGIT           
			return median.ToString("0.000");
		}

		/// <summary>
		/// P_2A: Number of tryptic peptides; total spectra count
		/// </summary>
		/// <returns></returns>
		public string P_2A()
		{
			//SET DB QUERY
			if (UsingPHRP)
					m_DBInterface.setQuery("SELECT Cleavage_State, Count(*) AS Spectra "
										+ " FROM ( SELECT Scan, Max(Cleavage_State) AS Cleavage_State "
										+ "        FROM temp_PSMs "
										+ "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
										+ "          AND random_id=" + m_Random_ID
										+ "        GROUP BY Scan ) StatsQ "
										+ " GROUP BY Cleavage_State;");

			else
				m_DBInterface.setQuery("SELECT Cleavage_State, Count(*) AS Spectra "
									+ " FROM ( SELECT temp_xt.Scan, Max(temp_xt_seqtoproteinmap.cleavage_state) AS Cleavage_State "
									+ "        FROM temp_xt "
									+ "             INNER JOIN temp_xt_resulttoseqmap ON temp_xt.result_id = temp_xt_resulttoseqmap.result_id "
									+ "             INNER JOIN temp_xt_seqtoproteinmap ON temp_xt_resulttoseqmap.unique_seq_id = temp_xt_seqtoproteinmap.unique_seq_id "
									+ "        WHERE temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
									+ "          AND temp_xt.random_id=" + m_Random_ID
									+ "          AND temp_xt_resulttoseqmap.random_id=" + m_Random_ID 
									+ "          AND temp_xt_seqtoproteinmap.random_id=" + m_Random_ID 
									+ "        GROUP BY temp_xt.Scan ) StatsQ "
									+ " GROUP BY Cleavage_State;");

			//DECLARE VARIABLES
			Dictionary<int, int> dctPSMStats = new Dictionary<int, int>();

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Cleavage_State", "Spectra" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				dctPSMStats.Add(Convert.ToInt32(m_MeasurementResults["Cleavage_State"]), Convert.ToInt32(m_MeasurementResults["Spectra"]));
			}			

			int spectraCount;
			if (dctPSMStats.TryGetValue(2, out spectraCount))
				return spectraCount.ToString();
			else
				return "0";
			
		}

		/// <summary>
		/// P_2B: Number of tryptic peptides; unique peptide & charge count
		/// </summary>
		/// <returns></returns>
		public string P_2B()
		{
			bool groupByCharge = true;
			Dictionary<int, int> dctPeptideStats = SummarizePSMs(groupByCharge);

			int peptideCount;
			if (dctPeptideStats.TryGetValue(2, out peptideCount))
				return peptideCount.ToString();
			else
				return "0";			
		}

		/// <summary>
		/// P_2C: Number of tryptic peptides; unique peptide count
		/// </summary>
		/// <returns></returns>
		public string P_2C()
		{

			bool groupByCharge = false;
			Dictionary<int, int> dctPeptideStats = SummarizePSMs(groupByCharge);

			int peptideCount;
			if (dctPeptideStats.TryGetValue(2, out peptideCount))
				return peptideCount.ToString();
			else
				return "0";			

		}

		/// <summary>
		/// P_3: Ratio of semi-tryptic / fully tryptic peptides
		/// </summary>
		/// <returns></returns>
		public string P_3()
		{

			bool groupByCharge = false;
			Dictionary<int, int> dctPeptideStats = SummarizePSMs(groupByCharge);

			int peptideCountFullyTryptic;
			int peptideCountSemiTryptic;

			if (!dctPeptideStats.TryGetValue(2, out peptideCountFullyTryptic))
				peptideCountFullyTryptic = 0;

			if (!dctPeptideStats.TryGetValue(1, out peptideCountSemiTryptic))
				peptideCountSemiTryptic = 0;

			// Compute the ratio of semi-tryptic / fully tryptic peptides
			double answer = 0;
			if (peptideCountFullyTryptic > 0)
				answer = peptideCountSemiTryptic / (double)peptideCountFullyTryptic;

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 6th DIGIT
			return answer.ToString("0.000000");
		}

		/// <summary>
		/// Counts the number of fully, partially, and non-tryptic peptides
		/// </summary>
		/// <param name="groupByCharge">If true, then counts charges separately</param>
		/// <returns></returns>
		protected Dictionary<int, int> SummarizePSMs(bool groupByCharge)
		{
			string chargeSql = String.Empty;

			if (groupByCharge)
			{
				if (UsingPHRP)
					chargeSql = ", Charge ";
				else
					chargeSql = ", temp_xt.Charge ";
			}

			//SET DB QUERY
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT Cleavage_State, Count(*) AS Peptides "
									+ " FROM ( SELECT Unique_Seq_ID" + chargeSql + ", Max(Cleavage_State) AS Cleavage_State "
									+ "        FROM temp_PSMs "
									+ "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
									+ "          AND random_id=" + m_Random_ID
									+ "        GROUP BY Unique_Seq_ID" + chargeSql + " ) StatsQ "
									+ " GROUP BY Cleavage_State;");

			else
				m_DBInterface.setQuery("SELECT Cleavage_State, Count(*) AS Peptides "
									+ " FROM ( SELECT temp_xt_resulttoseqmap.Unique_Seq_ID" + chargeSql + ", Max(temp_xt_seqtoproteinmap.cleavage_state) AS Cleavage_State "
									+ "        FROM temp_xt "
									+ "             INNER JOIN temp_xt_resulttoseqmap ON temp_xt.result_id = temp_xt_resulttoseqmap.result_id "
									+ "             INNER JOIN temp_xt_seqtoproteinmap ON temp_xt_resulttoseqmap.unique_seq_id = temp_xt_seqtoproteinmap.unique_seq_id "
									+ "        WHERE temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
									+ "          AND temp_xt.random_id=" + m_Random_ID
									+ "          AND temp_xt_resulttoseqmap.random_id=" + m_Random_ID
									+ "          AND temp_xt_seqtoproteinmap.random_id=" + m_Random_ID
									+ "        GROUP BY temp_xt_resulttoseqmap.Unique_Seq_ID" + chargeSql + " ) StatsQ "
									+ " GROUP BY Cleavage_State;");

			//DECLARE VARIABLES
			Dictionary<int, int> dctPeptideStats = new Dictionary<int, int>();

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Cleavage_State", "Peptides" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				dctPeptideStats.Add(Convert.ToInt32(m_MeasurementResults["Cleavage_State"]), Convert.ToInt32(m_MeasurementResults["Peptides"]));
			}

			return dctPeptideStats;
		
		}

	}
}
