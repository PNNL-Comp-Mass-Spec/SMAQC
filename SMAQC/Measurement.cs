using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace SMAQC
{
	class Measurement
	{
		struct udtPeptideEntry
		{
			public int Scan;
			public int Charge;
			public string Peptide_Sequence;
			public double Score;
		}

		struct udtMS2_4Counts
		{
			public Dictionary<int, int> ScanCount;		// Keys are quartile (1,2,3,4); values are the number of MS/MS scans in the quartile
			public Dictionary<int, int> PassFilt;		// Keys are quartile (1,2,3,4); values are the number of confidently identified MS/MS scans in the quartile
		}

		// CONSTANTS
		private const int XTANDEM_LOG_EVALUE_THRESHOLD = -2;
		public const double MSGF_SPECPROB_THRESHOLD = 1e-12;

		// CREATE DB INTERFACE OBJECT
		private readonly DBWrapper m_DBInterface;

		// MEASUREMENT RESULTS
		private Dictionary<string, string> m_MeasurementResults = new Dictionary<string, string>();

		// RANDOM ID FOR TEMP TABLES
		private readonly int m_Random_ID;

		// SOME MEASUREMENTS HAVE DATA REQUIRED BY OTHERS ... WILL BE STORED HERE
		private readonly Dictionary<string, double> m_ResultsStorage = new Dictionary<string, double>();

		// Cached data for computing median peak widths
		bool m_MedianPeakWidthDataCached;
		List<int> m_MPWCached_BestScan;										// Best Scan Number for each peptide
		Dictionary<int, double> m_MPWCached_FWHMinScans;					// Full width at half max, in scans; keys are FragScanNumbers
		Dictionary<int, int> m_MPWCached_OptimalPeakApexScanNumber;			// Optimal peak apex scan number; keys are FragScanNumbers
		Dictionary<int, double> m_MPWCached_ScanTime;						// Scan time for given scan number; keys are scan numbers

		// Cached data for PSM stats by charge
		Dictionary<int, int> m_Cached_PSM_Stats_by_Charge;					// Number of filter-passing PSMs for each charge state

		// Cached data for DS_1
		Dictionary<int, int> m_PeptideSamplingStats;						// Keys are the number of spectra that a peptide was observed in (passing filters) and values are the number of peptides identified by Key spectra</param>

		// Cached data for DS_3
		List<double> m_Cached_DS3;												// STORES MS1 max / MS1 sampled abundance
		List<double> m_Cached_DS3_Bottom50pct;                                  // STORES MS1 max / MS1 sampled abundance for ratios in the bottom 50%

		// Cached data for MS1_2
		List<double> m_Cached_BasePeakSignalToNoiseRatio;
		List<double> m_Cached_TotalIonIntensity;

		// Cached data for MS1_3
		double m_Cached_PeakMaxIntensity_5thPercentile;
		double m_Cached_PeakMaxIntensity_95thPercentile;
		List<double> m_Cached_MS1_3;

		// Cached data for MS1_5
		List<double> m_Cached_DelM;												// Delta mass
		List<double> m_Cached_DelM_ppm;                                         // Delta mass, in ppm

		// Cached data for MS4
		bool m_MS2_4_Counts_Cached;
		udtMS2_4Counts m_Cached_MS2_4_Counts;

		// Properties
		public bool UsingPHRP { get; set; }

		//CONSTRUCTOR
		public Measurement(int random_id, ref DBWrapper DBInterface)
		{
			//CREATE CONNECTIONS
			m_Random_ID = random_id;
			m_DBInterface = DBInterface;
			UsingPHRP = false;
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
		/// <param name="entryName"></param>
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
				var pos = (values.Count / 2);
				return values[pos];
			}
			else
			{
				//IF EVEN
				var pos = (values.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
				return (values[pos] + values[pos + 1]) / 2;
			}
		}
		protected double GetStoredValue(string entryName, double valueIfMissing)
		{
			double value;

			if (m_ResultsStorage.TryGetValue(entryName, out value))
				return value;
			
			return valueIfMissing;
		}

		protected int GetStoredValueInt(string entryName, int valueIfMissing)
		{
			var value = GetStoredValue(entryName, valueIfMissing);
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
			return C_1_Shared(countTailingPeptides:false);
		}

		/// <summary>
		/// C-1B: Fraction of peptides identified more than 4 minutes later than the chromatographic peak apex
		/// </summary>
		/// <returns></returns>
		public string C_1B()
		{
			return C_1_Shared(countTailingPeptides:true);
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
										+ "    temp_scanstats.ScanTime as ScanTime1, t2.ScanTime as ScanTimePeakApex "
										+ " FROM temp_PSMs, temp_scanstats, temp_sicstats as t1 "
										+ "      INNER JOIN temp_scanstats as t2 on t1.OptimalPeakApexScanNumber=t2.ScanNumber "
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
										+ "    temp_scanstats.ScanTime as ScanTime1, t2.ScanTime as ScanTimePeakApex "
										+ " FROM temp_xt, temp_scanstats, temp_sicstats as t1 "
										+ "      INNER JOIN temp_scanstats as t2 on t1.OptimalPeakApexScanNumber=t2.ScanNumber "
										+ " WHERE temp_xt.Scan = t1.FragScanNumber "
										+ "  AND temp_xt.Scan = temp_scanstats.ScanNumber "
										+ "  AND temp_xt.random_id=" + m_Random_ID
										+ "  AND temp_scanstats.random_id=" + m_Random_ID
										+ "  AND t1.random_id=" + m_Random_ID
										+ "  AND t2.random_id=" + m_Random_ID
										+ "  AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
										+ " ORDER BY Scan;");

			var psm_count_late_or_early = 0;
			var psm_count_total = 0;

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "FragScanNumber", "OptimalPeakApexScanNumber", "ScanTime1", "ScanTimePeakApex" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				// Calculate difference
				double temp_difference;
				if (countTailingPeptides)
					temp_difference = (Convert.ToDouble(m_MeasurementResults["ScanTime1"]) - Convert.ToDouble(m_MeasurementResults["ScanTimePeakApex"]));
				else
					temp_difference = (Convert.ToDouble(m_MeasurementResults["ScanTimePeakApex"]) - Convert.ToDouble(m_MeasurementResults["ScanTime1"]));

				// If difference is greater than 4 minutes, then increment the counter
				if (temp_difference >= 4.00)
				{
					psm_count_late_or_early += 1;
				}

				psm_count_total++;
			}

			//CALCULATE SOLUTION
			if (psm_count_total > 0)
			{
				var answer = psm_count_late_or_early / (double)psm_count_total;
				return answer.ToString("0.000000");
			}
			
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
			var lstFilterPassingPeptides = new SortedList<int, double>();

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "ScanNumber", "ScanTime1" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				// FILTER-PASSING PEPTIDE; Append to the dictionary
				int scanNumber;
				if (int.TryParse(m_MeasurementResults["ScanNumber"], out scanNumber))
				{
					double scanTime;
					if (double.TryParse(m_MeasurementResults["ScanTime1"], out scanTime))
					{

						if (!lstFilterPassingPeptides.ContainsKey(scanNumber))
						{
							lstFilterPassingPeptides.Add(scanNumber, scanTime);
						}
					}
				}
			}

			var index25th = -1;
			var index75th = -1;

			var C2AScanStart = 0;
			var C2AScanEnd = 0;

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

			var answer = C2AScanTimeEnd - C2AScanTimeStart;

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
			var lstScansWithFilterPassingIDs = new SortedSet<int>();

			var timeMinutesC2A = GetStoredValue("C_2A_TIME_MINUTES", 0);
			var scanStartC2A = GetStoredValueInt("C_2A_REGION_SCAN_START", 0);
			var scanEndC2A = GetStoredValueInt("C_2A_REGION_SCAN_END", 0);

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "ScanNumber", "ScanTime1" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				// FILTER-PASSING PEPTIDE; Append to the dictionary
				int scanNumber;
				if (int.TryParse(m_MeasurementResults["ScanNumber"], out scanNumber))
				{
					if (scanNumber >= scanStartC2A && scanNumber <= scanEndC2A && !lstScansWithFilterPassingIDs.Contains(scanNumber))
					{
						lstScansWithFilterPassingIDs.Add(scanNumber);
					}
				}
			}

			var answerText = string.Empty;

			if (timeMinutesC2A > 0)
			{
				var answer = lstScansWithFilterPassingIDs.Count / timeMinutesC2A;

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
			const double startScanRelative = 0;
			const double endScanRelative = 1;

			//COMPUTE RESULT
			return ComputeMedianPeakWidth(startScanRelative, endScanRelative);
		}

		/// <summary>
		/// C-3B: Median peak width during middle 50% of separation
		/// </summary>
		/// <returns></returns>
		public string C_3B()
		{
			const double startScanRelative = 0.25;
			const double endScanRelative = 0.75;

			//COMPUTE RESULT
			return ComputeMedianPeakWidth(startScanRelative, endScanRelative);
		}

		/// <summary>
		/// C-4A: Median peak width during first 10% of separation
		/// </summary>
		/// <returns></returns>
		public string C_4A()
		{
			const double startScanRelative = 0.00;
			const double endScanRelative = 0.10;

			//COMPUTE RESULT
			return ComputeMedianPeakWidth(startScanRelative, endScanRelative);
		}

		/// <summary>
		/// C-4B: Median peak width during last 10% of separation
		/// </summary>
		/// <returns></returns>
		public string C_4B()
		{
			const double startScanRelative = 0.90;
			const double endScanRelative = 1.00;

			//COMPUTE RESULT
			return ComputeMedianPeakWidth(startScanRelative, endScanRelative);
		}

		/// <summary>
		/// C-4C: Median peak width during middle 10% of separation
		/// </summary>
		/// <returns></returns>
		public string C_4C()
		{
			const double startScanRelative = 0.45;
			const double endScanRelative = 0.55;

			//COMPUTE RESULT
			return ComputeMedianPeakWidth(startScanRelative, endScanRelative);
		}

		protected void Cache_MedianPeakWidth_Data()
		{

			var lstPSMs = new List<udtPeptideEntry>();							// Cached, filter-passing peptide-spectrum matches

			m_MPWCached_BestScan = new List<int>();										// Best Scan Number for each peptide
			m_MPWCached_FWHMinScans = new Dictionary<int, double>();					// Full width at half max, in scans; keys are FragScanNumbers
			m_MPWCached_OptimalPeakApexScanNumber = new Dictionary<int, int>();			// Optimal peak apex scan number; keys are FragScanNumbers
			m_MPWCached_ScanTime = new Dictionary<int, double>();						// Scan time for given scan number; keys are scan numbers

			//SET DB QUERY [REQUIRED TO SORT BY PEPTIDE SEQUENCE]
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT Scan, Charge, Peptide_Sequence, MSGFSpecProb AS Peptide_Score"
									+ " FROM temp_PSMs "
									+ " WHERE random_id=" + m_Random_ID);
			else
				m_DBInterface.setQuery("SELECT Scan, Charge, Peptide_Sequence, Peptide_Expectation_Value_Log AS Peptide_Score"
									+ " FROM temp_xt "
									+ " WHERE random_id=" + m_Random_ID);

			//DECLARE FIELDS TO READ FROM
			string[] fields1 = { "Scan", "Charge", "Peptide_Sequence", "Peptide_Score" };

			//INIT READER
			m_DBInterface.initReader();

			// Read and cache the data since we need to trim off the prefix and suffix letters from the peptide sequence

			while ((m_DBInterface.readLines(fields1, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{

				string peptideResidues;
				string peptidePrefix;
				string peptideSuffix;

                PHRPReader.clsPeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(m_MeasurementResults["Peptide_Sequence"], out peptideResidues, out peptidePrefix, out peptideSuffix);

				var currentPeptide = new udtPeptideEntry();

				if (int.TryParse(m_MeasurementResults["Scan"], out currentPeptide.Scan))
				{
					if (int.TryParse(m_MeasurementResults["Charge"], out currentPeptide.Charge))
					{
						if (double.TryParse(m_MeasurementResults["Peptide_Score"], out currentPeptide.Score))
						{
							currentPeptide.Peptide_Sequence = peptideResidues;
							lstPSMs.Add(currentPeptide);
						}
					}
				}

			}

			// Sort by peptide sequence, then charge, then scan number
			var lstPSMsSorted = from item in lstPSMs
								orderby item.Peptide_Sequence, item.Charge, item.Scan
								select item;

			var previousPeptide = new udtPeptideEntry
			{
				Peptide_Sequence = string.Empty
			};

			// Parse the sorted data
			foreach (var psm in lstPSMsSorted)
			{
				double Best_Score;


				//IF PREVIOUS PEPTIDE SEQUENCES == EACH OTHER && PREVIOUS CHARGES == EACH OTHER
				if (previousPeptide.Peptide_Sequence.Equals(psm.Peptide_Sequence) && previousPeptide.Charge == psm.Charge)
				{
					//TAKE MIN [EITHER PREVIOUS BEST EVALUE OR CURRENT PEPTIDE SCORE]
					Best_Score = Math.Min(previousPeptide.Score, psm.Score);
				}
				else
				{
					Best_Score = psm.Score;
				}

				// Keep track of the scan number for the best score
				if (Math.Abs(Best_Score - psm.Score) < Single.Epsilon)
				{
					m_MPWCached_BestScan.Add(psm.Scan);
				}

				//UPDATE PREVIOUS VALUES FOR NEXT LOOP
				previousPeptide.Scan = psm.Scan;
				previousPeptide.Charge = psm.Charge;
				previousPeptide.Peptide_Sequence = string.Copy(psm.Peptide_Sequence);
				previousPeptide.Score = Best_Score;
			}

			//NOW SORT OUR VALUES SO THEY ARE IN THE CORRECT ORDER-
			m_MPWCached_BestScan.Sort();

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
				var fragScanNumber = Convert.ToInt32(m_MeasurementResults["FragScanNumber"]);

				m_MPWCached_FWHMinScans.Add(fragScanNumber, Convert.ToDouble(m_MeasurementResults["FWHMInScans"]));
				m_MPWCached_OptimalPeakApexScanNumber.Add(fragScanNumber, Convert.ToInt32(m_MeasurementResults["OptimalPeakApexScanNumber"]));
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
				m_MPWCached_ScanTime.Add(Convert.ToInt32(m_MeasurementResults["ScanNumber"]), Convert.ToDouble(m_MeasurementResults["ScanTime"]));
			}

			m_MedianPeakWidthDataCached = true;

		}

		protected string ComputeMedianPeakWidth(double startScanRelative, double endScanRelative)
		{
			var result = new List<double>();								//STORE RESULT FOR FINAL CALCULATION

			if (!m_MedianPeakWidthDataCached)
			{
				Cache_MedianPeakWidth_Data();
			}

			//NOW START THE ACTUAL MEASUREMENT CALCULATION

			//LOOP THROUGH BESTSCAN
			for (var i = 0; i < m_MPWCached_BestScan.Count; i++)
			{
				//FIND INDEX + OPTIMAL PEAK APEX SCAN +- FWHMIN FOR EACH RESULT [COLUMNS: M,O]				
				var OptimalPeakApexScanMinusFWHM = m_MPWCached_OptimalPeakApexScanNumber[m_MPWCached_BestScan[i]] - Convert.ToInt32(Math.Ceiling(m_MPWCached_FWHMinScans[m_MPWCached_BestScan[i]] / 2));
				var OptimalPeakApexScanPlusFWHM = m_MPWCached_OptimalPeakApexScanNumber[m_MPWCached_BestScan[i]] + Convert.ToInt32(Math.Ceiling(m_MPWCached_FWHMinScans[m_MPWCached_BestScan[i]] / 2));

				//FIND OTHER COLUMNS [N,P, Q,R,T]

				double start_time;

			    if (m_MPWCached_ScanTime.TryGetValue(OptimalPeakApexScanMinusFWHM, out start_time))
				{
				    double end_time;
				    if (m_MPWCached_ScanTime.TryGetValue(OptimalPeakApexScanPlusFWHM, out end_time))
					{

						var end_minus_start_in_secs = (end_time - start_time) * 60;
						var percent = (i + 1) / (double)m_MPWCached_BestScan.Count;

						//CHECK FOR VALID RANGE DATA THEN ADD TO OUR RESULTS
						if (percent >= startScanRelative && percent <= endScanRelative)
						{
							//WE ARE WITHING OUR VALID RANGE ... SO ADD end_minus_start_in_secs TO OUR LIST [COLUMN U]
							result.Add(end_minus_start_in_secs);
						}

					}
				}
			}

			var resultText = string.Empty;

			if (result.Count > 0)
			{
				//CALCULATE MEDIAN
				var median = ComputeMedian(result);													//INIT MEDIAN

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
			double result = 0;																		//SOLUTION

			if (m_PeptideSamplingStats == null)
				Cache_DS_1_Data();

			//NOW CALCULATE DS_1B
			//Return 0 if number of peptides identified with 3 spectra is 0

		    int numPeptidesWithTwoSpectra;

			if (m_PeptideSamplingStats != null && (m_PeptideSamplingStats.TryGetValue(2, out numPeptidesWithTwoSpectra)))
			{
			    if (numPeptidesWithTwoSpectra > 0)
			    {
			        int numPeptidesWithOneSpectrum;
			        if (!m_PeptideSamplingStats.TryGetValue(1, out numPeptidesWithOneSpectrum))
			            numPeptidesWithOneSpectrum = 0;

			        result = numPeptidesWithOneSpectrum / (double)numPeptidesWithTwoSpectra;
			    }
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

			double result = 0;																		//SOLUTION

			if (m_PeptideSamplingStats == null)
				Cache_DS_1_Data();

			//NOW CALCULATE DS_1B
			//Return 0 if number of peptides identified with 3 spectra is 0

		    int numPeptidesWithThreeSpectra;

			if (m_PeptideSamplingStats != null && (m_PeptideSamplingStats.TryGetValue(3, out numPeptidesWithThreeSpectra)))
			{
			    if (numPeptidesWithThreeSpectra > 0)
			    {
			        int numPeptidesWithTwoSpectra;
			        if (!m_PeptideSamplingStats.TryGetValue(2, out numPeptidesWithTwoSpectra))
			            numPeptidesWithTwoSpectra = 0;

			        result = numPeptidesWithTwoSpectra / (double)numPeptidesWithThreeSpectra;
			    }
			}

			return result.ToString("0.000");

		}

		/// <summary>
		/// Computes stats on the number of spectra by which each peptide was identified
		/// </summary>
		protected void Cache_DS_1_Data()
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

			m_PeptideSamplingStats = new Dictionary<int, int>();

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Spectra", "Peptides" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				var spectra = Convert.ToInt32(m_MeasurementResults["Spectra"]);
				var peptides = Convert.ToInt32(m_MeasurementResults["Peptides"]);

				m_PeptideSamplingStats.Add(spectra, peptides);
			}

		}

		/// <summary>
		/// DS-2A: Number of MS1 scans taken over middle 50% of separation
		/// </summary>
		/// <returns></returns>
		public string DS_2A()
		{
			const int msLevel = 1;
			return DS_2_Shared(msLevel).ToString();
		}

		/// <summary>
		/// DS-2B: Number of MS2 scans taken over middle 50% of separation
		/// </summary>
		/// <returns></returns>
		public string DS_2B()
		{
			const int msLevel = 2;
			return DS_2_Shared(msLevel).ToString();
		}

		protected int DS_2_Shared(int msLevel)
		{
			//DECLARE VARIABLES
			var scanStartC2A = GetStoredValueInt("C_2A_REGION_SCAN_START", 0);
			var scanEndC2A = GetStoredValueInt("C_2A_REGION_SCAN_END", 0);

			//SET DB QUERY
			m_DBInterface.setQuery("SELECT COUNT(*) AS ScanCount "
				+ " FROM temp_scanstats "
				+ " WHERE temp_scanstats.random_id=" + m_Random_ID
				+ "   AND ScanType = " + msLevel
				+ "   AND ScanNumber >= " + scanStartC2A
				+ "   AND ScanNumber <= " + scanEndC2A);

		    //DECLARE FIELDS TO READ FROM
			string[] fields = { "ScanCount" };

			//INIT READER
			m_DBInterface.initReader();
			m_DBInterface.readSingleLine(fields, ref m_MeasurementResults);

			var intScanCount = Convert.ToInt32(m_MeasurementResults["ScanCount"]);

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
				m_DBInterface.setQuery("SELECT Peptide_MH, Charge "
									+ " FROM temp_PSMs "
									+ " WHERE random_id=" + m_Random_ID
									+ "   AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);
			else
				m_DBInterface.setQuery("SELECT Peptide_MH, Charge "
									+ " FROM temp_xt "
									+ " WHERE random_id=" + m_Random_ID
									+ "   AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD);

			//DECLARE VARIABLES
			var MZ_list = new SortedSet<double>();								// MZ List

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Peptide_MH", "Charge" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//COMPUTE MZ VALUE
				var temp_mz = PHRPReader.clsPeptideMassCalculator.ConvoluteMass(Convert.ToDouble(m_MeasurementResults["Peptide_MH"]), 1, Convert.ToInt32(m_MeasurementResults["Charge"]));

				if (!MZ_list.Contains(temp_mz))
					MZ_list.Add(temp_mz);

			}

			// Compute the median
			var median = ComputeMedian(MZ_list.ToList());

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 4TH DIGIT       
			return median.ToString("0.0000");
		}

		/// <summary>
		/// IS-3A: Count of 1+ peptides / count of 2+ peptides
		/// </summary>
		/// <returns></returns>
		public string IS_3A()
		{
			int psm_count_charge1;
		    double result = 0;																//RESULT OF MEASUREMENT

			if (m_Cached_PSM_Stats_by_Charge == null)
			{
				Cache_IS3_Data();
			}

			//CALC MEASUREMENT
			if (m_Cached_PSM_Stats_by_Charge != null && m_Cached_PSM_Stats_by_Charge.TryGetValue(1, out psm_count_charge1))
			{
			    int psm_count_charge2;
			    if (m_Cached_PSM_Stats_by_Charge.TryGetValue(2, out psm_count_charge2))
					result = psm_count_charge1 / (double)psm_count_charge2;
			}

		    //ROUND
			return result.ToString("0.000000");
		}

		/// <summary>
		/// IS-3B: Count of 3+ peptides / count of 2+ peptides
		/// </summary>
		/// <returns></returns>
		public string IS_3B()
		{
			int psm_count_charge2;
		    double result = 0;																//RESULT OF MEASUREMENT

			if (m_Cached_PSM_Stats_by_Charge == null)
			{
				Cache_IS3_Data();
			}

			//CALC MEASUREMENT
			if (m_Cached_PSM_Stats_by_Charge != null && m_Cached_PSM_Stats_by_Charge.TryGetValue(2, out psm_count_charge2))
			{
			    int psm_count_charge3;
			    if (m_Cached_PSM_Stats_by_Charge.TryGetValue(3, out psm_count_charge3))
					result = psm_count_charge3 / (double)psm_count_charge2;
			}

		    //ROUND
			return result.ToString("0.000000");
		}

		/// <summary>
		/// IS-3C: Count of 4+ peptides / count of 2+ peptides
		/// </summary>
		/// <returns></returns>
		public string IS_3C()
		{
			int psm_count_charge2;
		    double result = 0;																//RESULT OF MEASUREMENT

			if (m_Cached_PSM_Stats_by_Charge == null)
			{
				Cache_IS3_Data();
			}

			//CALC MEASUREMENT
			if (m_Cached_PSM_Stats_by_Charge != null && m_Cached_PSM_Stats_by_Charge.TryGetValue(2, out psm_count_charge2))
			{
			    int psm_count_charge4;
			    if (m_Cached_PSM_Stats_by_Charge.TryGetValue(4, out psm_count_charge4))
					result = psm_count_charge4 / (double)psm_count_charge2;
			}

		    //ROUND
			return result.ToString("0.000000");
		}

		protected void Cache_IS3_Data()
		{

			//SET DB QUERY
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT Charge, COUNT(*) AS PSMs "
						+ " FROM temp_PSMs "
						+ " WHERE random_id=" + m_Random_ID
						+ "   AND MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
						+ " GROUP BY Charge;");
			else
				m_DBInterface.setQuery("SELECT Charge, COUNT(*) AS PSMs "
						+ " FROM temp_xt "
						+ " WHERE random_id=" + m_Random_ID
						+ "   AND Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
						+ " GROUP BY Charge;");

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Charge", "PSMs" };

			//INIT READER
			m_DBInterface.initReader();

			m_Cached_PSM_Stats_by_Charge = new Dictionary<int, int>();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				int charge;
				if (int.TryParse(m_MeasurementResults["Charge"], out charge))
				{
					int psm_count;
					if (int.TryParse(m_MeasurementResults["PSMs"], out psm_count))
					{
						if (!m_Cached_PSM_Stats_by_Charge.ContainsKey(charge))
							m_Cached_PSM_Stats_by_Charge.Add(charge, psm_count);
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
			m_DBInterface.setQuery("SELECT temp_scanstatsex.Ion_Injection_Time "
				+ " FROM temp_scanstats, temp_scanstatsex "
				+ " WHERE temp_scanstats.ScanNumber = temp_scanstatsex.ScanNumber "
				+ "  AND temp_scanstatsex.random_id = " + m_Random_ID
				+ "  AND temp_scanstats.random_id = " + m_Random_ID
				+ "  AND temp_scanstats.ScanType = 1 "
				+ " ORDER BY temp_scanstats.ScanNumber;");

			//DECLARE VARIABLES
			var lstValues = new List<double>();                            //FILTER LIST

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Ion_Injection_Time" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//ADD TO FILTER LIST
				lstValues.Add(Convert.ToDouble(m_MeasurementResults["Ion_Injection_Time"]));
			}

			var median = ComputeMedian(lstValues);

            return Convert.ToString(median, CultureInfo.InvariantCulture);
		}

		/// <summary>
		/// MS1_2A: Median S/N value for MS1 spectra from run start through middle 50% of separation
		/// </summary>
		/// <returns></returns>
		public string MS1_2A()
		{
			var median = 0.00;                                               //RESULT

			if (m_Cached_BasePeakSignalToNoiseRatio == null)
				Cache_MS1_2_Data();

			if (m_Cached_BasePeakSignalToNoiseRatio != null && m_Cached_BasePeakSignalToNoiseRatio.Count > 0)
			{
				//CALC MEDIAN OF COLUMN J
				median = ComputeMedian(m_Cached_BasePeakSignalToNoiseRatio);
			}

			return Convert.ToString(median, CultureInfo.InvariantCulture);
		}

		/// <summary>
		/// MS1_2B: Median TIC value for identified peptides from run start through middle 50% of separation
		/// </summary>
		/// <returns></returns>
		public string MS1_2B()
		{
			if (m_Cached_TotalIonIntensity == null)
				Cache_MS1_2_Data();

			//CALC MEDIAN OF COLUMN K
			var median = ComputeMedian(m_Cached_TotalIonIntensity);

			//DIVIDE BY 1000
			median = median / 1000;

			return Convert.ToString(median, CultureInfo.InvariantCulture);
		}

		protected void Cache_MS1_2_Data()
		{

			var scanFirstPeptide = GetStoredValueInt("SCAN_FIRST_FILTER_PASSING_PEPTIDE", 0);
			var scanEndC2A = GetStoredValueInt("C_2A_REGION_SCAN_END", 0);

			//SET DB QUERY
			m_DBInterface.setQuery("SELECT BasePeakSignalToNoiseRatio, TotalIonIntensity "
				+ " FROM temp_scanstats"
				+ " WHERE temp_scanstats.random_id=" + m_Random_ID
				+ "   AND ScanType = 1 "
				+ "   AND ScanNumber >= " + scanFirstPeptide
				+ "   AND ScanNumber <= " + scanEndC2A);

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "BasePeakSignalToNoiseRatio", "TotalIonIntensity" };

			//INIT READER
			m_DBInterface.initReader();

			m_Cached_BasePeakSignalToNoiseRatio = new List<double>();
			m_Cached_TotalIonIntensity = new List<double>();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				m_Cached_BasePeakSignalToNoiseRatio.Add(Convert.ToDouble(m_MeasurementResults["BasePeakSignalToNoiseRatio"]));
				m_Cached_TotalIonIntensity.Add(Convert.ToDouble(m_MeasurementResults["TotalIonIntensity"]));
			}

		}

		/// <summary>
		/// MS1_3A: Dynamic range estimate using 95th percentile peptide peak apex intensity / 5th percentile
		/// </summary>
		/// <returns></returns>
		public string MS1_3A()
		{
			double final = 0;

			Cache_MS1_3_Data();

			//CALCULATE FINAL MEASUREMENT VALUE
			if (m_Cached_PeakMaxIntensity_5thPercentile > 0)
				final = m_Cached_PeakMaxIntensity_95thPercentile / m_Cached_PeakMaxIntensity_5thPercentile;

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 3RD DIGIT
			return final.ToString("0.000");
		}

		/// <summary>
		/// MS1_3B: Median peak apex intensity for all peptides
		/// </summary>
		/// <returns></returns>
		public string MS1_3B()
		{
			if (m_Cached_MS1_3 == null)
				Cache_MS1_3_Data();

			//NOW CALCULATE MEDIAN
			var median = ComputeMedian(m_Cached_MS1_3);

			//WE NOW HAVE RESULT
			if (median > 100)
				return median.ToString("0");
			
			return median.ToString("0.0");
		}

		protected void Cache_MS1_3_Data()
		{
			//SET DB QUERY
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT temp_sicstats.PeakMaxIntensity"
					+ " FROM temp_sicstats, temp_PSMs"
					+ " WHERE temp_sicstats.FragScanNumber=temp_PSMs.Scan"
					+ "   AND temp_sicstats.random_id=" + m_Random_ID
					+ "   AND temp_PSMs.random_id=" + m_Random_ID
					+ "   AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
					+ " ORDER BY temp_sicstats.PeakMaxIntensity, temp_PSMs.Result_ID DESC;");
			else
				m_DBInterface.setQuery("SELECT temp_sicstats.PeakMaxIntensity"
					+ " FROM temp_sicstats, temp_xt"
					+ " WHERE temp_sicstats.FragScanNumber=temp_xt.Scan"
					+ "   AND temp_sicstats.random_id=" + m_Random_ID
					+ "   AND temp_xt.random_id=" + m_Random_ID
					+ "   AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
					+ " ORDER BY temp_sicstats.PeakMaxIntensity, temp_xt.Result_ID DESC;");

			//DECLARE VARIABLES
			var MPI_list = new List<double>();                                                         //STORES MAX PEAK INTENSITY FOR 5-95%

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "PeakMaxIntensity" };

			//INIT READER
			m_DBInterface.initReader();

			m_Cached_PeakMaxIntensity_5thPercentile = 0;
			m_Cached_PeakMaxIntensity_95thPercentile = 0;
			m_Cached_MS1_3 = new List<double>();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//ADD TO FILTER LIST
				m_Cached_MS1_3.Add(Convert.ToDouble(m_MeasurementResults["PeakMaxIntensity"]));
			}

			if (m_Cached_MS1_3.Count > 0)
			{
				// Store values between the 5th and 95th percentiles
				for (var i = 0; i < m_Cached_MS1_3.Count; i++)
				{

					//CHECK IF BETWEEN 5-95%
					var percent = Convert.ToDouble(i) / Convert.ToDouble(m_Cached_MS1_3.Count);
					if (percent >= 0.05 && percent <= 0.95)
					{
						//ADD TO MPI LIST
						MPI_list.Add(m_Cached_MS1_3[i]);
					}

				}
			}

			if (MPI_list.Count > 0)
			{
				//CALCULATE FINAL VALUES
				m_Cached_PeakMaxIntensity_5thPercentile = MPI_list.Min();
				m_Cached_PeakMaxIntensity_95thPercentile = MPI_list.Max();
			}


		}

		/// <summary>
		/// DS_3A: Median of MS1 max / MS1 sampled abundance
		/// </summary>
		/// <returns></returns>
		public string DS_3A()
		{
			return DS_3_Shared(bottom50Pct:false);
		}

		/// <summary>
		/// DS_3B: Median of MS1 max / MS1 sampled abundance; limit to bottom 50% of peptides by abundance
		/// </summary>
		/// <returns></returns>
		public string DS_3B()
		{
			return DS_3_Shared(bottom50Pct:true);
		}

		protected void DS_3_CacheData()
		{
			//SET DB QUERY
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT ParentIonIntensity, PeakMaxIntensity"
					+ " FROM temp_sicstats, temp_PSMs "
					+ " WHERE temp_sicstats.FragScanNumber=temp_PSMs.Scan"
					+ "   AND temp_sicstats.random_id=" + m_Random_ID
					+ "   AND temp_PSMs.random_id=" + m_Random_ID
					+ "   AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);
			else
				m_DBInterface.setQuery("SELECT ParentIonIntensity, PeakMaxIntensity"
					+ " FROM temp_sicstats, temp_xt "
					+ " WHERE temp_sicstats.FragScanNumber=temp_xt.Scan"
					+ "   AND temp_sicstats.random_id=" + m_Random_ID
					+ "   AND temp_xt.random_id=" + m_Random_ID
					+ "   AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD);

			//DECLARE VARIABLES
			var lstResult = new List<double>();                        // Ratio of PeakMaxIntensity over ParentIonIntensity

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "ParentIonIntensity", "PeakMaxIntensity" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{

				// Compute the ratio of the PeakMaxIntensity over ParentIonIntensity
				var parentIonIntensity = Convert.ToDouble(m_MeasurementResults["ParentIonIntensity"]);
				double ratioPeakMaxToParentIonIntensity;
				if (parentIonIntensity > 0)
					ratioPeakMaxToParentIonIntensity = Convert.ToDouble(m_MeasurementResults["PeakMaxIntensity"]) / parentIonIntensity;
				else
					ratioPeakMaxToParentIonIntensity = 0;

				lstResult.Add(ratioPeakMaxToParentIonIntensity);

			}

			// Sort the values
			lstResult.Sort();

			m_Cached_DS3 = new List<double>();													// STORES MS1 max / MS1 sampled abundance
			m_Cached_DS3_Bottom50pct = new List<double>();                                      // STORES MS1 max / MS1 sampled abundance for ratios in the bottom 50%

			//LOOP THROUGH ALL KEYS
			for (var i = 0; i < lstResult.Count; i++)
			{

				//ADD TO FILTERED LIST FOR COLUMN K
				m_Cached_DS3.Add(lstResult[i]);

				//IF IN VALID BOTTOM 50%
				if ((i + 1) / (double)lstResult.Count <= 0.5)
				{
					//ADD TO FILTERED LIST FOR COLUMN N
					m_Cached_DS3_Bottom50pct.Add(lstResult[i]);
				}

			}
		}

		/// <summary>
		/// Median of MS1 max / MS1 sampled abundance
		/// </summary>
		/// <param name="bottom50Pct">Set to true to limit to the bottom 50% of peptides by abundance</param>
		/// <returns></returns>
		protected string DS_3_Shared(bool bottom50Pct)
		{
			if (m_Cached_DS3 == null)
			{
				DS_3_CacheData();
			}

			double median;
			if (bottom50Pct)
				median = ComputeMedian(m_Cached_DS3_Bottom50pct);
			else
				median = ComputeMedian(m_Cached_DS3);

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
			const int foldThreshold = 10;

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
			const int foldThreshold = 10;

			IS_1_Shared(foldThreshold, out countMS1Jump10x, out countMS1Fall10x);
			return Convert.ToString(countMS1Fall10x);
		}

		protected void IS_1_Shared(int foldThreshold, out int countMS1Jump10x, out int countMS1Fall10x)
		{
			//SET DB QUERY
			m_DBInterface.setQuery("SELECT ScanNumber, BasePeakIntensity  "
				+ " FROM temp_scanstats "
				+ " WHERE temp_scanstats.random_id=" + m_Random_ID
				+ "   AND ScanType = 1");

			//DECLARE VARIABLES
			double bpiPrevious = -1;
			countMS1Jump10x = 0;                                                          //STORE COUNT FOR IS_1A
			countMS1Fall10x = 0;                                                          //STORE COUNT FOR IS_1B

			//VALIDATE foldThreshold
			if (foldThreshold < 2)
				foldThreshold = 2;

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "ScanNumber", "BasePeakIntensity" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{

				var bpiCurrent = Convert.ToDouble(m_MeasurementResults["BasePeakIntensity"]);

				if (bpiPrevious > -1)
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

		/// <summary>
		/// MS1_5A: Median of precursor mass error (Th)
		/// </summary>
		/// <returns></returns>
		public string MS1_5A()
		{

			if (m_Cached_DelM == null)
				Cache_MS1_5_Data();

			if (m_Cached_DelM != null && m_Cached_DelM.Count == 0)
				return "0";

			//NOW CALCULATE MEDIAN
			var median = ComputeMedian(m_Cached_DelM);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 6th DIGIT
			return median.ToString("0.000000");
		}

		/// <summary>
		/// MS1_5B: Median of absolute value of precursor mass error (Th)
		/// </summary>
		/// <returns></returns>
		public string MS1_5B()
		{

			if (m_Cached_DelM == null)
				Cache_MS1_5_Data();

		    if (m_Cached_DelM == null)
		    {
		        return "0";
		    }
		    
		    var lstAbsDelM = new List<double>(m_Cached_DelM.Count);

		    if (m_Cached_DelM.Count == 0)
		    {
		        return "0";
		    }

		    foreach (var value in m_Cached_DelM)
		    {
		        lstAbsDelM.Add(Math.Abs(value));
		    }

		    //CALCULATE AVERAGE
		    var average = lstAbsDelM.Average();

		    //WE NOW HAVE RESULT ... NOW ROUND IT TO 6th DIGIT
		    return average.ToString("0.000000");
		  
		}

		/// <summary>
		/// MS1_5C: Median of precursor mass error (ppm)
		/// </summary>
		/// <returns></returns>
		public string MS1_5C()
		{
			if (m_Cached_DelM_ppm == null)
				Cache_MS1_5_Data();

            if (m_Cached_DelM_ppm == null || m_Cached_DelM_ppm.Count == 0)
				return "0";

			//NOW CALCULATE MEDIAN
			var median = ComputeMedian(m_Cached_DelM_ppm);

			return median.ToString("0.000");
		}

		/// <summary>
		/// MS1_5D: Interquartile distance in ppm-based precursor mass error
		/// </summary>
		/// <returns></returns>
		public string MS1_5D()
		{
			if (m_Cached_DelM_ppm == null)
				Cache_MS1_5_Data();

			if (m_Cached_DelM_ppm == null || m_Cached_DelM_ppm.Count == 0)
				return "0";

			// Sort the DelM_ppm values
			m_Cached_DelM_ppm.Sort();

			var lstInterquartilePPMErrors = new List<double>();                          //STORE ERRORS FROM PPMList [COLUMN M]

			//CALCULATE INTER_QUARTILE_START AND INTER_QUARTILE_END
			var inter_quartile_start = Convert.ToInt32(Math.Round(0.25 * Convert.ToDouble(m_Cached_DelM_ppm.Count)));
			var inter_quartile_end = Convert.ToInt32(Math.Round(0.75 * Convert.ToDouble(m_Cached_DelM_ppm.Count)));

			//LOOP THROUGH EACH ITEM IN LIST
			for (var i = 0; i < m_Cached_DelM_ppm.Count; i++)
			{
				if ((i >= inter_quartile_start) && (i <= inter_quartile_end))
				{
					//ADD TO LIST [COLUMN M]
					lstInterquartilePPMErrors.Add(m_Cached_DelM_ppm[i]);
				}
			}

			//NOW CALCULATE MEDIAN
			var median = ComputeMedian(lstInterquartilePPMErrors);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 3 DIGITS
			return median.ToString("0.000");
		}

		protected void Cache_MS1_5_Data()
		{
			//SET DB QUERY
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT temp_PSMs.Peptide_MH, temp_PSMs.Charge, temp_sicstats.MZ, temp_PSMs.DelM_Da, temp_PSMs.DelM_PPM"
						+ " FROM temp_PSMs, temp_sicstats"
						+ " WHERE temp_sicstats.FragScanNumber=temp_PSMs.Scan AND temp_sicstats.random_id=" + m_Random_ID + " AND temp_PSMs.random_id=" + m_Random_ID
						+ " AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);
			else
				m_DBInterface.setQuery("SELECT temp_xt.Peptide_MH, temp_xt.Charge, temp_sicstats.MZ, 0 AS DelM_Da, temp_xt.DelM_PPM "
						+ " FROM temp_xt, temp_sicstats"
						+ " WHERE temp_sicstats.FragScanNumber=temp_xt.Scan AND temp_sicstats.random_id=" + m_Random_ID + " AND temp_xt.random_id=" + m_Random_ID
						+ " AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD);

			//DECLARE VARIABLES
			const double massC13 = 1.00335483;

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Peptide_MH", "Charge", "MZ", "DelM_Da", "DelM_PPM" };

			//INIT READER
			m_DBInterface.initReader();

			m_Cached_DelM = new List<double>();
			m_Cached_DelM_ppm = new List<double>();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//CALC THEORETICAL VALUE [COLUMN F], which would be theoretical m/z of the peptide
				// Instead, calculate theoretical monoisotopic mass of the peptide

				var theoMonoMass = PHRPReader.clsPeptideMassCalculator.ConvoluteMass(Convert.ToDouble(m_MeasurementResults["Peptide_MH"]), 1, 0);

				// Compute observed precursor mass, as monoisotopic mass					
				int observedCharge = Convert.ToInt16(m_MeasurementResults["Charge"]);
				var observedMonoMass = PHRPReader.clsPeptideMassCalculator.ConvoluteMass(Convert.ToDouble(m_MeasurementResults["MZ"]), observedCharge, 0);

				var delm = observedMonoMass - theoMonoMass;

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

				m_Cached_DelM.Add(delm);

				m_Cached_DelM_ppm.Add(delMppm);

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
				m_DBInterface.setQuery("SELECT temp_scanstatsex.Ion_Injection_Time "
					+ " FROM temp_PSMs, temp_scanstatsex "
					+ " WHERE temp_PSMs.Scan=temp_scanstatsex.ScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_scanstatsex.random_id=" + m_Random_ID
					+ "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);
			else
				m_DBInterface.setQuery("SELECT  temp_scanstatsex.Ion_Injection_Time "
					+ " FROM temp_xt, temp_scanstatsex "
					+ " WHERE temp_xt.Scan=temp_scanstatsex.ScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_scanstatsex.random_id=" + m_Random_ID
					+ "  AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD);

			//DECLARE VARIABLES
			var FilterList = new List<double>();                                       //FILTERED LIST [COLUMN P]

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Ion_Injection_Time" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//ADD TO FILTERED LIST
				FilterList.Add(Convert.ToDouble(m_MeasurementResults["Ion_Injection_Time"]));
			}

			//NOW CALCULATE MEDIAN
			var median = ComputeMedian(FilterList);

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
				m_DBInterface.setQuery("SELECT temp_scanstats.BasePeakSignalToNoiseRatio "
					+ " FROM temp_PSMs, temp_scanstats "
					+ " WHERE temp_PSMs.Scan=temp_scanstats.ScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_scanstats.random_id=" + m_Random_ID
					+ "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);
			else
				m_DBInterface.setQuery("SELECT temp_scanstats.BasePeakSignalToNoiseRatio "
					+ " FROM temp_xt, temp_scanstats "
					+ " WHERE temp_xt.Scan=temp_scanstats.ScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_scanstats.random_id=" + m_Random_ID
					+ "  AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD);

			//DECLARE VARIABLES
			var FilterList = new List<double>();                                       //FILTERED LIST [COLUMN H]
			var FinishedList = new List<double>();                                     //FINISHED LIST [COLUMN J]

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "BasePeakSignalToNoiseRatio" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//ADD TO FILTERED LIST
				FilterList.Add(Convert.ToDouble(m_MeasurementResults["BasePeakSignalToNoiseRatio"]));
			}

			//LOOP THROUGH FILTERED LIST
			for (var i = 0; i < FilterList.Count; i++)
			{
				//CALCULATE IF <= 0.75
				if ((i + 1) <= (FilterList.Count * 0.75))
				{
					//ADD TO FINISHED FILTERED LIST
					FinishedList.Add(FilterList[i]);
				}
			}

			//NOW CALCULATE MEDIAN
			var median = ComputeMedian(FinishedList);

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
				m_DBInterface.setQuery("SELECT temp_scanstats.IonCountRaw "
					+ " FROM temp_PSMs, temp_scanstats "
					+ " WHERE temp_PSMs.Scan=temp_scanstats.ScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_scanstats.random_id=" + m_Random_ID
					+ "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);
			else
				m_DBInterface.setQuery("SELECT temp_scanstats.IonCountRaw "
					+ " FROM temp_xt, temp_scanstats "
					+ " WHERE temp_xt.Scan=temp_scanstats.ScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_scanstats.random_id=" + m_Random_ID
					+ "  AND temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD);

			//DECLARE VARIABLES
			var FilterList = new List<double>();                                       //FILTERED LIST [COLUMN M]

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "IonCountRaw" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//ADD TO FILTERED LIST
				FilterList.Add(Convert.ToDouble(m_MeasurementResults["IonCountRaw"]));
			}

			//NOW CALCULATE MEDIAN
			var median = ComputeMedian(FilterList);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO THE NEAREST INTEGER
			return median.ToString("0");
		}

		/// <summary>
		/// MS2_4A: Fraction of all MS2 spectra identified; low abundance quartile (determined using MS1 intensity of identified peptides)
		/// </summary>
		/// <returns></returns>
		public string MS2_4A()
		{

			if (!m_MS2_4_Counts_Cached)
				Cache_MS2_4_Data();

			var result = Compute_MS2_4_Ratio(1);
			return result.ToString("0.0000");
		}

		/// <summary>
		/// MS2_4B: Fraction of all MS2 spectra identified; second quartile (determined using MS1 intensity of identified peptides)
		/// </summary>
		/// <returns></returns>
		public string MS2_4B()
		{
			if (!m_MS2_4_Counts_Cached)
				Cache_MS2_4_Data();

			var result = Compute_MS2_4_Ratio(2);
			return result.ToString("0.0000");

		}

		/// <summary>
		/// MS2_4C: Fraction of all MS2 spectra identified; third quartile (determined using MS1 intensity of identified peptides)
		/// </summary>
		/// <returns></returns>
		public string MS2_4C()
		{
			if (!m_MS2_4_Counts_Cached)
				Cache_MS2_4_Data();

			var result = Compute_MS2_4_Ratio(3);
			return result.ToString("0.0000");

		}

		/// <summary>
		/// MS2_4D: Fraction of all MS2 spectra identified; high abundance quartile (determined using MS1 intensity of identified peptides)
		/// </summary>
		/// <returns></returns>
		public string MS2_4D()
		{
			if (!m_MS2_4_Counts_Cached)
				Cache_MS2_4_Data();

			var result = Compute_MS2_4_Ratio(4);
			return result.ToString("0.0000");

		}

		protected double Compute_MS2_4_Ratio(int quartile)
		{
			int scanCountTotal;
			double result = 0;

			if (m_Cached_MS2_4_Counts.ScanCount.TryGetValue(quartile, out scanCountTotal))
			{
				if (scanCountTotal > 0)
				{
					result = m_Cached_MS2_4_Counts.PassFilt[quartile] / (double)scanCountTotal;
				}
			}

			return result;
		}

		protected void Cache_MS2_4_Data()
		{
			//SET DB QUERY [TO FIND MAX NUMBER OF ROWS]
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT COUNT(*) as MS2ScanCount "
					+ " FROM (SELECT DISTINCT temp_PSMs.Scan, temp_sicstats.PeakMaxIntensity "
					+ "       FROM temp_PSMs, temp_sicstats "
					+ "       WHERE temp_PSMs.Scan=temp_sicstats.FragScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_sicstats.random_id=" + m_Random_ID
					+ "      ) LookupQ;");
			else
				m_DBInterface.setQuery("SELECT COUNT(*) as MS2ScanCount "
					+ " FROM (SELECT DISTINCT temp_xt.Scan, temp_sicstats.PeakMaxIntensity "
					+ "       FROM temp_xt, temp_sicstats "
					+ "       WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_sicstats.random_id=" + m_Random_ID
					+ "      ) LookupQ;");

			//DECLARE FIELDS TO READ FROM
			string[] fields1 = { "MS2ScanCount" };

			//INIT READER
			m_DBInterface.initReader();

			//READ LINE
			m_DBInterface.readSingleLine(fields1, ref m_MeasurementResults);
			var scanCountMS2 = Convert.ToInt32(m_MeasurementResults["MS2ScanCount"]);                         //STORE TOTAL MS2 SCAN COUNT

			// SET DB QUERY
			// NOTE THAT WE SORT BY ASCENDING PeakMaxIntensity
			// Thus, quartile 1 in m_Cached_MS2_4_Counts will have the lowest abundance peptides
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT temp_PSMs.Scan, temp_sicstats.PeakMaxIntensity, Min(temp_PSMs.MSGFSpecProb) AS Peptide_Score "
					+ " FROM temp_PSMs, temp_sicstats"
					+ " WHERE temp_PSMs.Scan=temp_sicstats.FragScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_sicstats.random_id=" + m_Random_ID
					+ " GROUP BY temp_PSMs.Scan, temp_sicstats.PeakMaxIntensity "
					+ " ORDER BY temp_sicstats.PeakMaxIntensity;");
			else
				m_DBInterface.setQuery("SELECT temp_xt.Scan, temp_sicstats.PeakMaxIntensity, Min(temp_xt.Peptide_Expectation_Value_Log) AS Peptide_Score"
					+ " FROM temp_xt, temp_sicstats"
					+ " WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + m_Random_ID + " AND temp_sicstats.random_id=" + m_Random_ID
					+ " GROUP BY temp_xt.Scan, temp_sicstats.PeakMaxIntensity "
					+ " ORDER BY temp_sicstats.PeakMaxIntensity;");

			//DECLARE VARIABLES

			m_Cached_MS2_4_Counts.ScanCount = new Dictionary<int, int>();		// Keys are quartile (1,2,3,4); values are the number of MS/MS scans in the quartile
			m_Cached_MS2_4_Counts.PassFilt = new Dictionary<int, int>();		// Keys are quartile (1,2,3,4); values are the number of confidently identified MS/MS scans in the quartile

			for (var i = 1; i <= 4; i++)
			{
				m_Cached_MS2_4_Counts.ScanCount.Add(i, 0);
				m_Cached_MS2_4_Counts.PassFilt.Add(i, 0);
			}

			var running_scan_count = 1;									// RUNNING SCAN COUNT

			//DECLARE FIELDS TO READ FROM
			string[] fields2 = { "Scan", "Peptide_Score", "PeakMaxIntensity" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields2, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				//DID IT PASS OUR FILTER?
				var passed_filter = false;

				//CALCULATE COLUMN G

				// Compare the peptide_score vs. the threshold
				double peptideScore;
				if (double.TryParse(m_MeasurementResults["Peptide_Score"], out peptideScore) && PeptideScorePassesFilter(peptideScore))
				{
					passed_filter = true;
				}

				// Compare the running count to scanCountMS2 to determine the quartile
				if (running_scan_count < (scanCountMS2 * 0.25))
				{
					// 1st quartile
					UpdateMS2_4_QuartileStats(1, passed_filter);
				}

				if (running_scan_count >= (scanCountMS2 * 0.25) && running_scan_count < (scanCountMS2 * 0.5))
				{
					// 2nd quartile
					UpdateMS2_4_QuartileStats(2, passed_filter);
				}

				if (running_scan_count >= (scanCountMS2 * 0.5) && running_scan_count < (scanCountMS2 * 0.75))
				{
					// 3rd quartile
					UpdateMS2_4_QuartileStats(3, passed_filter);
				}

				if (running_scan_count >= (scanCountMS2 * 0.75))
				{
					// 4th quartile
					UpdateMS2_4_QuartileStats(4, passed_filter);

				}

				running_scan_count++;
			}

			if (running_scan_count > scanCountMS2 + 1 && running_scan_count > scanCountMS2 * 1.01)
				Console.WriteLine("Possible bug in Cache_MS2_4_Data, running_scan_count >> scanCountMS2: " + running_scan_count + " vs. " + scanCountMS2);

			m_MS2_4_Counts_Cached = true;

		}

		protected void UpdateMS2_4_QuartileStats(int quartile, bool passed_filter)
		{

			/*
			  int newValue;
			  newValue = m_Cached_MS2_4_Counts.ScanCount[quartile];
			  m_Cached_MS2_4_Counts.ScanCount[quartile] = newValue;
			*/

			m_Cached_MS2_4_Counts.ScanCount[quartile]++;

			if (passed_filter)
				m_Cached_MS2_4_Counts.PassFilt[quartile]++;
		}

		/// <summary>
		/// P_1A: Median peptide ID score (X!Tandem hyperscore or -Log10(MSGF_SpecProb))
		/// </summary>
		/// <returns></returns>
		public string P_1A()
		{
			//SET DB QUERY
			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT Scan, Max(-Log10(MSGFSpecProb)) AS Peptide_Score"
					+ " FROM temp_PSMs "
					+ " WHERE random_id=" + m_Random_ID
					+ " GROUP BY Scan "
					+ " ORDER BY Scan;");
			else
				m_DBInterface.setQuery("SELECT Scan, Max(Peptide_Hyperscore) AS Peptide_Score"
					+ " FROM temp_xt "
					+ " WHERE random_id=" + m_Random_ID
					+ " GROUP BY Scan "
					+ " ORDER BY Scan;");

			//DECLARE VARIABLES
			var Peptide_score_List = new List<double>();                          // Track X!Tandem Hyperscore or -Log10(MSGFSpecProb)

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Scan", "Peptide_Score" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				double peptideScore;
				if (double.TryParse(m_MeasurementResults["Peptide_Score"], out peptideScore))
				{
					Peptide_score_List.Add(peptideScore);
				}
			}

			//NOW CALCULATE MEDIAN
			var median = ComputeMedian(Peptide_score_List);

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
				m_DBInterface.setQuery("SELECT Scan, Min(Log10(MSGFSpecProb)) AS Peptide_Score"
					+ " FROM temp_PSMs "
					+ " WHERE random_id=" + m_Random_ID
					+ " GROUP BY Scan "
					+ " ORDER BY Scan;");
			else
				m_DBInterface.setQuery("SELECT Scan, Min(Peptide_Expectation_Value_Log) AS Peptide_Score"
					+ " FROM temp_xt "
					+ " WHERE random_id=" + m_Random_ID
					+ " GROUP BY Scan "
					+ " ORDER BY Scan;");

			//DECLARE VARIABLES
			var Peptide_score_List = new List<double>();                          // Track X!Tandem Peptide_Expectation_Value_Log or Log10(MSGFSpecProb)

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
					Peptide_score_List.Add(peptideScore);
				}
			}

			//NOW CALCULATE MEDIAN
			var median = ComputeMedian(Peptide_score_List);

			//WE NOW HAVE RESULT ... NOW ROUND IT TO 3rd DIGIT           
			return median.ToString("0.000");
		}

		/// <summary>
		/// P_2A: Number of tryptic peptides; total spectra count
		/// </summary>
		/// <returns></returns>
		public string P_2A()
		{
			return P_2A_Shared(phosphoPeptides: false);
		}

		protected string P_2A_Shared(bool phosphoPeptides)
		{
			//SET DB QUERY
			

			if (UsingPHRP)
				m_DBInterface.setQuery("SELECT Cleavage_State, Count(*) AS Spectra "
									+ " FROM ( SELECT Scan, Max(Cleavage_State) AS Cleavage_State "
									+ "        FROM temp_PSMs "
									+ "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
									+ "          AND random_id=" + m_Random_ID
									+ PhosphoFilter(phosphoPeptides)
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
			var dctPSMStats = new Dictionary<int, int>();

			//DECLARE FIELDS TO READ FROM
			string[] fields = { "Cleavage_State", "Spectra" };

			//INIT READER
			m_DBInterface.initReader();

			//READ ROWS
			while ((m_DBInterface.readLines(fields, ref m_MeasurementResults)) && (m_MeasurementResults.Count > 0))
			{
				dctPSMStats.Add(Convert.ToInt32(m_MeasurementResults["Cleavage_State"]), Convert.ToInt32(m_MeasurementResults["Spectra"]));
			}

			// Lookup the number of fully tryptic spectra (Cleavage_State = 2)
			int spectraCount;
			if (dctPSMStats.TryGetValue(2, out spectraCount))
				return spectraCount.ToString();
			
			return "0";

		}

		/// <summary>
		/// P_2B: Number of tryptic peptides; unique peptide & charge count
		/// </summary>
		/// <returns></returns>
		public string P_2B()
		{
			const bool groupByCharge = true;
			Dictionary<int, int> dctPeptideStats = SummarizePSMs(groupByCharge);

			// Lookup the number of fully tryptic peptides (Cleavage_State = 2)
			int peptideCount;
			if (dctPeptideStats.TryGetValue(2, out peptideCount))
				return peptideCount.ToString();
			
			return "0";
		}

		/// <summary>
		/// P_2C: Number of tryptic peptides; unique peptide count
		/// </summary>
		/// <returns></returns>
		public string P_2C()
		{

			const bool groupByCharge = false;
			Dictionary<int, int> dctPeptideStats = SummarizePSMs(groupByCharge);

			// Lookup the number of fully tryptic peptides (Cleavage_State = 2)
			int peptideCount;
			if (dctPeptideStats.TryGetValue(2, out peptideCount))
				return peptideCount.ToString();
			
			return "0";

		}
		
		/// <summary>
		/// P_3: Ratio of semi-tryptic / fully tryptic peptides
		/// </summary>
		/// <returns></returns>
		public string P_3()
		{

			Dictionary<int, int> dctPeptideStats = SummarizePSMs(groupByCharge:false);

			int peptideCountFullyTryptic;
			int peptideCountSemiTryptic;

			// Lookup the number of fully tryptic peptides (Cleavage_State = 2)
			if (!dctPeptideStats.TryGetValue(2, out peptideCountFullyTryptic))
				peptideCountFullyTryptic = 0;

			// Lookup the number of partially tryptic peptides (Cleavage_State = 1)
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
		/// Phos_2A: Number of tryptic phosphopeptides; total spectra count
		/// </summary>
		/// <returns></returns>
		public string Phos_2A()
		{
			return P_2A_Shared(phosphoPeptides: true);
		}

		/// <summary>
		/// Phos_2C: Number of tryptic phosphopeptides; unique peptide count
		/// </summary>
		/// <returns></returns>
		public string Phos_2C()
		{

			Dictionary<int, int> dctPeptideStats = SummarizePSMs(groupByCharge: false, phosphoPeptides: true);

			// Lookup the number of fully tryptic peptides (Cleavage_State = 2)
			int peptideCount;
			if (dctPeptideStats.TryGetValue(2, out peptideCount))
				return peptideCount.ToString();
			
			return "0";

		}

		protected string PhosphoFilter(bool phosphoPeptides)
		{
			if (phosphoPeptides)
				return " AND Phosphopeptide = 1";
			
			return "";
		}

		/// <summary>
		/// Counts the number of fully, partially, and non-tryptic peptides
		/// </summary>
		/// <param name="groupByCharge">If true, then counts charges separately</param>
		/// <returns></returns>
		protected Dictionary<int, int> SummarizePSMs(bool groupByCharge)
		{
			return SummarizePSMs(groupByCharge, phosphoPeptides: false);
		}

		/// <summary>
		/// Counts the number of fully, partially, and non-tryptic peptides
		/// </summary>
		/// <param name="groupByCharge">If true, then counts charges separately</param>
		/// <param name="phosphoPeptides">If true, then only uses phosphopeptides (only valid if UsingPHRP=True)</param>
		/// <returns></returns>
		protected Dictionary<int, int> SummarizePSMs(bool groupByCharge, bool phosphoPeptides)
		{
			var chargeSql = String.Empty;

			if (groupByCharge)
			{
				if (UsingPHRP)
					chargeSql = ", Charge ";
				else
					chargeSql = ", temp_xt.Charge ";
			}

			//SET DB QUERY
			if (UsingPHRP)
			{
				m_DBInterface.setQuery("SELECT Cleavage_State, Count(*) AS Peptides "
				                    + " FROM ( SELECT Unique_Seq_ID" + chargeSql + ", Max(Cleavage_State) AS Cleavage_State "
				                    + "        FROM temp_PSMs "
				                    + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
				                    + "          AND random_id=" + m_Random_ID
				                    + PhosphoFilter(phosphoPeptides)
				                    + " GROUP BY Unique_Seq_ID" + chargeSql + " ) StatsQ "
					                + " GROUP BY Cleavage_State;");
			}
			else
			{
				m_DBInterface.setQuery("SELECT Cleavage_State, Count(*) AS Peptides "
				                       + " FROM ( SELECT temp_xt_resulttoseqmap.Unique_Seq_ID" + chargeSql +
				                       ", Max(temp_xt_seqtoproteinmap.cleavage_state) AS Cleavage_State "
				                       + "        FROM temp_xt "
				                       +
				                       "             INNER JOIN temp_xt_resulttoseqmap ON temp_xt.result_id = temp_xt_resulttoseqmap.result_id "
				                       +
				                       "             INNER JOIN temp_xt_seqtoproteinmap ON temp_xt_resulttoseqmap.unique_seq_id = temp_xt_seqtoproteinmap.unique_seq_id "
				                       + "        WHERE temp_xt.Peptide_Expectation_Value_Log <= " + XTANDEM_LOG_EVALUE_THRESHOLD
				                       + "          AND temp_xt.random_id=" + m_Random_ID
				                       + "          AND temp_xt_resulttoseqmap.random_id=" + m_Random_ID
				                       + "          AND temp_xt_seqtoproteinmap.random_id=" + m_Random_ID
				                       + "        GROUP BY temp_xt_resulttoseqmap.Unique_Seq_ID" + chargeSql + " ) StatsQ "
				                       + " GROUP BY Cleavage_State;");
			}
			//DECLARE VARIABLES
			var dctPeptideStats = new Dictionary<int, int>();

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
