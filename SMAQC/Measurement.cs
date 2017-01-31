using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using PHRPReader;

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
            // Keys are quartile (1,2,3,4); values are the number of MS/MS scans in the quartile
            public Dictionary<int, int> ScanCount;

            // Keys are quartile (1,2,3,4); values are the number of confidently identified MS/MS scans in the quartile (MSGFSpecProb less than 1E-12)
            public Dictionary<int, int> PassFilt;
        }

        private enum eCachedResult
        {
            ScanFirstFilterPassingPeptide = 0,
            C2A_RegionScanStart = 1,
            C2A_RegionScanEnd = 2,
            C2A_TimeMinutes = 3,
            ReporterIonNoiseThreshold = 4
        }

        // Constants
        // private const int XTANDEM_LOG_EVALUE_THRESHOLD = -2;
        public const double MSGF_SPECPROB_THRESHOLD = 1e-12;

        // DB interface object
        private readonly DBWrapper m_DBInterface;

        // Random ID for temp tables
        private readonly int m_Random_ID;

        // Some measurements have data required by others ... will be stored here
        private readonly Dictionary<eCachedResult, double> m_ResultsStorage = new Dictionary<eCachedResult, double>();

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
        List<double> m_Cached_DS3;											// Stores MS1 max / MS1 sampled abundance
        List<double> m_Cached_DS3_Bottom50pct;                              // Stores MS1 max / MS1 sampled abundance for ratios in the bottom 50%

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

        // Cached data for the ReporterIon metrics
        private List<string> mReporterIonColumns;

        private readonly clsPeptideMassCalculator mPeptideMassCalculator;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="random_id"></param>
        /// <param name="DBInterface"></param>
        public Measurement(int random_id, DBWrapper DBInterface)
        {
            m_Random_ID = random_id;
            m_DBInterface = DBInterface;
            mPeptideMassCalculator = new clsPeptideMassCalculator();
        }

        /// <summary>
        /// Add (or update) entryName in mResultsStorage
        /// </summary>
        /// <param name="entryType"></param>
        /// <param name="value"></param>
        private void AddUpdateResultsStorage(eCachedResult entryType, double value)
        {
            if (m_ResultsStorage.ContainsKey(entryType))
                m_ResultsStorage[entryType] = value;
            else
                m_ResultsStorage.Add(entryType, value);
        }

        /// <summary>
        /// Clear cached data
        /// </summary>
        public void Reset()
        {
            m_ResultsStorage?.Clear();

            m_MedianPeakWidthDataCached = false;
            m_MPWCached_BestScan?.Clear();
            m_MPWCached_FWHMinScans?.Clear();
            m_MPWCached_OptimalPeakApexScanNumber?.Clear();
            m_MPWCached_ScanTime?.Clear();

            m_Cached_PSM_Stats_by_Charge?.Clear();

            m_PeptideSamplingStats?.Clear();

            m_Cached_DS3?.Clear();
            m_Cached_DS3_Bottom50pct?.Clear();

            m_Cached_BasePeakSignalToNoiseRatio?.Clear();
            m_Cached_TotalIonIntensity?.Clear();

            m_Cached_PeakMaxIntensity_5thPercentile = 0;
            m_Cached_PeakMaxIntensity_95thPercentile = 0;
            m_Cached_MS1_3?.Clear();

            m_Cached_DelM?.Clear();
            m_Cached_DelM_ppm?.Clear();

            m_MS2_4_Counts_Cached = false;
            m_Cached_MS2_4_Counts.ScanCount?.Clear();
            m_Cached_MS2_4_Counts.PassFilt?.Clear();

            mReporterIonColumns?.Clear();

        }

        private double ComputeMedian(List<double> values)
        {
            if (values.Count == 0)
                return 0;

            if (values.Count == 1)
                return values[0];

            // Assure the list is sorted
            values.Sort();

            // If odd # of results we must divide by 2 and take result 
            // (no need to add 1 due to starting at 0 position like you normally would if a median has odd total #)
            if (values.Count % 2 == 1)
            {
                // Odd
                var pos = (values.Count / 2);
                return values[pos];
            }
            else
            {
                // Even
                var pos = (values.Count / 2) - 1;
                return (values[pos] + values[pos + 1]) / 2;
            }
        }

        private double GetStoredValue(eCachedResult entryType, double valueIfMissing)
        {
            double value;

            if (m_ResultsStorage.TryGetValue(entryType, out value))
                return value;

            return valueIfMissing;
        }

        private int GetStoredValueInt(eCachedResult entryType, int valueIfMissing)
        {
            var value = GetStoredValue(entryType, valueIfMissing);
            return (int)value;
        }

        private bool PeptideScorePassesFilter(double peptideScore)
        {
            if (peptideScore <= MSGF_SPECPROB_THRESHOLD)
                return true;

            return false;
        }

        /// <summary>
        /// C-1A: Fraction of peptides identified more than 4 minutes earlier than the chromatographic peak apex
        /// </summary>
        /// <returns></returns>
        public string C_1A()
        {
            return C_1_Shared(countTailingPeptides: false);
        }

        /// <summary>
        /// C-1B: Fraction of peptides identified more than 4 minutes later than the chromatographic peak apex
        /// </summary>
        /// <returns></returns>
        public string C_1B()
        {
            return C_1_Shared(countTailingPeptides: true);
        }

        /// <summary>
        /// Counts the number of peptides identified more than 4 minutes earlier or more than 4 minutes later than the chromatographic peak apex
        /// </summary>
        /// <param name="countTailingPeptides">False means to count early eluting peptides; True means to count late-eluting peptides</param>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        private string C_1_Shared(bool countTailingPeptides)
        {

            m_DBInterface.SetQuery("SELECT temp_PSMs.Scan, t1.FragScanNumber, t1.OptimalPeakApexScanNumber,"
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


            var psm_count_late_or_early = 0;
            var psm_count_total = 0;

            string[] fields = { "Scan", "FragScanNumber", "OptimalPeakApexScanNumber", "ScanTime1", "ScanTimePeakApex" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                // Calculate difference
                double temp_difference;
                if (countTailingPeptides)
                    temp_difference = (Convert.ToDouble(measurementResults["ScanTime1"]) - Convert.ToDouble(measurementResults["ScanTimePeakApex"]));
                else
                    temp_difference = (Convert.ToDouble(measurementResults["ScanTimePeakApex"]) - Convert.ToDouble(measurementResults["ScanTime1"]));

                // If difference is greater than 4 minutes, then increment the counter
                if (temp_difference >= 4.00)
                {
                    psm_count_late_or_early += 1;
                }

                psm_count_total++;
            }

            // Calculate solution
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
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string C_2A()
        {

            m_DBInterface.SetQuery("SELECT temp_PSMs.Scan, t1.FragScanNumber as ScanNumber,"
                                    + "    temp_scanstats.ScanTime as ScanTime1 "
                                    + " FROM temp_PSMs, temp_scanstats, temp_sicstats as t1 "
                                    + " WHERE temp_PSMs.Scan = t1.FragScanNumber "
                                    + "  AND temp_PSMs.Scan = temp_scanstats.ScanNumber "
                                    + "  AND temp_PSMs.random_id=" + m_Random_ID
                                    + "  AND temp_scanstats.random_id=" + m_Random_ID
                                    + "  AND t1.random_id=" + m_Random_ID + " "
                                    + "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                    + " ORDER BY Scan;");

            // This list stores scan numbers and elution times for filter-passing peptides; duplicate scans are not allowed
            var lstFilterPassingPeptides = new SortedList<int, double>();

            string[] fields = { "Scan", "ScanNumber", "ScanTime1" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                // Filter-passing peptide; Append to the dictionary
                int scanNumber;
                if (int.TryParse(measurementResults["ScanNumber"], out scanNumber))
                {
                    double scanTime;
                    if (double.TryParse(measurementResults["ScanTime1"], out scanTime))
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

                // Determine the scan numbers at which the 25th and 75th percentiles are located
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
                // Add to global list for use with MS_2A/B
                // ScanFirstFilterPassingPeptide is the scan number of the first filter-passing peptide
                AddUpdateResultsStorage(eCachedResult.ScanFirstFilterPassingPeptide, lstFilterPassingPeptides.Keys.Min());
            }

            // Cache the scan numbers at the start and end of the intequartile region
            AddUpdateResultsStorage(eCachedResult.C2A_RegionScanStart, C2AScanStart);
            AddUpdateResultsStorage(eCachedResult.C2A_RegionScanEnd, C2AScanEnd);

            var timeMinutes = C2AScanTimeEnd - C2AScanTimeStart;

            AddUpdateResultsStorage(eCachedResult.C2A_TimeMinutes, timeMinutes);

            return timeMinutes.ToString("0.0000");
        }


        /// <summary>
        /// C-2B: Rate of peptide identification during C-2A
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string C_2B()
        {

            m_DBInterface.SetQuery("SELECT temp_PSMs.Scan, t1.FragScanNumber as ScanNumber,"
                                    + "    temp_scanstats.ScanTime as ScanTime1 "
                                    + " FROM temp_PSMs, temp_scanstats, temp_sicstats as t1 "
                                    + " WHERE temp_PSMs.Scan = t1.FragScanNumber "
                                    + "  AND temp_PSMs.Scan = temp_scanstats.ScanNumber "
                                    + "  AND temp_PSMs.random_id=" + m_Random_ID
                                    + "  AND temp_scanstats.random_id=" + m_Random_ID
                                    + "  AND t1.random_id=" + m_Random_ID
                                    + "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                    + " ORDER BY Scan;");

            // This list keeps track of the scan numbers already processed so that we can avoid double-counting a scan number
            var lstScansWithFilterPassingIDs = new SortedSet<int>();

            var timeMinutesC2A = GetStoredValue(eCachedResult.C2A_TimeMinutes, 0);
            var scanStartC2A = GetStoredValueInt(eCachedResult.C2A_RegionScanStart, 0);
            var scanEndC2A = GetStoredValueInt(eCachedResult.C2A_RegionScanEnd, 0);

            string[] fields = { "Scan", "ScanNumber", "ScanTime1" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                // Filter-passing peptide; Append to the dictionary
                int scanNumber;
                if (int.TryParse(measurementResults["ScanNumber"], out scanNumber))
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

                // Round the result
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

            // Compute the result
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

            // Compute the result
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

            // Compute the result
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

            // Compute the result
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

            // Compute the result
            return ComputeMedianPeakWidth(startScanRelative, endScanRelative);
        }

        private void Cache_MedianPeakWidth_Data()
        {

            var lstPSMs = new List<udtPeptideEntry>();							// Cached, filter-passing peptide-spectrum matches

            m_MPWCached_BestScan = new List<int>();										// Best Scan Number for each peptide
            m_MPWCached_FWHMinScans = new Dictionary<int, double>();					// Full width at half max, in scans; keys are FragScanNumbers
            m_MPWCached_OptimalPeakApexScanNumber = new Dictionary<int, int>();			// Optimal peak apex scan number; keys are FragScanNumbers
            m_MPWCached_ScanTime = new Dictionary<int, double>();						// Scan time for given scan number; keys are scan numbers

            m_DBInterface.SetQuery("SELECT Scan, Charge, Peptide_Sequence, MSGFSpecProb AS Peptide_Score"
                                + " FROM temp_PSMs "
                                + " WHERE random_id=" + m_Random_ID);

            string[] fields1 = { "Scan", "Charge", "Peptide_Sequence", "Peptide_Score" };

            m_DBInterface.initReader();

            // Read and cache the data since we need to trim off the prefix and suffix letters from the peptide sequence

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields1, out measurementResults) && measurementResults.Count > 0)
            {

                string peptideResidues;
                string peptidePrefix;
                string peptideSuffix;

                clsPeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(measurementResults["Peptide_Sequence"], out peptideResidues, out peptidePrefix, out peptideSuffix);

                var currentPeptide = new udtPeptideEntry();

                if (int.TryParse(measurementResults["Scan"], out currentPeptide.Scan))
                {
                    if (int.TryParse(measurementResults["Charge"], out currentPeptide.Charge))
                    {
                        if (double.TryParse(measurementResults["Peptide_Score"], out currentPeptide.Score))
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


                // Check whether previous peptide sequences are equivalent and have the same charge
                if (previousPeptide.Peptide_Sequence.Equals(psm.Peptide_Sequence) && previousPeptide.Charge == psm.Charge)
                {
                    // Use the minimum (either previous best evalue or current peptide score)
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

                // Update previous values for next loop
                previousPeptide.Scan = psm.Scan;
                previousPeptide.Charge = psm.Charge;
                previousPeptide.Peptide_Sequence = string.Copy(psm.Peptide_Sequence);
                previousPeptide.Score = Best_Score;
            }

            // Sort the data
            m_MPWCached_BestScan.Sort();

            m_DBInterface.SetQuery("SELECT FragScanNumber, FWHMInScans, OptimalPeakApexScanNumber FROM temp_sicstats WHERE temp_sicstats.random_id=" + m_Random_ID);

            string[] fields2 = { "FragScanNumber", "FWHMInScans", "OptimalPeakApexScanNumber" };

            m_DBInterface.initReader();

            // Fetch columns d-f

            while (m_DBInterface.ReadLines(fields2, out measurementResults) && measurementResults.Count > 0)
            {
                var fragScanNumber = Convert.ToInt32(measurementResults["FragScanNumber"]);

                m_MPWCached_FWHMinScans.Add(fragScanNumber, Convert.ToDouble(measurementResults["FWHMInScans"]));
                m_MPWCached_OptimalPeakApexScanNumber.Add(fragScanNumber, Convert.ToInt32(measurementResults["OptimalPeakApexScanNumber"]));
            }

            m_DBInterface.SetQuery("SELECT temp_scanstats.ScanNumber, temp_scanstats.ScanTime FROM temp_scanstats WHERE temp_scanstats.random_id=" + m_Random_ID);

            string[] fields3 = { "ScanNumber", "ScanTime" };

            m_DBInterface.initReader();

            // Fetch columns h-i
            while (m_DBInterface.ReadLines(fields3, out measurementResults) && measurementResults.Count > 0)
            {
                m_MPWCached_ScanTime.Add(Convert.ToInt32(measurementResults["ScanNumber"]), Convert.ToDouble(measurementResults["ScanTime"]));
            }

            m_MedianPeakWidthDataCached = true;

        }

        private string ComputeMedianPeakWidth(double startScanRelative, double endScanRelative)
        {
            var result = new List<double>();

            if (!m_MedianPeakWidthDataCached)
            {
                Cache_MedianPeakWidth_Data();
            }

            // Loop through bestscan
            for (var i = 0; i < m_MPWCached_BestScan.Count; i++)
            {
                // Find index + optimal peak apex scan +- FWHM for each result (columns: m,o)
                var OptimalPeakApexScanMinusFWHM = m_MPWCached_OptimalPeakApexScanNumber[m_MPWCached_BestScan[i]] - Convert.ToInt32(Math.Ceiling(m_MPWCached_FWHMinScans[m_MPWCached_BestScan[i]] / 2));
                var OptimalPeakApexScanPlusFWHM = m_MPWCached_OptimalPeakApexScanNumber[m_MPWCached_BestScan[i]] + Convert.ToInt32(Math.Ceiling(m_MPWCached_FWHMinScans[m_MPWCached_BestScan[i]] / 2));

                // Find other columns [n,p, q,r,t]

                double start_time;

                if (m_MPWCached_ScanTime.TryGetValue(OptimalPeakApexScanMinusFWHM, out start_time))
                {
                    double end_time;
                    if (m_MPWCached_ScanTime.TryGetValue(OptimalPeakApexScanPlusFWHM, out end_time))
                    {

                        var end_minus_start_in_secs = (end_time - start_time) * 60;
                        var percent = (i + 1) / (double)m_MPWCached_BestScan.Count;

                        // check for valid range data then add to our results
                        if (percent >= startScanRelative && percent <= endScanRelative)
                        {
                            // We are within our valid range ... so add end_minus_start_in_secs to the list
                            result.Add(end_minus_start_in_secs);
                        }

                    }
                }
            }

            var resultText = string.Empty;

            if (result.Count > 0)
            {
                // Calculate the median
                var median = ComputeMedian(result);

                // Round the result
                resultText = median.ToString("0.00");

                // Implementation notes
                /*
                 * result.Count == # OF U COLUMN VALID RESULTS
                 * Console.WriteLine("MEDIAN={0} -- {1} [POS={2}]", result[pos], result[pos + 1], pos); == HELPFUL FOR DEBUGGING
                */
            }

            return resultText;
        }

        /// <summary>
        /// DS-1A: Count of peptides with one spectrum / count of peptides with two spectra
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string DS_1A()
        {
            double result = 0;

            if (m_PeptideSamplingStats == null || m_PeptideSamplingStats.Count == 0)
                Cache_DS_1_Data();

            // Calculate the value; return 0 if number of peptides identified with 2 spectra is 0

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
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string DS_1B()
        {
            // Keys are the number of spectra that a peptide was observed in (passing filters) and values are the number of peptides identified by Key spectra</param>

            double result = 0;

            if (m_PeptideSamplingStats == null || m_PeptideSamplingStats.Count == 0)
                Cache_DS_1_Data();

            // Calculate the value; return 0 if number of peptides identified with 3 spectra is 0

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
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        private void Cache_DS_1_Data()
        {

            m_DBInterface.SetQuery("SELECT Spectra, Count(*) AS Peptides "
                                + " FROM ( SELECT Unique_Seq_ID, COUNT(*) AS Spectra "
                                + "        FROM ( SELECT Unique_Seq_ID, Scan "
                                + "               FROM temp_PSMs "
                                + "               WHERE random_id=" + m_Random_ID
                                + "                 AND MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                + "               GROUP BY Unique_Seq_ID, Scan ) DistinctQ "
                                + "        GROUP BY Unique_Seq_ID ) CountQ "
                                + " GROUP BY Spectra;");

            m_PeptideSamplingStats = new Dictionary<int, int>();

            string[] fields = { "Spectra", "Peptides" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                var spectra = Convert.ToInt32(measurementResults["Spectra"]);
                var peptides = Convert.ToInt32(measurementResults["Peptides"]);

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

        private int DS_2_Shared(int msLevel)
        {

            var scanStartC2A = GetStoredValueInt(eCachedResult.C2A_RegionScanStart, 0);
            var scanEndC2A = GetStoredValueInt(eCachedResult.C2A_RegionScanEnd, 0);

            m_DBInterface.SetQuery("SELECT COUNT(*) AS ScanCount "
                + " FROM temp_scanstats "
                + " WHERE temp_scanstats.random_id=" + m_Random_ID
                + "   AND ScanType = " + msLevel
                + "   AND ScanNumber >= " + scanStartC2A
                + "   AND ScanNumber <= " + scanEndC2A);

            string[] fields = { "ScanCount" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            m_DBInterface.ReadSingleLine(fields, out measurementResults);

            var intScanCount = Convert.ToInt32(measurementResults["ScanCount"]);

            return intScanCount;
        }

        /// <summary>
        /// IS-2: Median precursor m/z for all peptides
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string IS_2()
        {

            m_DBInterface.SetQuery("SELECT Peptide_MH, Charge "
                                + " FROM temp_PSMs "
                                + " WHERE random_id=" + m_Random_ID
                                + "   AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);

            var MZ_list = new SortedSet<double>();

            string[] fields = { "Peptide_MH", "Charge" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {

                var temp_mz = mPeptideMassCalculator.ConvoluteMass(Convert.ToDouble(measurementResults["Peptide_MH"]), 1, Convert.ToInt32(measurementResults["Charge"]));

                if (!MZ_list.Contains(temp_mz))
                    MZ_list.Add(temp_mz);

            }

            // Compute the median
            var median = ComputeMedian(MZ_list.ToList());

            // Round the result
            return median.ToString("0.0000");
        }

        /// <summary>
        /// IS-3A: Count of 1+ peptides / count of 2+ peptides
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string IS_3A()
        {
            int psm_count_charge1;
            double result = 0;

            if (m_Cached_PSM_Stats_by_Charge == null || m_Cached_PSM_Stats_by_Charge.Count == 0)
            {
                Cache_IS3_Data();
            }

            if (m_Cached_PSM_Stats_by_Charge != null && m_Cached_PSM_Stats_by_Charge.TryGetValue(1, out psm_count_charge1))
            {
                int psm_count_charge2;
                if (m_Cached_PSM_Stats_by_Charge.TryGetValue(2, out psm_count_charge2))
                    result = psm_count_charge1 / (double)psm_count_charge2;
            }

            return result.ToString("0.000000");
        }

        /// <summary>
        /// IS-3B: Count of 3+ peptides / count of 2+ peptides
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string IS_3B()
        {
            int psm_count_charge2;
            double result = 0;

            if (m_Cached_PSM_Stats_by_Charge == null || m_Cached_PSM_Stats_by_Charge.Count == 0)
            {
                Cache_IS3_Data();
            }

            if (m_Cached_PSM_Stats_by_Charge != null && m_Cached_PSM_Stats_by_Charge.TryGetValue(2, out psm_count_charge2))
            {
                int psm_count_charge3;
                if (m_Cached_PSM_Stats_by_Charge.TryGetValue(3, out psm_count_charge3))
                    result = psm_count_charge3 / (double)psm_count_charge2;
            }

            return result.ToString("0.000000");
        }

        /// <summary>
        /// IS-3C: Count of 4+ peptides / count of 2+ peptides
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string IS_3C()
        {
            int psm_count_charge2;
            double result = 0;

            if (m_Cached_PSM_Stats_by_Charge == null || m_Cached_PSM_Stats_by_Charge.Count == 0)
            {
                Cache_IS3_Data();
            }

            if (m_Cached_PSM_Stats_by_Charge != null && m_Cached_PSM_Stats_by_Charge.TryGetValue(2, out psm_count_charge2))
            {
                int psm_count_charge4;
                if (m_Cached_PSM_Stats_by_Charge.TryGetValue(4, out psm_count_charge4))
                    result = psm_count_charge4 / (double)psm_count_charge2;
            }

            return result.ToString("0.000000");
        }

        private void Cache_IS3_Data()
        {

            m_DBInterface.SetQuery("SELECT Charge, COUNT(*) AS PSMs "
                    + " FROM temp_PSMs "
                    + " WHERE random_id=" + m_Random_ID
                    + "   AND MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                    + " GROUP BY Charge;");

            string[] fields = { "Charge", "PSMs" };

            m_DBInterface.initReader();

            m_Cached_PSM_Stats_by_Charge = new Dictionary<int, int>();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                int charge;
                if (int.TryParse(measurementResults["Charge"], out charge))
                {
                    int psm_count;
                    if (int.TryParse(measurementResults["PSMs"], out psm_count))
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

            m_DBInterface.SetQuery("SELECT temp_scanstatsex.Ion_Injection_Time "
                + " FROM temp_scanstats, temp_scanstatsex "
                + " WHERE temp_scanstats.ScanNumber = temp_scanstatsex.ScanNumber "
                + "  AND temp_scanstatsex.random_id = " + m_Random_ID
                + "  AND temp_scanstats.random_id = " + m_Random_ID
                + "  AND temp_scanstats.ScanType = 1 "
                + " ORDER BY temp_scanstats.ScanNumber;");


            var lstValues = new List<double>();

            string[] fields = { "Ion_Injection_Time" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                // Add to the filter list
                lstValues.Add(Convert.ToDouble(measurementResults["Ion_Injection_Time"]));
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
            var median = 0.00;

            if (m_Cached_BasePeakSignalToNoiseRatio == null || m_Cached_BasePeakSignalToNoiseRatio.Count == 0)
                Cache_MS1_2_Data();

            if (m_Cached_BasePeakSignalToNoiseRatio != null && m_Cached_BasePeakSignalToNoiseRatio.Count > 0)
            {
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
            if (m_Cached_TotalIonIntensity == null || m_Cached_TotalIonIntensity.Count == 0)
                Cache_MS1_2_Data();

            var median = ComputeMedian(m_Cached_TotalIonIntensity);

            // Divide by 1000
            median = median / 1000;

            return Convert.ToString(median, CultureInfo.InvariantCulture);
        }

        private void Cache_MS1_2_Data()
        {

            var scanFirstPeptide = GetStoredValueInt(eCachedResult.ScanFirstFilterPassingPeptide, 0);
            var scanEndC2A = GetStoredValueInt(eCachedResult.C2A_RegionScanEnd, 0);

            m_DBInterface.SetQuery("SELECT BasePeakSignalToNoiseRatio, TotalIonIntensity "
                + " FROM temp_scanstats"
                + " WHERE temp_scanstats.random_id=" + m_Random_ID
                + "   AND ScanType = 1 "
                + "   AND ScanNumber >= " + scanFirstPeptide
                + "   AND ScanNumber <= " + scanEndC2A);

            string[] fields = { "BasePeakSignalToNoiseRatio", "TotalIonIntensity" };

            m_DBInterface.initReader();

            m_Cached_BasePeakSignalToNoiseRatio = new List<double>();
            m_Cached_TotalIonIntensity = new List<double>();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                m_Cached_BasePeakSignalToNoiseRatio.Add(Convert.ToDouble(measurementResults["BasePeakSignalToNoiseRatio"]));
                m_Cached_TotalIonIntensity.Add(Convert.ToDouble(measurementResults["TotalIonIntensity"]));
            }

        }

        /// <summary>
        /// MS1_3A: Dynamic range estimate using 95th percentile peptide peak apex intensity / 5th percentile
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS1_3A()
        {
            double final = 0;

            Cache_MS1_3_Data();

            if (m_Cached_PeakMaxIntensity_5thPercentile > 0)
                final = m_Cached_PeakMaxIntensity_95thPercentile / m_Cached_PeakMaxIntensity_5thPercentile;

            // Round the result
            return final.ToString("0.000");
        }

        /// <summary>
        /// MS1_3B: Median peak apex intensity for all peptides
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS1_3B()
        {
            if (m_Cached_MS1_3 == null || m_Cached_MS1_3.Count == 0)
                Cache_MS1_3_Data();

            var median = ComputeMedian(m_Cached_MS1_3);

            // Round based on the magnitude of the intensity

            if (median > 100)
                return median.ToString("0");

            return median.ToString("0.0");
        }

        private void Cache_MS1_3_Data()
        {

            m_DBInterface.SetQuery("SELECT temp_sicstats.PeakMaxIntensity"
                + " FROM temp_sicstats, temp_PSMs"
                + " WHERE temp_sicstats.FragScanNumber=temp_PSMs.Scan"
                + "   AND temp_sicstats.random_id=" + m_Random_ID
                + "   AND temp_PSMs.random_id=" + m_Random_ID
                + "   AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                + " ORDER BY temp_sicstats.PeakMaxIntensity, temp_PSMs.Result_ID DESC;");

            // This list stores the max peak intensity for 5-95%
            var MPI_list = new List<double>();

            string[] fields = { "PeakMaxIntensity" };

            m_DBInterface.initReader();

            m_Cached_PeakMaxIntensity_5thPercentile = 0;
            m_Cached_PeakMaxIntensity_95thPercentile = 0;
            m_Cached_MS1_3 = new List<double>();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                // Add to the filter list
                m_Cached_MS1_3.Add(Convert.ToDouble(measurementResults["PeakMaxIntensity"]));
            }

            if (m_Cached_MS1_3.Count > 0)
            {
                // Store values between the 5th and 95th percentiles
                for (var i = 0; i < m_Cached_MS1_3.Count; i++)
                {

                    // Check if between 5-95%
                    var percent = Convert.ToDouble(i) / Convert.ToDouble(m_Cached_MS1_3.Count);
                    if (percent >= 0.05 && percent <= 0.95)
                    {
                        // Add to the MPI list
                        MPI_list.Add(m_Cached_MS1_3[i]);
                    }

                }
            }

            if (MPI_list.Count > 0)
            {
                // Calculate final values
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
            return DS_3_Shared(bottom50Pct: false);
        }

        /// <summary>
        /// DS_3B: Median of MS1 max / MS1 sampled abundance; limit to bottom 50% of peptides by abundance
        /// </summary>
        /// <returns></returns>
        public string DS_3B()
        {
            return DS_3_Shared(bottom50Pct: true);
        }

        private void DS_3_CacheData()
        {

            m_DBInterface.SetQuery("SELECT ParentIonIntensity, PeakMaxIntensity"
                + " FROM temp_sicstats, temp_PSMs "
                + " WHERE temp_sicstats.FragScanNumber=temp_PSMs.Scan"
                + "   AND temp_sicstats.random_id=" + m_Random_ID
                + "   AND temp_PSMs.random_id=" + m_Random_ID
                + "   AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);

            // Ratio of PeakMaxIntensity over ParentIonIntensity
            var lstResult = new List<double>();

            string[] fields = { "ParentIonIntensity", "PeakMaxIntensity" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {

                // Compute the ratio of the PeakMaxIntensity over ParentIonIntensity
                var parentIonIntensity = Convert.ToDouble(measurementResults["ParentIonIntensity"]);
                double ratioPeakMaxToParentIonIntensity;
                if (parentIonIntensity > 0)
                    ratioPeakMaxToParentIonIntensity = Convert.ToDouble(measurementResults["PeakMaxIntensity"]) / parentIonIntensity;
                else
                    ratioPeakMaxToParentIonIntensity = 0;

                lstResult.Add(ratioPeakMaxToParentIonIntensity);

            }

            // Sort the values
            lstResult.Sort();

            // Stores MS1 max / MS1 sampled abundance
            m_Cached_DS3 = new List<double>();

            // Stores MS1 max / MS1 sampled abundance for ratios in the bottom 50%
            m_Cached_DS3_Bottom50pct = new List<double>();

            // Loop through all keys
            for (var i = 0; i < lstResult.Count; i++)
            {

                // Add to the list
                m_Cached_DS3.Add(lstResult[i]);

                if ((i + 1) / (double)lstResult.Count <= 0.5)
                {
                    // In valid bottom 50% so add to DS3_Bottom50pct
                    m_Cached_DS3_Bottom50pct.Add(lstResult[i]);
                }

            }
        }

        /// <summary>
        /// Median of MS1 max / MS1 sampled abundance
        /// </summary>
        /// <param name="bottom50Pct">Set to true to limit to the bottom 50% of peptides by abundance</param>
        /// <returns></returns>
        private string DS_3_Shared(bool bottom50Pct)
        {
            if (m_Cached_DS3 == null || m_Cached_DS3.Count == 0)
            {
                DS_3_CacheData();
            }

            double median;
            if (bottom50Pct)
                median = ComputeMedian(m_Cached_DS3_Bottom50pct);
            else
                median = ComputeMedian(m_Cached_DS3);

            // Round the result
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

        private void IS_1_Shared(int foldThreshold, out int countMS1Jump10x, out int countMS1Fall10x)
        {

            m_DBInterface.SetQuery("SELECT ScanNumber, BasePeakIntensity  "
                + " FROM temp_scanstats "
                + " WHERE temp_scanstats.random_id=" + m_Random_ID
                + "   AND ScanType = 1");

            double bpiPrevious = -1;

            // Count for IS_1A
            countMS1Jump10x = 0;

            // Count for IS_1B
            countMS1Fall10x = 0;

            // Validate the foldThreshold
            if (foldThreshold < 2)
                foldThreshold = 2;

            string[] fields = { "ScanNumber", "BasePeakIntensity" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {

                var bpiCurrent = Convert.ToDouble(measurementResults["BasePeakIntensity"]);

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
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS1_5A()
        {

            if (m_Cached_DelM == null || m_Cached_DelM.Count == 0)
                Cache_MS1_5_Data();

            if (m_Cached_DelM == null || m_Cached_DelM.Count == 0)
                return "0";

            var median = ComputeMedian(m_Cached_DelM);

            // Round the result
            return median.ToString("0.000000");
        }

        /// <summary>
        /// MS1_5B: Median of absolute value of precursor mass error (Th)
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS1_5B()
        {

            if (m_Cached_DelM == null || m_Cached_DelM.Count == 0)
                Cache_MS1_5_Data();

            if (m_Cached_DelM == null || m_Cached_DelM.Count == 0)
            {
                return "0";
            }

            var lstAbsDelM = new List<double>(m_Cached_DelM.Count);

            foreach (var value in m_Cached_DelM)
            {
                lstAbsDelM.Add(Math.Abs(value));
            }

            var average = lstAbsDelM.Average();

            // Round the result
            return average.ToString("0.000000");

        }

        /// <summary>
        /// MS1_5C: Median of precursor mass error (ppm)
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS1_5C()
        {
            if (m_Cached_DelM_ppm == null || m_Cached_DelM_ppm.Count == 0)
                Cache_MS1_5_Data();

            if (m_Cached_DelM_ppm == null || m_Cached_DelM_ppm.Count == 0)
                return "0";

            var median = ComputeMedian(m_Cached_DelM_ppm);

            return median.ToString("0.000");
        }

        /// <summary>
        /// MS1_5D: Interquartile distance in ppm-based precursor mass error
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS1_5D()
        {
            if (m_Cached_DelM_ppm == null || m_Cached_DelM_ppm.Count == 0)
                Cache_MS1_5_Data();

            if (m_Cached_DelM_ppm == null || m_Cached_DelM_ppm.Count == 0)
                return "0";

            // Sort the DelM_ppm values
            m_Cached_DelM_ppm.Sort();

            var lstInterquartilePPMErrors = new List<double>();

            // Calculate inter_quartile start and end
            var inter_quartile_start = Convert.ToInt32(Math.Round(0.25 * Convert.ToDouble(m_Cached_DelM_ppm.Count)));
            var inter_quartile_end = Convert.ToInt32(Math.Round(0.75 * Convert.ToDouble(m_Cached_DelM_ppm.Count)));

            // Loop through the cached ppm errors
            for (var i = 0; i < m_Cached_DelM_ppm.Count; i++)
            {
                if ((i >= inter_quartile_start) && (i <= inter_quartile_end))
                {
                    lstInterquartilePPMErrors.Add(m_Cached_DelM_ppm[i]);
                }
            }

            var median = ComputeMedian(lstInterquartilePPMErrors);

            // Round the result
            return median.ToString("0.000");
        }

        private void Cache_MS1_5_Data()
        {

            m_DBInterface.SetQuery("SELECT temp_PSMs.Peptide_MH, temp_PSMs.Charge, temp_sicstats.MZ, temp_PSMs.DelM_Da, temp_PSMs.DelM_PPM"
                    + " FROM temp_PSMs, temp_sicstats"
                    + " WHERE temp_sicstats.FragScanNumber=temp_PSMs.Scan AND temp_sicstats.random_id=" + m_Random_ID + " AND temp_PSMs.random_id=" + m_Random_ID
                    + " AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);

            const double massC13 = 1.00335483;

            string[] fields = { "Peptide_MH", "Charge", "MZ", "DelM_Da", "DelM_PPM" };

            m_DBInterface.initReader();

            m_Cached_DelM = new List<double>();
            m_Cached_DelM_ppm = new List<double>();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                // Calculate the theoretical monoisotopic mass of the peptide
                var theoMonoMass = mPeptideMassCalculator.ConvoluteMass(Convert.ToDouble(measurementResults["Peptide_MH"]), 1, 0);

                // Compute observed precursor mass, as monoisotopic mass					
                int observedCharge = Convert.ToInt16(measurementResults["Charge"]);
                var observedMonoMass = mPeptideMassCalculator.ConvoluteMass(Convert.ToDouble(measurementResults["MZ"]), observedCharge, 0);

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
        /// MS2_1: Median MS2 ion injection time for identified peptides
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_1()
        {

            m_DBInterface.SetQuery("SELECT temp_scanstatsex.Ion_Injection_Time "
                + " FROM temp_PSMs, temp_scanstatsex "
                + " WHERE temp_PSMs.Scan=temp_scanstatsex.ScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_scanstatsex.random_id=" + m_Random_ID
                + "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);

            var FilterList = new List<double>();

            string[] fields = { "Ion_Injection_Time" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                FilterList.Add(Convert.ToDouble(measurementResults["Ion_Injection_Time"]));
            }

            var median = ComputeMedian(FilterList);

            // Round the result
            return median.ToString("0.000");
        }

        /// <summary>
        /// MS2_2: Median S/N value for identified MS2 spectra
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_2()
        {

            m_DBInterface.SetQuery("SELECT temp_scanstats.BasePeakSignalToNoiseRatio "
                + " FROM temp_PSMs, temp_scanstats "
                + " WHERE temp_PSMs.Scan=temp_scanstats.ScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_scanstats.random_id=" + m_Random_ID
                + "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);

            var FilterList = new List<double>();
            var FinishedList = new List<double>();

            string[] fields = { "BasePeakSignalToNoiseRatio" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                // Add to the filtered list
                FilterList.Add(Convert.ToDouble(measurementResults["BasePeakSignalToNoiseRatio"]));
            }

            for (var i = 0; i < FilterList.Count; i++)
            {
                if ((i + 1) <= (FilterList.Count * 0.75))
                {
                    // Below the 75th percentile, so add to the final list
                    FinishedList.Add(FilterList[i]);
                }
            }

            var median = ComputeMedian(FinishedList);

            // Round the result
            return median.ToString("0.000");
        }

        /// <summary>
        /// MS2_3: Median number of peaks in all MS2 spectra
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_3()
        {

            m_DBInterface.SetQuery("SELECT temp_scanstats.IonCountRaw "
                + " FROM temp_PSMs, temp_scanstats "
                + " WHERE temp_PSMs.Scan=temp_scanstats.ScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_scanstats.random_id=" + m_Random_ID
                + "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);

            var FilterList = new List<double>();

            string[] fields = { "IonCountRaw" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                FilterList.Add(Convert.ToDouble(measurementResults["IonCountRaw"]));
            }

            var median = ComputeMedian(FilterList);

            // Round to the nearest integer
            return median.ToString("0");
        }

        /// <summary>
        /// MS2_4A: Fraction of all MS2 spectra identified; low abundance quartile (determined using MS1 intensity of identified peptides)
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
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
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
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
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
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
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_4D()
        {
            if (!m_MS2_4_Counts_Cached)
                Cache_MS2_4_Data();

            var result = Compute_MS2_4_Ratio(4);
            return result.ToString("0.0000");

        }

        private double Compute_MS2_4_Ratio(int quartile)
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

        private void Cache_MS2_4_Data()
        {

            m_DBInterface.SetQuery("SELECT COUNT(*) as MS2ScanCount "
                + " FROM (SELECT DISTINCT temp_PSMs.Scan, temp_sicstats.PeakMaxIntensity "
                + "       FROM temp_PSMs, temp_sicstats "
                + "       WHERE temp_PSMs.Scan=temp_sicstats.FragScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_sicstats.random_id=" + m_Random_ID
                + "      ) LookupQ;");

            string[] fields1 = { "MS2ScanCount" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            m_DBInterface.ReadSingleLine(fields1, out measurementResults);
            var scanCountMS2 = Convert.ToInt32(measurementResults["MS2ScanCount"]);

            // Note that we sort by ascending PeakMaxIntensity
            // Thus, quartile 1 in m_Cached_MS2_4_Counts will have the lowest abundance peptides

            m_DBInterface.SetQuery("SELECT temp_PSMs.Scan, temp_sicstats.PeakMaxIntensity, Min(temp_PSMs.MSGFSpecProb) AS Peptide_Score "
                + " FROM temp_PSMs, temp_sicstats"
                + " WHERE temp_PSMs.Scan=temp_sicstats.FragScanNumber AND temp_PSMs.random_id=" + m_Random_ID + " AND temp_sicstats.random_id=" + m_Random_ID
                + " GROUP BY temp_PSMs.Scan, temp_sicstats.PeakMaxIntensity "
                + " ORDER BY temp_sicstats.PeakMaxIntensity;");

            // Keys are quartile (1,2,3,4); values are the number of MS/MS scans in the quartile
            m_Cached_MS2_4_Counts.ScanCount = new Dictionary<int, int>();

            // Keys are quartile (1,2,3,4); values are the number of confidently identified MS/MS scans in the quartile
            m_Cached_MS2_4_Counts.PassFilt = new Dictionary<int, int>();

            for (var i = 1; i <= 4; i++)
            {
                m_Cached_MS2_4_Counts.ScanCount.Add(i, 0);
                m_Cached_MS2_4_Counts.PassFilt.Add(i, 0);
            }

            var running_scan_count = 1;

            string[] fields2 = { "Scan", "Peptide_Score", "PeakMaxIntensity" };

            m_DBInterface.initReader();

            while (m_DBInterface.ReadLines(fields2, out measurementResults) && measurementResults.Count > 0)
            {
                var passed_filter = false;

                // Compare the peptide_score vs. the threshold
                double peptideScore;
                if (double.TryParse(measurementResults["Peptide_Score"], out peptideScore) && PeptideScorePassesFilter(peptideScore))
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

        private void UpdateMS2_4_QuartileStats(int quartile, bool passed_filter)
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

            m_DBInterface.SetQuery("SELECT Scan, Max(-Log10(MSGFSpecProb)) AS Peptide_Score"
                + " FROM temp_PSMs "
                + " WHERE random_id=" + m_Random_ID
                + " GROUP BY Scan "
                + " ORDER BY Scan;");

            // Track X!Tandem Hyperscore or -Log10(MSGFSpecProb)
            var Peptide_score_List = new List<double>();

            string[] fields = { "Scan", "Peptide_Score" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                double peptideScore;
                if (double.TryParse(measurementResults["Peptide_Score"], out peptideScore))
                {
                    Peptide_score_List.Add(peptideScore);
                }
            }

            var median = ComputeMedian(Peptide_score_List);

            // Round the result
            return median.ToString("0.00");
        }

        /// <summary>
        /// P_1B: Median peptide ID score (X!Tandem Peptide_Expectation_Value_Log(e) or Log10(MSGF_SpecProb)
        /// </summary>
        /// <returns></returns>
        public string P_1B()
        {

            m_DBInterface.SetQuery("SELECT Scan, Min(Log10(MSGFSpecProb)) AS Peptide_Score"
                + " FROM temp_PSMs "
                + " WHERE random_id=" + m_Random_ID
                + " GROUP BY Scan "
                + " ORDER BY Scan;");

            // Track X!Tandem Peptide_Expectation_Value_Log or Log10(MSGFSpecProb)
            var Peptide_score_List = new List<double>();

            string[] fields = { "Scan", "Peptide_Score" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                double peptideScore;
                if (double.TryParse(measurementResults["Peptide_Score"], out peptideScore))
                {
                    Peptide_score_List.Add(peptideScore);
                }
            }

            var median = ComputeMedian(Peptide_score_List);

            // Round the result
            return median.ToString("0.000");
        }

        /// <summary>
        /// P_2A: Number of fully, partially, and non-tryptic peptides; total spectra count
        /// </summary>
        /// <returns>Total PSMs (spectra with a filter-passing match)</returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string P_2A()
        {
            return P_2A_Shared(phosphoPeptides: false);
        }

        private string P_2A_Shared(bool phosphoPeptides)
        {

            m_DBInterface.SetQuery("SELECT Cleavage_State, Count(*) AS Spectra "
                                + " FROM ( SELECT Scan, Max(Cleavage_State) AS Cleavage_State "
                                + "        FROM temp_PSMs "
                                + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                + "          AND random_id=" + m_Random_ID
                                + PhosphoFilter(phosphoPeptides)
                                + "        GROUP BY Scan ) StatsQ "
                                + " GROUP BY Cleavage_State;");

            var dctPSMStats = new Dictionary<int, int>();

            string[] fields = { "Cleavage_State", "Spectra" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                dctPSMStats.Add(Convert.ToInt32(measurementResults["Cleavage_State"]), Convert.ToInt32(measurementResults["Spectra"]));
            }

            // Lookup the number of fully tryptic spectra (Cleavage_State = 2)
            int spectraCount;
            if (dctPSMStats.TryGetValue(2, out spectraCount))
                return spectraCount.ToString();

            return "0";

        }

        /// <summary>
        /// P_2B:Number of unique fully, partially, and non-tryptic peptides peptides; unique peptide & charge count
        /// </summary>
        /// <returns>Unique peptide count, counting charge states separately</returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string P_2B()
        {

            const bool groupByCharge = true;
            var dctPeptideStats = SummarizePSMs(groupByCharge);

            // Lookup the number of fully tryptic peptides (Cleavage_State = 2)
            int peptideCount;
            if (dctPeptideStats.TryGetValue(2, out peptideCount))
                return peptideCount.ToString();

            return "0";
        }

        /// <summary>
        /// P_2C: Number of unique fully, partially, and non-tryptic peptides peptides; unique count regardless of charge
        /// </summary>
        /// <returns>Unique peptide count</returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string P_2C()
        {

            const bool groupByCharge = false;
            var dctPeptideStats = SummarizePSMs(groupByCharge);

            // Lookup the number of fully tryptic peptides (Cleavage_State = 2)
            int peptideCount;
            if (dctPeptideStats.TryGetValue(2, out peptideCount))
                return peptideCount.ToString();

            return "0";

        }

        /// <summary>
        /// P_3: Ratio of unique semi-tryptic / fully tryptic peptides
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string P_3()
        {

            var dctPeptideStats = SummarizePSMs(groupByCharge: false);

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

            // Round the result
            return answer.ToString("0.000000");
        }

        /// <summary>
        /// P_4A: Ratio of unique fully-tryptic / total unique peptides
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string P_4A()
        {

            var dctPeptideStats = SummarizePSMs(groupByCharge: false);

            int peptideCountFullyTryptic;

            // Lookup the number of fully tryptic peptides (Cleavage_State = 2)
            if (!dctPeptideStats.TryGetValue(2, out peptideCountFullyTryptic))
                peptideCountFullyTryptic = 0;

            // Obtain the total unique number of peptides
            var peptideCountTotal = dctPeptideStats.Values.Sum();

            // Compute the ratio of fully-tryptic / total peptides (unique counts)

            double answer = 0;
            if (peptideCountTotal > 0)
                answer = peptideCountFullyTryptic / (double)peptideCountTotal;

            // Round the result
            return answer.ToString("0.000000");
        }

        /// <summary>
        /// P_4B: Ratio of total missed cleavages (among unique peptides) / total unique peptides
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string P_4B()
        {

            m_DBInterface.SetQuery("SELECT Count(*) AS Peptides, SUM(MissedCleavages) as TotalMissedCleavages"
                                   + " FROM ( SELECT Unique_Seq_ID, Max(MissedCleavages) AS MissedCleavages "
                                   + "        FROM temp_PSMs "
                                   + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                   + "          AND random_id=" + m_Random_ID
                                   + " GROUP BY Unique_Seq_ID ) StatsQ");

            string[] fields = { "Peptides", "TotalMissedCleavages" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            m_DBInterface.ReadSingleLine(fields, out measurementResults);

            var uniquePeptides = Convert.ToInt32(measurementResults["Peptides"]);
            var totalMissedCleavages = Convert.ToInt32(measurementResults["TotalMissedCleavages"]);

            // Compute the ratio of total missed cleavagesc / total unique peptides

            double answer = 0;
            if (uniquePeptides > 0)
                answer = totalMissedCleavages / (double)uniquePeptides;

            // Round the result
            return answer.ToString("0.000000");
        }

        /// <summary>
        /// Phos_2A: Number of tryptic phosphopeptides; total spectra count
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string Phos_2A()
        {
            return P_2A_Shared(phosphoPeptides: true);
        }

        /// <summary>
        /// Phos_2C: Number of tryptic phosphopeptides; unique peptide count
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string Phos_2C()
        {

            var dctPeptideStats = SummarizePSMs(groupByCharge: false, phosphoPeptides: true);

            // Lookup the number of fully tryptic peptides (Cleavage_State = 2)
            int peptideCount;
            if (dctPeptideStats.TryGetValue(2, out peptideCount))
                return peptideCount.ToString();

            return "0";

        }

        /// <summary>
        /// Keratin_2A: Number of keratin peptides; total spectra count
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string Keratin_2A()
        {
            m_DBInterface.SetQuery("SELECT Count(*) AS Spectra "
                                   + " FROM ( SELECT Scan, Max(Cleavage_State) AS Cleavage_State "
                                   + "        FROM temp_PSMs "
                                   + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                   + "          AND random_id=" + m_Random_ID
                                   + "          AND Keratinpeptide = 1"
                                   + "        GROUP BY Scan ) StatsQ "
                                   + " WHERE Cleavage_State >= 0");

            string[] fields = { "Spectra" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            m_DBInterface.ReadSingleLine(fields, out measurementResults);

            var keratinCount = Convert.ToInt32(measurementResults["Spectra"]);

            return keratinCount.ToString("0");
        }

        /// <summary>
        /// Keratin_2C: Number of keratin peptides; unique peptide count
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string Keratin_2C()
        {

            m_DBInterface.SetQuery("SELECT Count(*) AS Peptides "
                                   + " FROM ( SELECT Unique_Seq_ID, Max(Cleavage_State) AS Cleavage_State "
                                   + "        FROM temp_PSMs "
                                   + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                   + "          AND random_id=" + m_Random_ID
                                   + "          AND Keratinpeptide = 1"
                                   + "        GROUP BY Unique_Seq_ID ) StatsQ "
                                   + " WHERE Cleavage_State >= 0");

            string[] fields = { "Peptides" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            m_DBInterface.ReadSingleLine(fields, out measurementResults);

            var keratinCount = Convert.ToInt32(measurementResults["Peptides"]);

            return keratinCount.ToString("0");

        }

        /// <summary>
        /// Trypsin_2A: Number of peptides from trypsin; total spectra count
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string Trypsin_2A()
        {
            m_DBInterface.SetQuery("SELECT Count(*) AS Spectra "
                                   + " FROM ( SELECT Scan, Max(Cleavage_State) AS Cleavage_State "
                                   + "        FROM temp_PSMs "
                                   + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                   + "          AND random_id=" + m_Random_ID
                                   + "          AND Trypsinpeptide = 1"
                                   + "        GROUP BY Scan ) StatsQ "
                                   + " WHERE Cleavage_State >= 0");

            string[] fields = { "Spectra" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            m_DBInterface.ReadSingleLine(fields, out measurementResults);

            var trypsinCount = Convert.ToInt32(measurementResults["Spectra"]);

            return trypsinCount.ToString("0");
        }

        /// <summary>
        /// Trypsin_2C: Number of peptides from trypsin; unique peptide count
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string Trypsin_2C()
        {

            m_DBInterface.SetQuery("SELECT Count(*) AS Peptides "
                                   + " FROM ( SELECT Unique_Seq_ID, Max(Cleavage_State) AS Cleavage_State "
                                   + "        FROM temp_PSMs "
                                   + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                   + "          AND random_id=" + m_Random_ID
                                   + "          AND Trypsinpeptide = 1"
                                   + "        GROUP BY Unique_Seq_ID ) StatsQ "
                                   + " WHERE Cleavage_State >= 0");

            string[] fields = { "Peptides" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            m_DBInterface.ReadSingleLine(fields, out measurementResults);

            var trypsinCount = Convert.ToInt32(measurementResults["Peptides"]);

            return trypsinCount.ToString("0");

        }

        /// <summary>
        /// MS2_RepIon_All: Number of Filter-passing PSMs for which all of the Reporter Ions were seen
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_RepIon_All()
        {
            var psmCount = MS2_RepIon_Lookup(0);
            return psmCount.ToString("0");
        }

        /// <summary>
        /// MS2_RepIon_1Missing: Number of Filter-passing PSMs for which one Reporter Ion was not observed
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_RepIon_1Missing()
        {
            var psmCount = MS2_RepIon_Lookup(1);
            return psmCount.ToString("0");
        }

        /// <summary>
        /// MS2_RepIon_2Missing: Number of Filter-passing PSMs for which two Reporter Ions were not observed
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_RepIon_2Missing()
        {
            var psmCount = MS2_RepIon_Lookup(2);
            return psmCount.ToString("0");
        }

        /// <summary>
        /// MS2_RepIon_3Missing: Number of Filter-passing PSMs for which three Reporter Ions were not observed
        /// </summary>
        /// <returns></returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_RepIon_3Missing()
        {
            var psmCount = MS2_RepIon_Lookup(3);
            return psmCount.ToString("0");
        }

        private int MS2_RepIon_Lookup(int numMissingReporterIons)
        {
            // Determine the reporter ion mode by looking for non-null values
            // a) Make a list of all of the "Ion_" columns in table temp_reporterions
            // b) Find the columns with at least one non-null value
            //    - Compute the average of all non-zero values
            //    - Define MinimumThreshold = GlobalAverage / 1000
            // c) Now query those columns to determine the number of columns with data >= MinimumThreshold

            var ionColumns = DetermineReporterIonColumns();
            if (ionColumns.Count == 0)
                return 0;

            // Compute the average of all of the non-zero-values
            var threshold = ComputeReporterIonNoiseThreshold(ionColumns);

            // Determine the number of PSMs where all of the reporter ions were seen
            var reporterIonObsCount = ionColumns.Count - numMissingReporterIons;
            var psmCount = CountPSMsWithNumObservedReporterIons(ionColumns, threshold, reporterIonObsCount);

            return psmCount;
        }

        private double ComputeReporterIonNoiseThreshold(List<string> ionColumns)
        {
            var cachedThreshold = GetStoredValue(eCachedResult.ReporterIonNoiseThreshold, -1);
            if (cachedThreshold > -1)
                return cachedThreshold;

            var sbSql = new StringBuilder();

            foreach (var column in ionColumns)
            {
                if (sbSql.Length == 0)
                    sbSql.Append("SELECT ");
                else
                    sbSql.Append(", ");

                sbSql.Append("SUM (IfNull([" + column + "], 0)) AS [" + column + "_Sum], ");
                sbSql.Append("SUM (CASE WHEN IfNull([" + column + "], 0) = 0 Then 0 Else 1 End) AS [" + column + "_Count]");
            }

            sbSql.Append(" FROM temp_reporterions");
            sbSql.Append(" WHERE random_id=" + m_Random_ID);

            m_DBInterface.SetQuery(sbSql.ToString());

            var fields = (from item in ionColumns select item + "_Sum").ToList();
            fields.AddRange((from item in ionColumns select item + "_Count").ToList());

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            m_DBInterface.ReadSingleLine(fields.ToArray(), out measurementResults);

            var overallSum = 0.0;
            var overallCount = 0;

            foreach (var column in ionColumns)
            {
                var columnSum = Convert.ToDouble(measurementResults[column + "_Sum"]);
                var columnCount = Convert.ToInt32(measurementResults[column + "_Count"]);

                overallSum += columnSum;
                overallCount += columnCount;
            }

            if (overallCount > 0)
            {
                var nonZeroAverage = overallSum / (double)overallCount;
                var noiseThreshold = nonZeroAverage / 1000.0;

                AddUpdateResultsStorage(eCachedResult.ReporterIonNoiseThreshold, noiseThreshold);
                return noiseThreshold;
            }

            AddUpdateResultsStorage(eCachedResult.ReporterIonNoiseThreshold, 0);
            return 0;
        }

        private int CountPSMsWithNumObservedReporterIons(List<string> ionColumns, double threshold, int reporterIonObsCount)
        {
            var sbSql = new StringBuilder();
            var thresholdText = threshold.ToString("0.000");

            foreach (var column in ionColumns)
            {
                if (sbSql.Length == 0)
                    sbSql.Append("SELECT ");
                else
                    sbSql.Append(" + ");

                sbSql.Append("CASE WHEN [" + column + "] > " + thresholdText + " Then 1 Else 0 End");
            }

            sbSql.Append(" AS ReporterIonCount");
            sbSql.Append(" FROM temp_reporterions INNER JOIN ");
            sbSql.Append("   temp_psms ON temp_reporterions.ScanNumber = temp_PSMs.scan AND " +
                         "   temp_reporterions.random_id = temp_PSMs.random_id ");
            sbSql.Append(" WHERE temp_PSMs.random_id=" + m_Random_ID);
            sbSql.Append("   AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);

            var sql = " SELECT COUNT(*) AS PSMs" +
                      " FROM (" + sbSql + ") AS FilterQ" +
                      " WHERE ReporterIonCount = " + reporterIonObsCount;

            m_DBInterface.SetQuery(sql);

            string[] fields = { "PSMs" };

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            m_DBInterface.ReadSingleLine(fields.ToArray(), out measurementResults);

            var psmCount = Convert.ToInt32(measurementResults["PSMs"]);
            return psmCount;

        }

        private List<string> DetermineReporterIonColumns()
        {
            if (mReporterIonColumns != null && mReporterIonColumns.Count > 0)
                return mReporterIonColumns;

            var columnList = m_DBInterface.GetTableColumns("temp_reporterions");
            var ionColumns = columnList.Where(column => column.StartsWith("Ion_")).ToList();

            var sbSql = new StringBuilder();

            foreach (var column in ionColumns)
            {
                if (sbSql.Length == 0)
                    sbSql.Append("SELECT ");
                else
                    sbSql.Append(", ");

                sbSql.Append("SUM (CASE WHEN [" + column + "] IS NULL Then 0 Else 1 End) AS [" + column + "]");
            }

            sbSql.Append(" FROM temp_reporterions");
            sbSql.Append(" WHERE random_id=" + m_Random_ID);

            m_DBInterface.SetQuery(sbSql.ToString());

            var fields = ionColumns.ToArray();

            m_DBInterface.initReader();

            Dictionary<string, string> measurementResults;
            m_DBInterface.ReadSingleLine(fields, out measurementResults);

            var ionColumnsToUse = new List<string>();
            foreach (var column in ionColumns)
            {
                if (!string.IsNullOrWhiteSpace(measurementResults[column]))
                {
                    var nonNullCount = Convert.ToInt32(measurementResults[column]);
                    if (nonNullCount > 0)
                        ionColumnsToUse.Add(column);
                }
            }

            mReporterIonColumns = ionColumnsToUse;

            return mReporterIonColumns;
        }

        private string PhosphoFilter(bool phosphoPeptides)
        {
            if (phosphoPeptides)
                return " AND Phosphopeptide = 1";

            return "";
        }

        /// <summary>
        /// Counts the number of fully, partially, and non-tryptic peptides
        /// </summary>
        /// <param name="groupByCharge">If true, then counts charges separately</param>
        /// <returns>Unique peptide count</returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        private Dictionary<int, int> SummarizePSMs(bool groupByCharge)
        {
            return SummarizePSMs(groupByCharge, phosphoPeptides: false);
        }

        /// <summary>
        /// Counts the number of unique fully, partially, and non-tryptic peptides
        /// </summary>
        /// <param name="groupByCharge">If true, then counts charges separately</param>
        /// <param name="phosphoPeptides">If true, then only uses phosphopeptides</param>
        /// <returns>Unique peptide count</returns>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        private Dictionary<int, int> SummarizePSMs(bool groupByCharge, bool phosphoPeptides)
        {
            var chargeSql = string.Empty;

            if (groupByCharge)
            {
                chargeSql = ", Charge ";
            }

            m_DBInterface.SetQuery("SELECT Cleavage_State, Count(*) AS Peptides "
                                + " FROM ( SELECT Unique_Seq_ID" + chargeSql + ", Max(Cleavage_State) AS Cleavage_State "
                                + "        FROM temp_PSMs "
                                + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                + "          AND random_id=" + m_Random_ID
                                + PhosphoFilter(phosphoPeptides)
                                + " GROUP BY Unique_Seq_ID" + chargeSql + " ) StatsQ "
                                + " GROUP BY Cleavage_State;");

            var dctPeptideStats = new Dictionary<int, int>();

            string[] fields = { "Cleavage_State", "Peptides" };

            m_DBInterface.initReader();


            Dictionary<string, string> measurementResults;
            while (m_DBInterface.ReadLines(fields, out measurementResults) && measurementResults.Count > 0)
            {
                dctPeptideStats.Add(Convert.ToInt32(measurementResults["Cleavage_State"]), Convert.ToInt32(measurementResults["Peptides"]));
            }

            return dctPeptideStats;

        }

    }
}
