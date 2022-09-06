using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using PHRPReader;

// ReSharper disable UnusedMember.Global

namespace SMAQC
{
    /// <summary>
    /// This class defines the logic for computing each QC metric
    /// </summary>
    /// <remarks>
    /// The methods in this file are used by the MeasurementFactory class
    /// They are loaded via reflection, specifically
    /// mMeasurement.GetType().GetMethod(methodName)
    /// </remarks>
    internal class Measurement
    {
        // Ignore Spelling: frag, da, hyperscore, phos

        private struct PeptideEntry
        {
            public int Scan;
            public int Charge;
            public string PeptideSequence;
            public double Score;
        }

        private enum CachedResultTypes
        {
            ScanFirstFilterPassingPeptide = 0,
            C2A_RegionScanStart = 1,
            C2A_RegionScanEnd = 2,
            C2A_TimeMinutes = 3,
            ReporterIonNoiseThreshold = 4
        }

        public const double MSGF_SPECPROB_THRESHOLD = 1e-12;

        /// <summary>
        /// DB interface object
        /// </summary>
        private readonly DBWrapper mDBInterface;

        /// <summary>
        /// Random ID for temp tables
        /// </summary>
        private readonly int mRandomId;

        /// <summary>
        /// Some measurements have data required by others ... will be stored here
        /// </summary>
        private readonly Dictionary<CachedResultTypes, double> mResultsStorage = new();

        private bool mMedianPeakWidthDataCached;

        /// <summary>
        /// Best Scan Number for each peptide
        /// </summary>
        /// <remarks>Used when computing median peak widths</remarks>
        private readonly List<int> PsmBestScan = new();

        /// <summary>
        /// Full width at half max, in scans; keys are FragScanNumbers
        /// </summary>
        /// <remarks>Used when computing median peak widths</remarks>
        private readonly Dictionary<int, double> PsmFWHMinScans = new();

        /// <summary>
        /// Optimal peak apex scan number; keys are FragScanNumbers
        /// </summary>
        /// <remarks>Used when computing median peak widths</remarks>
        private readonly Dictionary<int, int> PsmOptimalPeakApexScanNumber = new();

        /// <summary>
        /// Scan time for given scan number; keys are scan numbers
        /// </summary>
        /// <remarks>Used when computing median peak widths</remarks>
        private readonly Dictionary<int, double> PsmScanTime = new();

        /// <summary>
        /// Cached data for PSM stats by charge
        /// </summary>
        /// <remarks>Number of filter-passing PSMs for each charge state</remarks>
        private readonly Dictionary<int, int> mCachedPSMStatsByCharge = new();

        /// <summary>
        /// Cached data for DS_1
        /// </summary>
        /// <remarks>Keys are the number of spectra that a peptide was observed in (passing filters) and values are the number of peptides identified by Key spectra</remarks>
        private readonly Dictionary<int, int> mPeptideSamplingStats = new();

        /// <summary>
        /// Cached data for DS_3: Stores MS1 max / MS1 sampled abundance
        /// </summary>
        private readonly List<double> mCachedDS3 = new();

        /// <summary>
        /// Cached data for DS_3: Stores MS1 max / MS1 sampled abundance for ratios in the bottom 50%
        /// </summary>
        private readonly List<double> mCachedDS3_Bottom50pct = new();

        /// <summary>
        /// Cached data for MS1_2
        /// </summary>
        private readonly List<double> mCachedBasePeakSignalToNoiseRatio = new();
        private readonly List<double> mCachedTotalIonIntensity = new();

        /// <summary>
        /// Cached data for MS1_3
        /// </summary>
        private double mCachedPeakMaxIntensity_5thPercentile;
        private double mCachedPeakMaxIntensity_95thPercentile;

        /// <summary>
        /// PeakMaxIntensity for each filter-passing PSM
        /// </summary>
        private readonly List<double> mCachedMS1_3 = new();

        /// <summary>
        /// Cached data for MS1_5: Delta mass
        /// </summary>
        private readonly List<double> mCachedDelM = new();

        /// <summary>
        /// Cached data for MS1_5: Delta mass, in ppm
        /// </summary>
        // ReSharper disable once IdentifierTypo
        private readonly List<double> mCachedDelMppm = new();

        private bool mMS2QuartileCountsCached;

        /// <summary>
        /// Cached data for MS4: scan counts
        /// </summary>
        /// <remarks>
        /// Keys are quartile (1,2,3,4); values are the number of MS/MS scans in the quartile
        /// </remarks>
        private readonly Dictionary<int, int> mMS2QuartileScanCounts = new();

        /// <summary>
        /// Cached data for MS4: confident PSMs
        /// </summary>
        /// <remarks>
        /// Keys are quartile (1,2,3,4); values are the number of confidently identified MS/MS scans in the quartile (MSGFSpecProb less than 1E-12)
        /// </remarks>
        private readonly Dictionary<int, int> mMS2QuartileConfidentPSMs = new();

        /// <summary>
        /// Cached data for the ReporterIon metrics
        /// </summary>
        private readonly List<string> mReporterIonColumns = new();

        private bool mIgnoreReporterIons;

        private readonly PeptideMassCalculator mPeptideMassCalculator;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="randomId"></param>
        /// <param name="DBInterface"></param>
        public Measurement(int randomId, DBWrapper DBInterface)
        {
            mRandomId = randomId;
            mDBInterface = DBInterface;
            mPeptideMassCalculator = new PeptideMassCalculator();
        }

        /// <summary>
        /// Add (or update) entryName in mResultsStorage
        /// </summary>
        /// <param name="entryType"></param>
        /// <param name="value"></param>
        private void AddUpdateResultsStorage(CachedResultTypes entryType, double value)
        {
            // Add/update the dictionary
            mResultsStorage[entryType] = value;
        }

        /// <summary>
        /// Clear cached data
        /// </summary>
        public void Reset()
        {
            mResultsStorage.Clear();

            mMedianPeakWidthDataCached = false;
            PsmBestScan.Clear();
            PsmFWHMinScans.Clear();
            PsmOptimalPeakApexScanNumber.Clear();
            PsmScanTime.Clear();

            mCachedPSMStatsByCharge.Clear();

            mPeptideSamplingStats.Clear();

            mCachedDS3.Clear();
            mCachedDS3_Bottom50pct.Clear();

            mCachedBasePeakSignalToNoiseRatio.Clear();
            mCachedTotalIonIntensity.Clear();

            mCachedPeakMaxIntensity_5thPercentile = 0;
            mCachedPeakMaxIntensity_95thPercentile = 0;
            mCachedMS1_3.Clear();

            mCachedDelM.Clear();
            mCachedDelMppm.Clear();

            mMS2QuartileCountsCached = false;
            mMS2QuartileScanCounts.Clear();
            mMS2QuartileConfidentPSMs.Clear();

            mReporterIonColumns.Clear();
            mIgnoreReporterIons = false;
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
                var pos = values.Count / 2;
                return values[pos];
            }
            else
            {
                // Even
                var pos = values.Count / 2 - 1;
                return (values[pos] + values[pos + 1]) / 2;
            }
        }

        private double GetStoredValue(CachedResultTypes entryType, double valueIfMissing)
        {
            if (mResultsStorage.TryGetValue(entryType, out var value))
                return value;

            return valueIfMissing;
        }

        private int GetStoredValueInt(CachedResultTypes entryType, int valueIfMissing)
        {
            var value = GetStoredValue(entryType, valueIfMissing);
            return (int)value;
        }

        private bool PeptideScorePassesFilter(double peptideScore)
        {
            return peptideScore <= MSGF_SPECPROB_THRESHOLD;
        }

        /// <summary>
        /// C-1A: Fraction of peptides identified more than 4 minutes earlier than the chromatographic peak apex
        /// </summary>
        public string C_1A()
        {
            return C_1_Shared(countTailingPeptides: false);
        }

        /// <summary>
        /// C-1B: Fraction of peptides identified more than 4 minutes later than the chromatographic peak apex
        /// </summary>
        public string C_1B()
        {
            return C_1_Shared(countTailingPeptides: true);
        }

        /// <summary>
        /// Counts the number of peptides identified more than 4 minutes earlier or more than 4 minutes later than the chromatographic peak apex
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        /// <param name="countTailingPeptides">False means to count early eluting peptides; True means to count late-eluting peptides</param>
        private string C_1_Shared(bool countTailingPeptides)
        {
            mDBInterface.SetQuery("SELECT temp_PSMs.Scan, t1.FragScanNumber, t1.OptimalPeakApexScanNumber,"
                                    + "    temp_ScanStats.ScanTime as ScanTime1, t2.ScanTime as ScanTimePeakApex "
                                    + " FROM temp_PSMs, temp_ScanStats, temp_SICStats as t1 "
                                    + "      INNER JOIN temp_ScanStats as t2 on t1.OptimalPeakApexScanNumber=t2.ScanNumber "
                                    + " WHERE temp_PSMs.Scan = t1.FragScanNumber "
                                    + "  AND temp_PSMs.Scan = temp_ScanStats.ScanNumber "
                                    + "  AND temp_PSMs.random_id=" + mRandomId
                                    + "  AND temp_ScanStats.random_id=" + mRandomId
                                    + "  AND t1.random_id=" + mRandomId
                                    + "  AND t2.random_id=" + mRandomId
                                    + "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                    + " ORDER BY Scan;");

            var psmCountLateOrEarly = 0;
            var psmCountTotal = 0;

            string[] columnNames = { "Scan", "FragScanNumber", "OptimalPeakApexScanNumber", "ScanTime1", "ScanTimePeakApex" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                // Calculate difference
                double temp_difference;
                if (countTailingPeptides)
                    temp_difference = double.Parse(measurementResults["ScanTime1"]) - double.Parse(measurementResults["ScanTimePeakApex"]);
                else
                    temp_difference = double.Parse(measurementResults["ScanTimePeakApex"]) - double.Parse(measurementResults["ScanTime1"]);

                // If difference is greater than 4 minutes, increment the counter
                if (temp_difference >= 4.00)
                {
                    psmCountLateOrEarly++;
                }

                psmCountTotal++;
            }

            // Calculate solution
            if (psmCountTotal > 0)
            {
                var answer = psmCountLateOrEarly / (double)psmCountTotal;
                return PRISM.StringUtilities.DblToString(answer, 6, 0.0000001);
            }

            return string.Empty;
        }

        /// <summary>
        /// C-2A: Time period over which 50% of peptides are identified
        /// We also cache various scan numbers associated with filter-passing peptides
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string C_2A()
        {
            mDBInterface.SetQuery("SELECT temp_PSMs.Scan, t1.FragScanNumber as ScanNumber,"
                                    + "    temp_ScanStats.ScanTime as ScanTime1 "
                                    + " FROM temp_PSMs, temp_ScanStats, temp_SICStats as t1 "
                                    + " WHERE temp_PSMs.Scan = t1.FragScanNumber "
                                    + "  AND temp_PSMs.Scan = temp_ScanStats.ScanNumber "
                                    + "  AND temp_PSMs.random_id=" + mRandomId
                                    + "  AND temp_ScanStats.random_id=" + mRandomId
                                    + "  AND t1.random_id=" + mRandomId + " "
                                    + "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                    + " ORDER BY Scan;");

            // This list stores scan numbers and elution times for filter-passing peptides; duplicate scans are not allowed
            var filterPassingPeptides = new SortedList<int, double>();

            string[] columnNames = { "Scan", "ScanNumber", "ScanTime1" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                // Filter-passing peptide; Append to the dictionary
                if (int.TryParse(measurementResults["ScanNumber"], out var scanNumber))
                {
                    if (double.TryParse(measurementResults["ScanTime1"], out var scanTime))
                    {
                        if (!filterPassingPeptides.ContainsKey(scanNumber))
                        {
                            filterPassingPeptides.Add(scanNumber, scanTime);
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

            if (filterPassingPeptides.Count > 0)
            {
                // Determine the scan numbers at which the 25th and 75th percentiles are located
                index25th = (int)(filterPassingPeptides.Count * 0.25);
                index75th = (int)(filterPassingPeptides.Count * 0.75);

                if (index25th >= filterPassingPeptides.Count)
                    index25th = filterPassingPeptides.Count - 1;

                if (index75th >= filterPassingPeptides.Count)
                    index75th = filterPassingPeptides.Count - 1;

                if (index75th < index25th)
                    index75th = index25th;
            }

            if (index25th >= 0 && index25th < filterPassingPeptides.Count && index75th < filterPassingPeptides.Count)
            {
                C2AScanStart = filterPassingPeptides.Keys[index25th];
                C2AScanEnd = filterPassingPeptides.Keys[index75th];

                C2AScanTimeStart = filterPassingPeptides.Values[index25th];
                C2AScanTimeEnd = filterPassingPeptides.Values[index75th];
            }

            if (filterPassingPeptides.Count > 0)
            {
                // Add to global list for use with MS_2A/B
                // ScanFirstFilterPassingPeptide is the scan number of the first filter-passing peptide
                AddUpdateResultsStorage(CachedResultTypes.ScanFirstFilterPassingPeptide, filterPassingPeptides.Keys.Min());
            }

            // Cache the scan numbers at the start and end of the interquartile region
            AddUpdateResultsStorage(CachedResultTypes.C2A_RegionScanStart, C2AScanStart);
            AddUpdateResultsStorage(CachedResultTypes.C2A_RegionScanEnd, C2AScanEnd);

            var timeMinutes = C2AScanTimeEnd - C2AScanTimeStart;

            AddUpdateResultsStorage(CachedResultTypes.C2A_TimeMinutes, timeMinutes);

            return PRISM.StringUtilities.DblToString(timeMinutes, 4, 0.00001);
        }

        /// <summary>
        /// C-2B: Rate of peptide identification during C-2A
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string C_2B()
        {
            mDBInterface.SetQuery("SELECT temp_PSMs.Scan, t1.FragScanNumber as ScanNumber,"
                                    + "    temp_ScanStats.ScanTime as ScanTime1 "
                                    + " FROM temp_PSMs, temp_ScanStats, temp_SICStats as t1 "
                                    + " WHERE temp_PSMs.Scan = t1.FragScanNumber "
                                    + "  AND temp_PSMs.Scan = temp_ScanStats.ScanNumber "
                                    + "  AND temp_PSMs.random_id=" + mRandomId
                                    + "  AND temp_ScanStats.random_id=" + mRandomId
                                    + "  AND t1.random_id=" + mRandomId
                                    + "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                    + " ORDER BY Scan;");

            // This list keeps track of the scan numbers already processed so that we can avoid double-counting a scan number
            var scansWithFilterPassingIDs = new SortedSet<int>();

            var timeMinutesC2A = GetStoredValue(CachedResultTypes.C2A_TimeMinutes, 0);
            var scanStartC2A = GetStoredValueInt(CachedResultTypes.C2A_RegionScanStart, 0);
            var scanEndC2A = GetStoredValueInt(CachedResultTypes.C2A_RegionScanEnd, 0);

            string[] columnNames = { "Scan", "ScanNumber", "ScanTime1" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                // Filter-passing peptide; Append to the dictionary
                if (int.TryParse(measurementResults["ScanNumber"], out var scanNumber))
                {
                    if (scanNumber >= scanStartC2A && scanNumber <= scanEndC2A && !scansWithFilterPassingIDs.Contains(scanNumber))
                    {
                        scansWithFilterPassingIDs.Add(scanNumber);
                    }
                }
            }

            if (timeMinutesC2A > 0)
            {
                var answer = scansWithFilterPassingIDs.Count / timeMinutesC2A;

                // Round the result
                return PRISM.StringUtilities.DblToString(answer, 4, 0.00001);
            }

            return string.Empty;
        }

        /// <summary>
        /// C-3A: Median peak width for all peptides
        /// </summary>
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
        public string C_4C()
        {
            const double startScanRelative = 0.45;
            const double endScanRelative = 0.55;

            // Compute the result
            return ComputeMedianPeakWidth(startScanRelative, endScanRelative);
        }

        private void Cache_MedianPeakWidth_Data()
        {
            var psms = new List<PeptideEntry>();					            		// Cached, filter-passing peptide-spectrum matches

            mDBInterface.SetQuery("SELECT Scan, Charge, Peptide_Sequence, MSGFSpecProb AS Peptide_Score"
                                + " FROM temp_PSMs "
                                + " WHERE random_id=" + mRandomId);

            string[] columnNames = { "Scan", "Charge", "Peptide_Sequence", "Peptide_Score" };

            mDBInterface.InitReader();

            // Read and cache the data since we need to trim off the prefix and suffix letters from the peptide sequence

            Dictionary<string, string> measurementResults;
            while (mDBInterface.ReadNextRow(columnNames, out measurementResults) && measurementResults.Count > 0)
            {
                PeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(measurementResults["Peptide_Sequence"], out var peptideResidues, out _, out _);

                var currentPeptide = new PeptideEntry();

                if (int.TryParse(measurementResults["Scan"], out currentPeptide.Scan))
                {
                    if (int.TryParse(measurementResults["Charge"], out currentPeptide.Charge))
                    {
                        if (double.TryParse(measurementResults["Peptide_Score"], out currentPeptide.Score))
                        {
                            currentPeptide.PeptideSequence = peptideResidues;
                            psms.Add(currentPeptide);
                        }
                    }
                }
            }

            // Sort by peptide sequence, then charge, then scan number
            var sortedPSMs = from item in psms
                             orderby item.PeptideSequence, item.Charge, item.Scan
                             select item;

            var previousPeptide = new PeptideEntry()
            {
                PeptideSequence = string.Empty
            };

            // Parse the sorted data
            foreach (var psm in sortedPSMs)
            {
                double bestScore;

                // Check whether previous peptide sequences are equivalent and have the same charge
                if (previousPeptide.PeptideSequence.Equals(psm.PeptideSequence) && previousPeptide.Charge == psm.Charge)
                {
                    // Use the minimum (either previous best EValue or current peptide score)
                    bestScore = Math.Min(previousPeptide.Score, psm.Score);
                }
                else
                {
                    bestScore = psm.Score;
                }

                // Keep track of the scan number for the best score
                if (Math.Abs(bestScore - psm.Score) < float.Epsilon)
                {
                    PsmBestScan.Add(psm.Scan);
                }

                // Update previous values for next loop
                previousPeptide.Scan = psm.Scan;
                previousPeptide.Charge = psm.Charge;
                previousPeptide.PeptideSequence = string.Copy(psm.PeptideSequence);
                previousPeptide.Score = bestScore;
            }

            // Sort the data
            PsmBestScan.Sort();

            mDBInterface.SetQuery("SELECT FragScanNumber, FWHMInScans, OptimalPeakApexScanNumber FROM temp_SICStats WHERE temp_SICStats.random_id=" + mRandomId);

            string[] sicStatsColumnNames = { "FragScanNumber", "FWHMInScans", "OptimalPeakApexScanNumber" };

            mDBInterface.InitReader();

            // Fetch columns d-f

            while (mDBInterface.ReadNextRow(sicStatsColumnNames, out measurementResults) && measurementResults.Count > 0)
            {
                var fragScanNumber = int.Parse(measurementResults["FragScanNumber"]);

                PsmFWHMinScans.Add(fragScanNumber, double.Parse(measurementResults["FWHMInScans"]));
                PsmOptimalPeakApexScanNumber.Add(fragScanNumber, int.Parse(measurementResults["OptimalPeakApexScanNumber"]));
            }

            mDBInterface.SetQuery("SELECT temp_ScanStats.ScanNumber, temp_ScanStats.ScanTime FROM temp_ScanStats WHERE temp_ScanStats.random_id=" + mRandomId);

            string[] scanStatsColumnNames = { "ScanNumber", "ScanTime" };

            mDBInterface.InitReader();

            // Fetch columns h-i
            while (mDBInterface.ReadNextRow(scanStatsColumnNames, out measurementResults) && measurementResults.Count > 0)
            {
                PsmScanTime.Add(int.Parse(measurementResults["ScanNumber"]), double.Parse(measurementResults["ScanTime"]));
            }

            mMedianPeakWidthDataCached = true;
        }

        private string ComputeMedianPeakWidth(double startScanRelative, double endScanRelative)
        {
            var result = new List<double>();

            if (!mMedianPeakWidthDataCached)
            {
                Cache_MedianPeakWidth_Data();
            }

            // Loop through the list of best scans
            for (var i = 0; i < PsmBestScan.Count; i++)
            {
                var bestScan = PsmBestScan[i];
                var scanCenter = PsmOptimalPeakApexScanNumber[bestScan];
                var halfWidth = (int)Math.Ceiling(PsmFWHMinScans[bestScan] / 2);

                // Find index + optimal peak apex scan +- FWHM for each result (columns: m,o)
                var startScan = scanCenter - halfWidth;
                var endScan = scanCenter + halfWidth;

                // Find other columns [n,p, q,r,t]

                if (PsmScanTime.TryGetValue(startScan, out var startTime))
                {
                    if (PsmScanTime.TryGetValue(endScan, out var endTime))
                    {
                        var peakWidthSeconds = (endTime - startTime) * 60;
                        var percent = (i + 1) / (double)PsmBestScan.Count;

                        // check for valid range data then add to our results
                        if (percent >= startScanRelative && percent <= endScanRelative)
                        {
                            // We are within our valid range ... so add the peak width to the list
                            result.Add(peakWidthSeconds);
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
                return PRISM.StringUtilities.DblToString(median, 2, 0.0001);
            }

            return resultText;
        }

        /// <summary>
        /// DS-1A: Count of peptides with one spectrum / count of peptides with two spectra
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string DS_1A()
        {
            double result = 0;

            if (mPeptideSamplingStats.Count == 0)
            {
                Cache_DS_1_Data();
            }

            // Calculate the value; return 0 if number of peptides identified with 2 spectra is 0

            if (mPeptideSamplingStats.TryGetValue(2, out var numPeptidesWithTwoSpectra))
            {
                if (numPeptidesWithTwoSpectra > 0)
                {
                    if (!mPeptideSamplingStats.TryGetValue(1, out var numPeptidesWithOneSpectrum))
                        numPeptidesWithOneSpectrum = 0;

                    result = numPeptidesWithOneSpectrum / (double)numPeptidesWithTwoSpectra;
                }
            }

            return PRISM.StringUtilities.DblToString(result, 3, 0.00001);
        }

        /// <summary>
        /// DS-1B: Count of peptides with two spectra / count of peptides with three spectra
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string DS_1B()
        {
            // Keys are the number of spectra that a peptide was observed in (passing filters) and values are the number of peptides identified by Key spectra</param>

            double result = 0;

            if (mPeptideSamplingStats.Count == 0)
            {
                Cache_DS_1_Data();
            }

            // Calculate the value; return 0 if number of peptides identified with 3 spectra is 0

            if (mPeptideSamplingStats.TryGetValue(3, out var numPeptidesWithThreeSpectra))
            {
                if (numPeptidesWithThreeSpectra > 0)
                {
                    if (!mPeptideSamplingStats.TryGetValue(2, out var numPeptidesWithTwoSpectra))
                        numPeptidesWithTwoSpectra = 0;

                    result = numPeptidesWithTwoSpectra / (double)numPeptidesWithThreeSpectra;
                }
            }

            return PRISM.StringUtilities.DblToString(result, 3, 0.00001);
        }

        /// <summary>
        /// Computes stats on the number of spectra by which each peptide was identified
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        private void Cache_DS_1_Data()
        {
            mDBInterface.SetQuery("SELECT Spectra, Count(*) AS Peptides "
                                + " FROM ( SELECT Unique_Seq_ID, COUNT(*) AS Spectra "
                                + "        FROM ( SELECT Unique_Seq_ID, Scan "
                                + "               FROM temp_PSMs "
                                + "               WHERE random_id=" + mRandomId
                                + "                 AND MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                + "               GROUP BY Unique_Seq_ID, Scan ) DistinctQ "
                                + "        GROUP BY Unique_Seq_ID ) CountQ "
                                + " GROUP BY Spectra;");

            mPeptideSamplingStats.Clear();

            string[] columnNames = { "Spectra", "Peptides" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                var spectra = int.Parse(measurementResults["Spectra"]);
                var peptides = int.Parse(measurementResults["Peptides"]);

                mPeptideSamplingStats.Add(spectra, peptides);
            }
        }

        /// <summary>
        /// DS-2A: Number of MS1 scans taken over middle 50% of separation
        /// </summary>
        public string DS_2A()
        {
            const int msLevel = 1;
            return DS_2_Shared(msLevel).ToString();
        }

        /// <summary>
        /// DS-2B: Number of MS2 scans taken over middle 50% of separation
        /// </summary>
        public string DS_2B()
        {
            const int msLevel = 2;
            return DS_2_Shared(msLevel).ToString();
        }

        private int DS_2_Shared(int msLevel)
        {
            var scanStartC2A = GetStoredValueInt(CachedResultTypes.C2A_RegionScanStart, 0);
            var scanEndC2A = GetStoredValueInt(CachedResultTypes.C2A_RegionScanEnd, 0);

            mDBInterface.SetQuery("SELECT COUNT(*) AS ScanCount "
                + " FROM temp_ScanStats "
                + " WHERE temp_ScanStats.random_id=" + mRandomId
                + "   AND ScanType = " + msLevel
                + "   AND ScanNumber >= " + scanStartC2A
                + "   AND ScanNumber <= " + scanEndC2A);

            string[] columnNames = { "ScanCount" };

            mDBInterface.InitReader();

            mDBInterface.ReadSingleLine(columnNames, out var measurementResults);

            var scanCount = int.Parse(measurementResults["ScanCount"]);

            return scanCount;
        }

        /// <summary>
        /// IS-2: Median precursor m/z for all peptides
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string IS_2()
        {
            mDBInterface.SetQuery("SELECT Peptide_MH, Charge "
                                + " FROM temp_PSMs "
                                + " WHERE random_id=" + mRandomId
                                + "   AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);

            var mzList = new SortedSet<double>();

            string[] columnNames = { "Peptide_MH", "Charge" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                var mz = mPeptideMassCalculator.ConvoluteMass(double.Parse(measurementResults["Peptide_MH"]), 1, int.Parse(measurementResults["Charge"]));

                if (!mzList.Contains(mz))
                    mzList.Add(mz);
            }

            // Compute the median
            var median = ComputeMedian(mzList.ToList());

            // Round the result
            return PRISM.StringUtilities.DblToString(median, 4, 0.00001);
        }

        /// <summary>
        /// IS-3A: Count of 1+ peptides / count of 2+ peptides
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string IS_3A()
        {
            double result = 0;

            if (mCachedPSMStatsByCharge.Count == 0)
            {
                Cache_IS3_Data();
            }

            if (mCachedPSMStatsByCharge.TryGetValue(1, out var psmCountCharge1))
            {
                if (mCachedPSMStatsByCharge.TryGetValue(2, out var psmCountCharge2))
                    result = psmCountCharge1 / (double)psmCountCharge2;
            }

            return PRISM.StringUtilities.DblToString(result, 6, 0.0000001);
        }

        /// <summary>
        /// IS-3B: Count of 3+ peptides / count of 2+ peptides
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string IS_3B()
        {
            double result = 0;

            if (mCachedPSMStatsByCharge.Count == 0)
            {
                Cache_IS3_Data();
            }

            if (mCachedPSMStatsByCharge.TryGetValue(2, out var psmCountCharge2))
            {
                if (mCachedPSMStatsByCharge.TryGetValue(3, out var psmCountCharge3))
                    result = psmCountCharge3 / (double)psmCountCharge2;
            }

            return PRISM.StringUtilities.DblToString(result, 6, 0.0000001);
        }

        /// <summary>
        /// IS-3C: Count of 4+ peptides / count of 2+ peptides
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string IS_3C()
        {
            double result = 0;

            if (mCachedPSMStatsByCharge.Count == 0)
            {
                Cache_IS3_Data();
            }

            if (mCachedPSMStatsByCharge.TryGetValue(2, out var psmCountCharge2))
            {
                if (mCachedPSMStatsByCharge.TryGetValue(4, out var psmCountCharge4))
                    result = psmCountCharge4 / (double)psmCountCharge2;
            }

            return PRISM.StringUtilities.DblToString(result, 6, 0.0000001);
        }

        private void Cache_IS3_Data()
        {
            mDBInterface.SetQuery("SELECT Charge, COUNT(*) AS PSMs "
                    + " FROM temp_PSMs "
                    + " WHERE random_id=" + mRandomId
                    + "   AND MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                    + " GROUP BY Charge;");

            string[] columnNames = { "Charge", "PSMs" };

            mDBInterface.InitReader();

            mCachedPSMStatsByCharge.Clear();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                if (int.TryParse(measurementResults["Charge"], out var charge))
                {
                    if (int.TryParse(measurementResults["PSMs"], out var psmCount))
                    {
                        if (!mCachedPSMStatsByCharge.ContainsKey(charge))
                            mCachedPSMStatsByCharge.Add(charge, psmCount);
                    }
                }
            }
        }

        /// <summary>
        /// MS1_1: Median MS1 ion injection time
        /// </summary>
        public string MS1_1()
        {
            mDBInterface.SetQuery("SELECT temp_ScanStatsEx.Ion_Injection_Time "
                + " FROM temp_ScanStats, temp_ScanStatsEx "
                + " WHERE temp_ScanStats.ScanNumber = temp_ScanStatsEx.ScanNumber "
                + "  AND temp_ScanStatsEx.random_id = " + mRandomId
                + "  AND temp_ScanStats.random_id = " + mRandomId
                + "  AND temp_ScanStats.ScanType = 1 "
                + " ORDER BY temp_ScanStats.ScanNumber;");

            var values = new List<double>();

            string[] columnNames = { "Ion_Injection_Time" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                // Add to the filter list
                values.Add(double.Parse(measurementResults["Ion_Injection_Time"]));
            }

            var median = ComputeMedian(values);

            return PRISM.StringUtilities.ValueToString(median, 5);
        }

        /// <summary>
        /// MS1_2A: Median S/N value for MS1 spectra from run start through middle 50% of separation
        /// </summary>
        public string MS1_2A()
        {
            var median = 0.00;

            if (mCachedBasePeakSignalToNoiseRatio.Count == 0)
            {
                Cache_MS1_2_Data();
            }

            if (mCachedBasePeakSignalToNoiseRatio.Count > 0)
            {
                median = ComputeMedian(mCachedBasePeakSignalToNoiseRatio);
            }

            return PRISM.StringUtilities.ValueToString(median, 5);
        }

        /// <summary>
        /// MS1_2B: Median TIC value for identified peptides from run start through middle 50% of separation
        /// </summary>
        public string MS1_2B()
        {
            if (mCachedTotalIonIntensity.Count == 0)
            {
                Cache_MS1_2_Data();
            }

            var median = ComputeMedian(mCachedTotalIonIntensity);

            // Divide by 1000
            median /= 1000;

            return PRISM.StringUtilities.ValueToString(median, 5, 100000000);
        }

        private void Cache_MS1_2_Data()
        {
            var scanFirstPeptide = GetStoredValueInt(CachedResultTypes.ScanFirstFilterPassingPeptide, 0);
            var scanEndC2A = GetStoredValueInt(CachedResultTypes.C2A_RegionScanEnd, 0);

            mDBInterface.SetQuery("SELECT BasePeakSignalToNoiseRatio, TotalIonIntensity "
                + " FROM temp_ScanStats"
                + " WHERE temp_ScanStats.random_id=" + mRandomId
                + "   AND ScanType = 1 "
                + "   AND ScanNumber >= " + scanFirstPeptide
                + "   AND ScanNumber <= " + scanEndC2A);

            string[] columnNames = { "BasePeakSignalToNoiseRatio", "TotalIonIntensity" };

            mDBInterface.InitReader();

            mCachedBasePeakSignalToNoiseRatio.Clear();
            mCachedTotalIonIntensity.Clear();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                mCachedBasePeakSignalToNoiseRatio.Add(double.Parse(measurementResults["BasePeakSignalToNoiseRatio"]));
                mCachedTotalIonIntensity.Add(double.Parse(measurementResults["TotalIonIntensity"]));
            }
        }

        /// <summary>
        /// MS1_3A: Dynamic range estimate using 95th percentile peptide peak apex intensity / 5th percentile
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS1_3A()
        {
            double final = 0;

            if (mCachedMS1_3.Count == 0)
            {
                Cache_MS1_3_Data();
            }

            if (mCachedPeakMaxIntensity_5thPercentile > 0)
            {
                final = mCachedPeakMaxIntensity_95thPercentile / mCachedPeakMaxIntensity_5thPercentile;
            }

            // Round the result
            return PRISM.StringUtilities.DblToString(final, 3, 0.0001);
        }

        /// <summary>
        /// MS1_3B: Median peak apex intensity for all peptides
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS1_3B()
        {
            if (mCachedMS1_3.Count == 0)
            {
                Cache_MS1_3_Data();
            }

            var median = ComputeMedian(mCachedMS1_3);

            // Round based on the magnitude of the intensity

            if (median > 100)
                return median.ToString("0");

            return median.ToString("0.0");
        }

        private void Cache_MS1_3_Data()
        {
            mDBInterface.SetQuery("SELECT temp_SICStats.PeakMaxIntensity"
                + " FROM temp_SICStats, temp_PSMs"
                + " WHERE temp_SICStats.FragScanNumber=temp_PSMs.Scan"
                + "   AND temp_SICStats.random_id=" + mRandomId
                + "   AND temp_PSMs.random_id=" + mRandomId
                + "   AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                + " ORDER BY temp_SICStats.PeakMaxIntensity, temp_PSMs.Result_ID DESC;");

            // This list stores the max peak intensity for each PSM, using data between the 5th and 95th percentiles
            var maxPeakIntensities = new List<double>();

            string[] columnNames = { "PeakMaxIntensity" };

            mDBInterface.InitReader();

            mCachedPeakMaxIntensity_5thPercentile = 0;
            mCachedPeakMaxIntensity_95thPercentile = 0;
            mCachedMS1_3.Clear();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                // Add to the filter list
                mCachedMS1_3.Add(double.Parse(measurementResults["PeakMaxIntensity"]));
            }

            if (mCachedMS1_3.Count > 0)
            {
                // Store values between the 5th and 95th percentiles
                for (var i = 0; i < mCachedMS1_3.Count; i++)
                {
                    // Check if between 5-95%
                    var percent = i / (double)mCachedMS1_3.Count;
                    if (percent >= 0.05 && percent <= 0.95)
                    {
                        // Add to the MPI list
                        maxPeakIntensities.Add(mCachedMS1_3[i]);
                    }
                }
            }

            if (maxPeakIntensities.Count > 0)
            {
                // Calculate final values
                mCachedPeakMaxIntensity_5thPercentile = maxPeakIntensities.Min();
                mCachedPeakMaxIntensity_95thPercentile = maxPeakIntensities.Max();
            }
        }

        /// <summary>
        /// DS_3A: Median of MS1 max / MS1 sampled abundance
        /// </summary>
        public string DS_3A()
        {
            return DS_3_Shared(bottom50Pct: false);
        }

        /// <summary>
        /// DS_3B: Median of MS1 max / MS1 sampled abundance; limit to bottom 50% of peptides by abundance
        /// </summary>
        public string DS_3B()
        {
            return DS_3_Shared(bottom50Pct: true);
        }

        private void DS_3_CacheData()
        {
            mDBInterface.SetQuery("SELECT ParentIonIntensity, PeakMaxIntensity"
                + " FROM temp_SICStats, temp_PSMs "
                + " WHERE temp_SICStats.FragScanNumber=temp_PSMs.Scan"
                + "   AND temp_SICStats.random_id=" + mRandomId
                + "   AND temp_PSMs.random_id=" + mRandomId
                + "   AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);

            // Ratio of PeakMaxIntensity over ParentIonIntensity
            var values = new List<double>();

            string[] columnNames = { "ParentIonIntensity", "PeakMaxIntensity" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                // Compute the ratio of the PeakMaxIntensity over ParentIonIntensity
                var parentIonIntensity = double.Parse(measurementResults["ParentIonIntensity"]);
                double ratioPeakMaxToParentIonIntensity;
                if (parentIonIntensity > 0)
                    ratioPeakMaxToParentIonIntensity = double.Parse(measurementResults["PeakMaxIntensity"]) / parentIonIntensity;
                else
                    ratioPeakMaxToParentIonIntensity = 0;

                values.Add(ratioPeakMaxToParentIonIntensity);
            }

            // Sort the values
            values.Sort();

            // Stores MS1 max / MS1 sampled abundance
            mCachedDS3.Clear();

            // Stores MS1 max / MS1 sampled abundance for ratios in the bottom 50%
            mCachedDS3_Bottom50pct.Clear();

            // Loop through all keys
            for (var i = 0; i < values.Count; i++)
            {
                // Add to the list
                mCachedDS3.Add(values[i]);

                if ((i + 1) / (double)values.Count <= 0.5)
                {
                    // In valid bottom 50% so add to DS3_Bottom50pct
                    mCachedDS3_Bottom50pct.Add(values[i]);
                }
            }
        }

        /// <summary>
        /// Median of MS1 max / MS1 sampled abundance
        /// </summary>
        /// <param name="bottom50Pct">Set to true to limit to the bottom 50% of peptides by abundance</param>
        private string DS_3_Shared(bool bottom50Pct)
        {
            if (mCachedDS3.Count == 0)
            {
                DS_3_CacheData();
            }

            double median;
            if (bottom50Pct)
                median = ComputeMedian(mCachedDS3_Bottom50pct);
            else
                median = ComputeMedian(mCachedDS3);

            // Round the result
            return PRISM.StringUtilities.DblToString(median, 3, 0.0001);
        }

        /// <summary>
        /// IS_1A: Occurrences of MS1 jumping >10x
        /// </summary>
        public string IS_1A()
        {
            const int foldThreshold = 10;

            IS_1_Shared(foldThreshold, out var countMS1Jump10x, out _);
            return countMS1Jump10x.ToString();
        }

        /// <summary>
        /// IS_1B: Occurrences of MS1 falling >10x
        /// </summary>
        public string IS_1B()
        {
            const int foldThreshold = 10;

            IS_1_Shared(foldThreshold, out _, out var countMS1Fall10x);
            return countMS1Fall10x.ToString();
        }

        private void IS_1_Shared(int foldThreshold, out int countMS1Jump10x, out int countMS1Fall10x)
        {
            mDBInterface.SetQuery("SELECT ScanNumber, BasePeakIntensity  "
                + " FROM temp_ScanStats "
                + " WHERE temp_ScanStats.random_id=" + mRandomId
                + "   AND ScanType = 1");

            double bpiPrevious = -1;

            // Count for IS_1A
            countMS1Jump10x = 0;

            // Count for IS_1B
            countMS1Fall10x = 0;

            // Validate the foldThreshold
            if (foldThreshold < 2)
                foldThreshold = 2;

            string[] columnNames = { "ScanNumber", "BasePeakIntensity" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                var bpiCurrent = double.Parse(measurementResults["BasePeakIntensity"]);

                if (bpiPrevious > -1)
                {
                    if (bpiCurrent > 0 && bpiPrevious / bpiCurrent > foldThreshold)
                    {
                        countMS1Fall10x++;
                    }

                    if (bpiPrevious > 0 && bpiCurrent / bpiPrevious > foldThreshold)
                    {
                        countMS1Jump10x++;
                    }
                }

                bpiPrevious = bpiCurrent;
            }
        }

        /// <summary>
        /// MS1_5A: Median of precursor mass error (Th)
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS1_5A()
        {
            if (mCachedDelM.Count == 0)
            {
                Cache_MS1_5_Data();
            }

            if (mCachedDelM.Count == 0)
                return "0";

            var median = ComputeMedian(mCachedDelM);

            // Round the result
            return PRISM.StringUtilities.DblToString(median, 6, 0.0000001);
        }

        /// <summary>
        /// MS1_5B: Median of absolute value of precursor mass error (Th)
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS1_5B()
        {
            if (mCachedDelM.Count == 0)
            {
                Cache_MS1_5_Data();
            }

            if (mCachedDelM.Count == 0)
                return "0";

            // Take the absolute value of each DelM value and store in a list
            var values = new List<double>(mCachedDelM.Count);

            foreach (var value in mCachedDelM)
            {
                values.Add(Math.Abs(value));
            }

            var average = values.Average();

            // Round the result
            return PRISM.StringUtilities.DblToString(average, 6, 0.0000001);
        }

        /// <summary>
        /// MS1_5C: Median of precursor mass error (ppm)
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS1_5C()
        {
            if (mCachedDelMppm.Count == 0)
            {
                Cache_MS1_5_Data();
            }

            if (mCachedDelMppm.Count == 0)
                return "0";

            var median = ComputeMedian(mCachedDelMppm);

            return PRISM.StringUtilities.DblToString(median, 3, 0.0001);
        }

        /// <summary>
        /// MS1_5D: Interquartile distance in ppm-based precursor mass error
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS1_5D()
        {
            if (mCachedDelMppm.Count == 0)
            {
                Cache_MS1_5_Data();
            }

            if (mCachedDelMppm.Count == 0)
                return "0";

            // Sort the DelM_ppm values
            mCachedDelMppm.Sort();

            var interquartilePPMErrors = new List<double>();

            // Calculate inter_quartile start and end
            var interQuartileStart = (int)Math.Round(0.25 * mCachedDelMppm.Count);
            var interQuartileEnd = (int)Math.Round(0.75 * mCachedDelMppm.Count);

            // Loop through the cached ppm errors
            for (var i = 0; i < mCachedDelMppm.Count; i++)
            {
                if (i >= interQuartileStart && i <= interQuartileEnd)
                {
                    interquartilePPMErrors.Add(mCachedDelMppm[i]);
                }
            }

            var median = ComputeMedian(interquartilePPMErrors);

            // Round the result
            return PRISM.StringUtilities.DblToString(median, 3, 0.0001);
        }

        private void Cache_MS1_5_Data()
        {
            mDBInterface.SetQuery("SELECT temp_PSMs.Peptide_MH, temp_PSMs.Charge, temp_SICStats.MZ, temp_PSMs.DelM_Da, temp_PSMs.DelM_PPM"
                    + " FROM temp_PSMs, temp_SICStats"
                    + " WHERE temp_SICStats.FragScanNumber=temp_PSMs.Scan AND temp_SICStats.random_id=" + mRandomId + " AND temp_PSMs.random_id=" + mRandomId
                    + " AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);

            const double massC13 = 1.00335483;

            string[] columnNames = { "Peptide_MH", "Charge", "MZ", "DelM_Da", "DelM_PPM" };

            mDBInterface.InitReader();

            mCachedDelM.Clear();
            mCachedDelMppm.Clear();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                // Calculate the theoretical monoisotopic mass of the peptide
                var theoreticalMonoMass = mPeptideMassCalculator.ConvoluteMass(double.Parse(measurementResults["Peptide_MH"]), 1, 0);

                // Compute observed precursor mass, as monoisotopic mass
                var observedCharge = int.Parse(measurementResults["Charge"]);
                var observedMonoMass = mPeptideMassCalculator.ConvoluteMass(double.Parse(measurementResults["MZ"]), observedCharge, 0);

                var deltaMass = observedMonoMass - theoreticalMonoMass;

                // Correct the deltaMass value by assuring that it is between -0.5 and5 0.5
                // This corrects for the instrument choosing the 2nd or 3rd isotope of an isotopic distribution as the parent ion
                while (deltaMass < -0.5)
                    deltaMass += massC13;

                while (deltaMass > 0.5)
                    deltaMass -= massC13;

                // ReSharper disable once IdentifierTypo
                double delMppm = 0;

                if (theoreticalMonoMass > 0)
                {
                    delMppm = deltaMass / (theoreticalMonoMass / 1000000);
                }

                if (Math.Abs(delMppm) > 200)
                {
                    Console.WriteLine("Large DelM_PPM: " + delMppm);
                }

                mCachedDelM.Add(deltaMass);

                mCachedDelMppm.Add(delMppm);
            }
        }

        /// <summary>
        /// MS2_1: Median MS2 ion injection time for identified peptides
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_1()
        {
            mDBInterface.SetQuery("SELECT temp_ScanStatsEx.Ion_Injection_Time "
                + " FROM temp_PSMs, temp_ScanStatsEx "
                + " WHERE temp_PSMs.Scan=temp_ScanStatsEx.ScanNumber AND temp_PSMs.random_id=" + mRandomId + " AND temp_ScanStatsEx.random_id=" + mRandomId
                + "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);

            var filterList = new List<double>();

            string[] columnNames = { "Ion_Injection_Time" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                filterList.Add(double.Parse(measurementResults["Ion_Injection_Time"]));
            }

            var median = ComputeMedian(filterList);

            // Round the result
            return PRISM.StringUtilities.DblToString(median, 3, 0.0001);
        }

        /// <summary>
        /// MS2_2: Median S/N value for identified MS2 spectra
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_2()
        {
            mDBInterface.SetQuery("SELECT temp_ScanStats.BasePeakSignalToNoiseRatio "
                + " FROM temp_PSMs, temp_ScanStats "
                + " WHERE temp_PSMs.Scan=temp_ScanStats.ScanNumber AND temp_PSMs.random_id=" + mRandomId + " AND temp_ScanStats.random_id=" + mRandomId
                + "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);

            var filterList = new List<double>();
            var finishedList = new List<double>();

            string[] columnNames = { "BasePeakSignalToNoiseRatio" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                // Add to the filtered list
                filterList.Add(double.Parse(measurementResults["BasePeakSignalToNoiseRatio"]));
            }

            for (var i = 0; i < filterList.Count; i++)
            {
                if (i + 1 <= filterList.Count * 0.75)
                {
                    // Below the 75th percentile, so add to the final list
                    finishedList.Add(filterList[i]);
                }
            }

            var median = ComputeMedian(finishedList);

            // Round the result
            return PRISM.StringUtilities.DblToString(median, 3, 0.0001);
        }

        /// <summary>
        /// MS2_3: Median number of peaks in all MS2 spectra
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_3()
        {
            mDBInterface.SetQuery("SELECT temp_ScanStats.IonCountRaw "
                + " FROM temp_PSMs, temp_ScanStats "
                + " WHERE temp_PSMs.Scan=temp_ScanStats.ScanNumber AND temp_PSMs.random_id=" + mRandomId + " AND temp_ScanStats.random_id=" + mRandomId
                + "  AND temp_PSMs.MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD);

            var filterList = new List<double>();

            string[] columnNames = { "IonCountRaw" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                filterList.Add(double.Parse(measurementResults["IonCountRaw"]));
            }

            var median = ComputeMedian(filterList);

            // Round to the nearest integer
            return median.ToString("0");
        }

        /// <summary>
        /// MS2_4A: Fraction of all MS2 spectra identified; low abundance quartile (determined using MS1 intensity of identified peptides)
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_4A()
        {
            if (!mMS2QuartileCountsCached)
            {
                Cache_MS2_4_Data();
            }

            var result = Compute_MS2_4_Ratio(1);
            return PRISM.StringUtilities.DblToString(result, 4, 0.00001);
        }

        /// <summary>
        /// MS2_4B: Fraction of all MS2 spectra identified; second quartile (determined using MS1 intensity of identified peptides)
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_4B()
        {
            if (!mMS2QuartileCountsCached)
            {
                Cache_MS2_4_Data();
            }

            var result = Compute_MS2_4_Ratio(2);
            return PRISM.StringUtilities.DblToString(result, 4, 0.00001);
        }

        /// <summary>
        /// MS2_4C: Fraction of all MS2 spectra identified; third quartile (determined using MS1 intensity of identified peptides)
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_4C()
        {
            if (!mMS2QuartileCountsCached)
            {
                Cache_MS2_4_Data();
            }

            var result = Compute_MS2_4_Ratio(3);
            return PRISM.StringUtilities.DblToString(result, 4, 0.00001);
        }

        /// <summary>
        /// MS2_4D: Fraction of all MS2 spectra identified; high abundance quartile (determined using MS1 intensity of identified peptides)
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_4D()
        {
            if (!mMS2QuartileCountsCached)
            {
                Cache_MS2_4_Data();
            }

            var result = Compute_MS2_4_Ratio(4);
            return PRISM.StringUtilities.DblToString(result, 4, 0.00001);
        }

        private double Compute_MS2_4_Ratio(int quartile)
        {
            double result = 0;

            if (mMS2QuartileScanCounts.TryGetValue(quartile, out var scanCountTotal))
            {
                if (scanCountTotal > 0)
                {
                    result = mMS2QuartileConfidentPSMs[quartile] / (double)scanCountTotal;
                }
            }

            return result;
        }

        private void Cache_MS2_4_Data()
        {
            mDBInterface.SetQuery("SELECT COUNT(*) as MS2ScanCount "
                + " FROM (SELECT DISTINCT temp_PSMs.Scan, temp_SICStats.PeakMaxIntensity "
                + "       FROM temp_PSMs, temp_SICStats "
                + "       WHERE temp_PSMs.Scan=temp_SICStats.FragScanNumber AND temp_PSMs.random_id=" + mRandomId + " AND temp_SICStats.random_id=" + mRandomId
                + "      ) LookupQ;");

            string[] columnNames = { "MS2ScanCount" };

            mDBInterface.InitReader();

            mDBInterface.ReadSingleLine(columnNames, out var measurementResults);
            var scanCountMS2 = int.Parse(measurementResults["MS2ScanCount"]);

            // Note that we sort by ascending PeakMaxIntensity
            // Thus, quartile 1 in mMS2QuartileScanCounts and mMS2QuartileConfidentPSMs will have the lowest abundance peptides

            mDBInterface.SetQuery("SELECT temp_PSMs.Scan, temp_SICStats.PeakMaxIntensity, Min(temp_PSMs.MSGFSpecProb) AS Peptide_Score "
                + " FROM temp_PSMs, temp_SICStats"
                + " WHERE temp_PSMs.Scan=temp_SICStats.FragScanNumber AND temp_PSMs.random_id=" + mRandomId + " AND temp_SICStats.random_id=" + mRandomId
                + " GROUP BY temp_PSMs.Scan, temp_SICStats.PeakMaxIntensity "
                + " ORDER BY temp_SICStats.PeakMaxIntensity;");

            mMS2QuartileScanCounts.Clear();

            mMS2QuartileConfidentPSMs.Clear();

            for (var i = 1; i <= 4; i++)
            {
                mMS2QuartileScanCounts.Add(i, 0);
                mMS2QuartileConfidentPSMs.Add(i, 0);
            }

            var scansProcessed = 1;

            string[] columnNames2 = { "Scan", "Peptide_Score", "PeakMaxIntensity" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames2, out measurementResults) && measurementResults.Count > 0)
            {
                // Compare the peptide_score vs. the threshold
                var passedFilter = double.TryParse(measurementResults["Peptide_Score"], out var peptideScore) &&
                                   PeptideScorePassesFilter(peptideScore);

                // Compare the peptide_score vs. the threshold

                // Compare the running count to scanCountMS2 to determine the quartile
                if (scansProcessed < scanCountMS2 * 0.25)
                {
                    // 1st quartile
                    UpdateMS2_4_QuartileStats(1, passedFilter);
                }

                if (scansProcessed >= scanCountMS2 * 0.25 && scansProcessed < scanCountMS2 * 0.5)
                {
                    // 2nd quartile
                    UpdateMS2_4_QuartileStats(2, passedFilter);
                }

                if (scansProcessed >= scanCountMS2 * 0.5 && scansProcessed < scanCountMS2 * 0.75)
                {
                    // 3rd quartile
                    UpdateMS2_4_QuartileStats(3, passedFilter);
                }

                if (scansProcessed >= scanCountMS2 * 0.75)
                {
                    // 4th quartile
                    UpdateMS2_4_QuartileStats(4, passedFilter);
                }

                scansProcessed++;
            }

            if (scansProcessed > scanCountMS2 + 1 && scansProcessed > scanCountMS2 * 1.01)
            {
                Console.WriteLine("Possible bug in Cache_MS2_4_Data, running_scan_count >> scanCountMS2: " + scansProcessed + " vs. " + scanCountMS2);
            }

            mMS2QuartileCountsCached = true;
        }

        private void UpdateMS2_4_QuartileStats(int quartile, bool passedFilter)
        {
            mMS2QuartileScanCounts[quartile]++;

            if (passedFilter)
            {
                mMS2QuartileConfidentPSMs[quartile]++;
            }
        }

        /// <summary>
        /// P_1A: Median peptide ID score (X!Tandem hyperscore or -Log10(MSGF_SpecProb))
        /// </summary>
        public string P_1A()
        {
            mDBInterface.SetQuery("SELECT Scan, Max(-Log10(MSGFSpecProb)) AS Peptide_Score"
                + " FROM temp_PSMs "
                + " WHERE random_id=" + mRandomId
                + " GROUP BY Scan "
                + " ORDER BY Scan;");

            // Track X!Tandem hyperscore or -Log10(MSGFSpecProb)
            var peptideScoreList = new List<double>();

            string[] columnNames = { "Scan", "Peptide_Score" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                if (double.TryParse(measurementResults["Peptide_Score"], out var peptideScore))
                {
                    peptideScoreList.Add(peptideScore);
                }
            }

            var median = ComputeMedian(peptideScoreList);

            // Round the result
            return PRISM.StringUtilities.DblToString(median, 2, 0.0001);
        }

        /// <summary>
        /// P_1B: Median peptide ID score (X!Tandem Peptide_Expectation_Value_Log(e) or Log10(MSGF_SpecProb)
        /// </summary>
        public string P_1B()
        {
            mDBInterface.SetQuery("SELECT Scan, Min(Log10(MSGFSpecProb)) AS Peptide_Score"
                + " FROM temp_PSMs "
                + " WHERE random_id=" + mRandomId
                + " GROUP BY Scan "
                + " ORDER BY Scan;");

            // Track X!Tandem Peptide_Expectation_Value_Log or Log10(MSGFSpecProb)
            var peptideScoreList = new List<double>();

            string[] columnNames = { "Scan", "Peptide_Score" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                if (double.TryParse(measurementResults["Peptide_Score"], out var peptideScore))
                {
                    peptideScoreList.Add(peptideScore);
                }
            }

            var median = ComputeMedian(peptideScoreList);

            // Round the result
            return PRISM.StringUtilities.DblToString(median, 3, 0.0001);
        }

        /// <summary>
        /// P_2A: Number of fully, partially, and non-tryptic peptides; total spectra count
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        /// <returns>Total PSMs (spectra with a filter-passing match)</returns>
        public string P_2A()
        {
            return P_2A_Shared(phosphoPeptides: false);
        }

        private string P_2A_Shared(bool phosphoPeptides)
        {
            mDBInterface.SetQuery("SELECT Cleavage_State, Count(*) AS Spectra "
                                + " FROM ( SELECT Scan, Max(Cleavage_State) AS Cleavage_State "
                                + "        FROM temp_PSMs "
                                + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                + "          AND random_id=" + mRandomId
                                + PhosphoFilter(phosphoPeptides)
                                + "        GROUP BY Scan ) StatsQ "
                                + " GROUP BY Cleavage_State;");

            var dctPSMStats = new Dictionary<int, int>();

            string[] columnNames = { "Cleavage_State", "Spectra" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                dctPSMStats.Add(int.Parse(measurementResults["Cleavage_State"]), int.Parse(measurementResults["Spectra"]));
            }

            // Lookup the number of fully tryptic spectra (Cleavage_State = 2)
            if (dctPSMStats.TryGetValue(2, out var spectraCount))
                return spectraCount.ToString();

            return "0";
        }

        /// <summary>
        /// P_2B:Number of unique fully, partially, and non-tryptic peptides; unique peptide & charge count
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        /// <returns>Unique peptide count, counting charge states separately</returns>
        public string P_2B()
        {
            const bool groupByCharge = true;
            var dctPeptideStats = SummarizePSMs(groupByCharge);

            // Lookup the number of fully tryptic peptides (Cleavage_State = 2)
            if (dctPeptideStats.TryGetValue(2, out var peptideCount))
                return peptideCount.ToString();

            return "0";
        }

        /// <summary>
        /// P_2C: Number of unique fully, partially, and non-tryptic peptides; unique count regardless of charge
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        /// <returns>Unique peptide count</returns>
        public string P_2C()
        {
            const bool groupByCharge = false;
            var dctPeptideStats = SummarizePSMs(groupByCharge);

            // Lookup the number of fully tryptic peptides (Cleavage_State = 2)
            if (dctPeptideStats.TryGetValue(2, out var peptideCount))
                return peptideCount.ToString();

            return "0";
        }

        /// <summary>
        /// P_3: Ratio of unique semi-tryptic / fully tryptic peptides
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string P_3()
        {
            var dctPeptideStats = SummarizePSMs(groupByCharge: false);

            // Lookup the number of fully tryptic peptides (Cleavage_State = 2)
            if (!dctPeptideStats.TryGetValue(2, out var peptideCountFullyTryptic))
                peptideCountFullyTryptic = 0;

            // Lookup the number of partially tryptic peptides (Cleavage_State = 1)
            if (!dctPeptideStats.TryGetValue(1, out var peptideCountSemiTryptic))
                peptideCountSemiTryptic = 0;

            // Compute the ratio of semi-tryptic / fully tryptic peptides
            double answer = 0;
            if (peptideCountFullyTryptic > 0)
                answer = peptideCountSemiTryptic / (double)peptideCountFullyTryptic;

            // Round the result
            return PRISM.StringUtilities.DblToString(answer, 6, 0.0000001);
        }

        /// <summary>
        /// P_4A: Ratio of unique fully-tryptic / total unique peptides
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string P_4A()
        {
            var dctPeptideStats = SummarizePSMs(groupByCharge: false);

            // Lookup the number of fully tryptic peptides (Cleavage_State = 2)
            if (!dctPeptideStats.TryGetValue(2, out var peptideCountFullyTryptic))
                peptideCountFullyTryptic = 0;

            // Obtain the total unique number of peptides
            var peptideCountTotal = dctPeptideStats.Values.Sum();

            // Compute the ratio of fully-tryptic / total peptides (unique counts)

            double answer = 0;
            if (peptideCountTotal > 0)
                answer = peptideCountFullyTryptic / (double)peptideCountTotal;

            // Round the result
            return PRISM.StringUtilities.DblToString(answer, 6, 0.0000001);
        }

        /// <summary>
        /// P_4B: Ratio of total missed cleavages (among unique peptides) / total unique peptides
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string P_4B()
        {
            mDBInterface.SetQuery("SELECT Count(*) AS Peptides, SUM(MissedCleavages) as TotalMissedCleavages"
                                   + " FROM ( SELECT Unique_Seq_ID, Max(MissedCleavages) AS MissedCleavages "
                                   + "        FROM temp_PSMs "
                                   + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                   + "          AND random_id=" + mRandomId
                                   + " GROUP BY Unique_Seq_ID ) StatsQ");

            string[] columnNames = { "Peptides", "TotalMissedCleavages" };

            mDBInterface.InitReader();

            mDBInterface.ReadSingleLine(columnNames, out var measurementResults);

            var uniquePeptides = int.Parse(measurementResults["Peptides"]);
            var totalMissedCleavages = int.Parse(measurementResults["TotalMissedCleavages"]);

            // Compute the ratio of total missed cleavages / total unique peptides

            double answer = 0;
            if (uniquePeptides > 0)
                answer = totalMissedCleavages / (double)uniquePeptides;

            // Round the result
            return PRISM.StringUtilities.DblToString(answer, 6, 0.0000001);
        }

        /// <summary>
        /// Phos_2A: Number of tryptic phosphopeptides; total spectra count
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string Phos_2A()
        {
            return P_2A_Shared(phosphoPeptides: true);
        }

        /// <summary>
        /// Phos_2C: Number of tryptic phosphopeptides; unique peptide count
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string Phos_2C()
        {
            var dctPeptideStats = SummarizePSMs(groupByCharge: false, phosphoPeptides: true);

            // Lookup the number of fully tryptic peptides (Cleavage_State = 2)
            if (dctPeptideStats.TryGetValue(2, out var peptideCount))
                return peptideCount.ToString();

            return "0";
        }

        /// <summary>
        /// Keratin_2A: Number of keratin peptides; total spectra count
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string Keratin_2A()
        {
            mDBInterface.SetQuery("SELECT Count(*) AS Spectra "
                                   + " FROM ( SELECT Scan, Max(Cleavage_State) AS Cleavage_State "
                                   + "        FROM temp_PSMs "
                                   + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                   + "          AND random_id=" + mRandomId
                                   + "          AND KeratinPeptide = 1"
                                   + "        GROUP BY Scan ) StatsQ "
                                   + " WHERE Cleavage_State >= 0");

            string[] columnNames = { "Spectra" };

            mDBInterface.InitReader();

            mDBInterface.ReadSingleLine(columnNames, out var measurementResults);

            var keratinCount = int.Parse(measurementResults["Spectra"]);

            return keratinCount.ToString("0");
        }

        /// <summary>
        /// Keratin_2C: Number of keratin peptides; unique peptide count
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string Keratin_2C()
        {
            mDBInterface.SetQuery("SELECT Count(*) AS Peptides "
                                   + " FROM ( SELECT Unique_Seq_ID, Max(Cleavage_State) AS Cleavage_State "
                                   + "        FROM temp_PSMs "
                                   + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                   + "          AND random_id=" + mRandomId
                                   + "          AND KeratinPeptide = 1"
                                   + "        GROUP BY Unique_Seq_ID ) StatsQ "
                                   + " WHERE Cleavage_State >= 0");

            string[] columnNames = { "Peptides" };

            mDBInterface.InitReader();

            mDBInterface.ReadSingleLine(columnNames, out var measurementResults);

            var keratinCount = int.Parse(measurementResults["Peptides"]);

            return keratinCount.ToString("0");
        }

        /// <summary>
        /// Trypsin_2A: Number of peptides from trypsin; total spectra count
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string Trypsin_2A()
        {
            mDBInterface.SetQuery("SELECT Count(*) AS Spectra "
                                   + " FROM ( SELECT Scan, Max(Cleavage_State) AS Cleavage_State "
                                   + "        FROM temp_PSMs "
                                   + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                   + "          AND random_id=" + mRandomId
                                   + "          AND TrypsinPeptide = 1"
                                   + "        GROUP BY Scan ) StatsQ "
                                   + " WHERE Cleavage_State >= 0");

            string[] columnNames = { "Spectra" };

            mDBInterface.InitReader();

            mDBInterface.ReadSingleLine(columnNames, out var measurementResults);

            var trypsinCount = int.Parse(measurementResults["Spectra"]);

            return trypsinCount.ToString("0");
        }

        /// <summary>
        /// Trypsin_2C: Number of peptides from trypsin; unique peptide count
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string Trypsin_2C()
        {
            mDBInterface.SetQuery("SELECT Count(*) AS Peptides "
                                   + " FROM ( SELECT Unique_Seq_ID, Max(Cleavage_State) AS Cleavage_State "
                                   + "        FROM temp_PSMs "
                                   + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                   + "          AND random_id=" + mRandomId
                                   + "          AND TrypsinPeptide = 1"
                                   + "        GROUP BY Unique_Seq_ID ) StatsQ "
                                   + " WHERE Cleavage_State >= 0");

            string[] columnNames = { "Peptides" };

            mDBInterface.InitReader();

            mDBInterface.ReadSingleLine(columnNames, out var measurementResults);

            var trypsinCount = int.Parse(measurementResults["Peptides"]);

            return trypsinCount.ToString("0");
        }

        /// <summary>
        /// MS2_RepIon_All: Number of Filter-passing PSMs for which all of the Reporter Ions were seen
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_RepIon_All()
        {
            var psmCount = MS2_RepIon_Lookup(0);
            return psmCount.ToString("0");
        }

        /// <summary>
        /// MS2_RepIon_1Missing: Number of Filter-passing PSMs for which one Reporter Ion was not observed
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_RepIon_1Missing()
        {
            var psmCount = MS2_RepIon_Lookup(1);
            return psmCount.ToString("0");
        }

        /// <summary>
        /// MS2_RepIon_2Missing: Number of Filter-passing PSMs for which two Reporter Ions were not observed
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_RepIon_2Missing()
        {
            var psmCount = MS2_RepIon_Lookup(2);
            return psmCount.ToString("0");
        }

        /// <summary>
        /// MS2_RepIon_3Missing: Number of Filter-passing PSMs for which three Reporter Ions were not observed
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        public string MS2_RepIon_3Missing()
        {
            var psmCount = MS2_RepIon_Lookup(3);
            return psmCount.ToString("0");
        }

        private int MS2_RepIon_Lookup(int numMissingReporterIons)
        {
            // Determine the reporter ion mode by looking for non-null values
            // a) Make a list of all of the "Ion_" columns in table temp_ReporterIons
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

        private double ComputeReporterIonNoiseThreshold(IReadOnlyCollection<string> ionColumns)
        {
            var cachedThreshold = GetStoredValue(CachedResultTypes.ReporterIonNoiseThreshold, -1);
            if (cachedThreshold > -1)
                return cachedThreshold;

            var sql = new StringBuilder();

            foreach (var column in ionColumns)
            {
                if (sql.Length == 0)
                    sql.Append("SELECT ");
                else
                    sql.Append(", ");

                sql.AppendFormat("SUM (IfNull([{0}], 0)) AS [{0}_Sum], ", column);
                sql.AppendFormat("SUM (CASE WHEN IfNull([{0}], 0) = 0 Then 0 Else 1 End) AS [{0}_Count]", column);
            }

            sql.Append(" FROM temp_ReporterIons");
            sql.AppendFormat(" WHERE random_id={0}", mRandomId);

            mDBInterface.SetQuery(sql.ToString());

            var columnNames = (from item in ionColumns select item + "_Sum").ToList();
            columnNames.AddRange((from item in ionColumns select item + "_Count").ToList());

            mDBInterface.InitReader();

            mDBInterface.ReadSingleLine(columnNames.ToArray(), out var measurementResults);

            var overallSum = 0.0;
            var overallCount = 0;

            foreach (var column in ionColumns)
            {
                var columnSum = double.Parse(measurementResults[column + "_Sum"]);
                var columnCount = int.Parse(measurementResults[column + "_Count"]);

                overallSum += columnSum;
                overallCount += columnCount;
            }

            if (overallCount > 0)
            {
                var nonZeroAverage = overallSum / overallCount;
                var noiseThreshold = nonZeroAverage / 1000.0;

                AddUpdateResultsStorage(CachedResultTypes.ReporterIonNoiseThreshold, noiseThreshold);
                return noiseThreshold;
            }

            AddUpdateResultsStorage(CachedResultTypes.ReporterIonNoiseThreshold, 0);
            return 0;
        }

        private int CountPSMsWithNumObservedReporterIons(IEnumerable<string> ionColumns, double threshold, int reporterIonObsCount)
        {
            var sql = new StringBuilder();
            var thresholdText = threshold.ToString("0.000");

            foreach (var column in ionColumns)
            {
                if (sql.Length == 0)
                    sql.Append("SELECT ");
                else
                    sql.Append(" + ");

                sql.AppendFormat("CASE WHEN [{0}] > {1} Then 1 Else 0 End", column, thresholdText);
            }

            sql.Append(" AS ReporterIonCount");
            sql.Append(" FROM temp_ReporterIons INNER JOIN ");
            sql.Append("   temp_PSMs ON temp_ReporterIons.ScanNumber = temp_PSMs.scan AND ");
            sql.Append("   temp_ReporterIons.random_id = temp_PSMs.random_id ");
            sql.AppendFormat(" WHERE temp_PSMs.random_id={0}", mRandomId);
            sql.AppendFormat("   AND temp_PSMs.MSGFSpecProb <= {0}", MSGF_SPECPROB_THRESHOLD);

            var countQuery =
                " SELECT COUNT(*) AS PSMs" +
                " FROM (" + sql + ") AS FilterQ" +
                " WHERE ReporterIonCount = " + reporterIonObsCount;

            mDBInterface.SetQuery(countQuery);

            string[] columnNames = { "PSMs" };

            mDBInterface.InitReader();

            mDBInterface.ReadSingleLine(columnNames, out var measurementResults);

            var psmCount = int.Parse(measurementResults["PSMs"]);
            return psmCount;
        }

        private List<string> DetermineReporterIonColumns()
        {
            if (mIgnoreReporterIons)
                return new List<string>();

            if (mReporterIonColumns.Count > 0)
                return mReporterIonColumns;

            var columnList = mDBInterface.GetTableColumns("temp_ReporterIons");
            var ionColumns = columnList.Where(column => column.StartsWith("Ion_")).ToList();

            if (ionColumns.Count == 0)
            {
                mReporterIonColumns.Clear();
                mIgnoreReporterIons = true;
                return mReporterIonColumns;
            }

            var sql = new StringBuilder();

            foreach (var column in ionColumns)
            {
                if (sql.Length == 0)
                    sql.Append("SELECT ");
                else
                    sql.Append(", ");

                sql.AppendFormat("SUM (CASE WHEN [{0}] IS NULL Then 0 Else 1 End) AS [{0}]", column);
            }

            sql.Append(" FROM temp_ReporterIons");
            sql.AppendFormat(" WHERE random_id={0}", mRandomId);

            mDBInterface.SetQuery(sql.ToString());

            var columnNames = ionColumns.ToArray();

            mDBInterface.InitReader();

            mDBInterface.ReadSingleLine(columnNames, out var measurementResults);

            mReporterIonColumns.Clear();
            foreach (var column in ionColumns)
            {
                if (!string.IsNullOrWhiteSpace(measurementResults[column]))
                {
                    var nonNullCount = int.Parse(measurementResults[column]);
                    if (nonNullCount > 0)
                    {
                        mReporterIonColumns.Add(column);
                    }
                }
            }

            return mReporterIonColumns;
        }

        private string PhosphoFilter(bool phosphoPeptides)
        {
            if (phosphoPeptides)
                return " AND Phosphopeptide = 1";

            return string.Empty;
        }

        /// <summary>
        /// Counts the number of unique fully, partially, and non-tryptic peptides
        /// </summary>
        /// <remarks>Filters on MSGFSpecProb less than 1E-12</remarks>
        /// <param name="groupByCharge">If true, counts charges separately</param>
        /// <param name="phosphoPeptides">If true, only uses phosphopeptides</param>
        /// <returns>Unique peptide count</returns>
        private Dictionary<int, int> SummarizePSMs(bool groupByCharge, bool phosphoPeptides = false)
        {
            var chargeSql = string.Empty;

            if (groupByCharge)
            {
                chargeSql = ", Charge ";
            }

            mDBInterface.SetQuery("SELECT Cleavage_State, Count(*) AS Peptides "
                                + " FROM ( SELECT Unique_Seq_ID" + chargeSql + ", Max(Cleavage_State) AS Cleavage_State "
                                + "        FROM temp_PSMs "
                                + "        WHERE MSGFSpecProb <= " + MSGF_SPECPROB_THRESHOLD
                                + "          AND random_id=" + mRandomId
                                + PhosphoFilter(phosphoPeptides)
                                + " GROUP BY Unique_Seq_ID" + chargeSql + " ) StatsQ "
                                + " GROUP BY Cleavage_State;");

            var dctPeptideStats = new Dictionary<int, int>();

            string[] columnNames = { "Cleavage_State", "Peptides" };

            mDBInterface.InitReader();

            while (mDBInterface.ReadNextRow(columnNames, out var measurementResults) && measurementResults.Count > 0)
            {
                dctPeptideStats.Add(int.Parse(measurementResults["Cleavage_State"]), int.Parse(measurementResults["Peptides"]));
            }

            return dctPeptideStats;
        }
    }
}
