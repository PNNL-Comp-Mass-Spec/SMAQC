using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using System.IO;

namespace SMAQC
{
    class Measurement
    {
        //DECLARE VARIABLES
        private DBWrapper DBInterface;                                                      //CREATE DB INTERFACE OBJECT
        private Hashtable measurementhash = new Hashtable();                                        //HASH TABLE FOR MEASUREMENTS
        private int r_id;                                                                           //RANDOM ID FOR TEMP TABLES
		private Dictionary<string, double> mResultsStorage = new Dictionary<string, double>();		//SOME MEASUREMENTS HAVE DATA REQUIRED BY OTHERS ... WILL BE STORED HERE
                  
        //CONSTRUCTOR
        public Measurement(int r_id, ref DBWrapper DBInterface)
        {
            //CREATE CONNECTIONS
            this.r_id = r_id;                                                                       //SET RANDOM ID
            this.DBInterface = DBInterface;                                                         //SET DB OBJECT
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
			if (mResultsStorage.ContainsKey(entryName))
				mResultsStorage[entryName] = value;
			else
				mResultsStorage.Add(entryName, value);
		}

        //THIS CALLS OUR HASH TABLE CLEARER. WHICH WE NEED BETWEEN DATASETS AS IT IS NO LONGER NEEDED
        public void clearStorage()
        {
            //CLEAR HASHTABLE STORAGE
            measurementhash.Clear();
            mResultsStorage.Clear();
        }

		protected double GetStoredValue(string entryName, double valueIfMissing)
		{
			double value;

			if (mResultsStorage.TryGetValue(entryName, out value))
				return value;
			else
				return valueIfMissing;
		}

		protected int GetStoredValueInt(string entryName, int valueIfMissing)
		{
			double value = GetStoredValue(entryName, valueIfMissing);
			return (int)value;
		}

		protected System.Collections.Generic.Dictionary<int, int> GetResultIDToSeqIDTable()
		{
			System.Collections.Generic.Dictionary<int, int> ResultID_to_Unique_Seq_ID_Table = new System.Collections.Generic.Dictionary<int, int>();
			int resultID;
			int seqID;

			String[] fields_n2 = { "Result_ID", "Unique_Seq_ID" };
			Hashtable htValues = new Hashtable();                                        //HASH TABLE FOR MEASUREMENTS

			DBInterface.setQuery("SELECT * FROM temp_xt_resulttoseqmap WHERE temp_xt_resulttoseqmap.random_id=" + r_id + ";");
			DBInterface.initReader();
			while ((DBInterface.readLines(fields_n2, ref htValues)) && (htValues.Count > 0))
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

		protected System.Collections.Generic.Dictionary<int, int> GetSeqIDToCleavageStateTable()
		{
			System.Collections.Generic.Dictionary<int, int> Seq_ID_to_Cleavage_State_Table = new System.Collections.Generic.Dictionary<int, int>();
			int seqID;
			short cleavageState;

			String[] fields_n1 = { "Unique_Seq_ID", "Cleavage_State" };
			Hashtable htValues = new Hashtable();                                        //HASH TABLE FOR MEASUREMENTS

			DBInterface.setQuery("SELECT Unique_Seq_ID, MAX(Cleavage_State) AS Cleavage_State FROM `temp_xt_seqtoproteinmap` WHERE temp_xt_seqtoproteinmap.random_id=" + r_id + " GROUP BY Unique_Seq_ID;");
			DBInterface.initReader();
			while ((DBInterface.readLines(fields_n1, ref htValues)) && (htValues.Count > 0))
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
        public String C_1A()
        {
			bool countTailingPeptides = false;
			return C_1_Shared(countTailingPeptides);

        }

        /// <summary>
		/// C-1B: Fraction of peptides identified more than 4 minutes later than the chromatographic peak apex
        /// </summary>
        /// <returns></returns>
        public String C_1B()
        {
			bool countTailingPeptides = true;
			return C_1_Shared(countTailingPeptides);
        }

		/// <summary>
		/// Counts the number of peptides identified more than 4 minutes earlier or more than 4 minutes later than the chromatographic peak apex
		/// </summary>
		/// <param name="countTailingPeptides">False means to count early eluting peptides; True means to count late-eluting peptides</param>
		/// <returns></returns>
		protected String C_1_Shared(bool countTailingPeptides)
		{
			//SET DB QUERY
			DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.`Peptide_Expectation_Value_Log`, t1.FragScanNumber, t1.OptimalPeakApexScanNumber,"
			+ "temp_scanstats.ScanTime as ScanTime1, t2.ScanTime as ScanTime2 "
			+ "FROM temp_xt, temp_scanstats, temp_sicstats as t1 "
			+ "LEFT JOIN temp_scanstats as t2 on t1.OptimalPeakApexScanNumber=t2.ScanNumber "
			+ "WHERE temp_xt.Scan = t1.FragScanNumber "
			+ "AND temp_xt.Scan = temp_scanstats.ScanNumber "
			+ "AND temp_xt.random_id=" + r_id + " "
			+ "AND temp_scanstats.random_id=" + r_id + " "
			+ "AND t1.random_id=" + r_id + " "
			+ "AND t2.random_id=" + r_id + " "
			+ "ORDER BY Scan;");

			int difference_sum = 0;                                                             //FOR COLUMN J
			int valid_rows = 0;                                                                 //FOR COLUMN K
			decimal answer = 0.00M;                                                             //SOLUTION

			//DECLARE FIELDS TO READ FROM
			String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "FragScanNumber", "OptimalPeakApexScanNumber", "ScanTime1", "ScanTime2" };

			//INIT READER
			DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
			{
				//IF LOG(E) <= -2 ... CALCULATE DIFFERENCE [COLUMN C]
				if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					//CALC DIFFERENCE [COLUMN C]
					double temp_difference;
					if (countTailingPeptides)
						temp_difference = (Convert.ToDouble(measurementhash["ScanTime1"]) - Convert.ToDouble(measurementhash["ScanTime2"]));
					else
						temp_difference = (Convert.ToDouble(measurementhash["ScanTime2"]) - Convert.ToDouble(measurementhash["ScanTime1"]));

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
				answer = Math.Round(Convert.ToDecimal((double)difference_sum / valid_rows), 6);
				return Convert.ToString(answer);
			}
			else
				return string.Empty;
		}

        /// <summary>
		/// C-2A: Time period over which 50% of peptides are identified
        /// </summary>
        /// <returns></returns>
        public String C_2A()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.`Peptide_Expectation_Value_Log`, t1.FragScanNumber as ScanNumber,"
                + "temp_scanstats.ScanTime as ScanTime1 "
                + "FROM temp_xt, temp_scanstats, temp_sicstats as t1 "
                + "WHERE temp_xt.Scan = t1.FragScanNumber "
                + "AND temp_xt.Scan = temp_scanstats.ScanNumber "
                + "AND temp_xt.random_id=" + r_id + " "
                + "AND temp_scanstats.random_id=" + r_id + " "
                + "AND t1.random_id=" + r_id + " "
                + "ORDER BY Scan;");

            int running_sum = 0;                                                                //RUNNING SUM FOR COLUMN H
            List<double> ScanTimeList = new List<double>();                                     //STORES SCAN TIMES
            List<int> RunningSumList = new List<int>();                                         //STORES RUNNING SUM LISTS
            List<double> ScanRangeList = new List<double>();                                    //STORE SCAN TIME VALUES HERE THAT ARE WITHIN RANGE
            List<double> XScanTimeList = new List<double>();                                    //STORES SCAN TIMES [USED TO HELP FIND MS_2A/B]
            List<int> XScanNumberList = new List<int>();                                        //STORE SCAN NUMBER LIST [USED TO HELP FIND MS_2A/B]
            List<int> XAllScanNumberList = new List<int>();                                     //STORE SCAN NUMBER LIST [USED TO HELP FIND MS_2A/B]
            List<int> XQuartile_Scan = new List<int>();                                         //STORE ALL INTERQUARTILE SCANS, i.e. scans between the 25% and 75% quartile peptide ID passing filters [USED TO HELP FIND MS_2A/B]

            int prev_scan_time_within_range = 0;                                                //STORE PREV SCAN TIME TRUE/FALSE WITHIN RANGE [0=FALSE;1=TRUE] [USED TO HELP FIND MS_2A/B]
            int i = 0;                                                                          //COUNTER

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "ScanNumber", "ScanTime1" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //IF LOG(E) <= -2 ... CALCULATE DIFFERENCE [COLUMN D]
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //FOUND MATCH SO INCREMENT RUNNING SUM
                    running_sum++;

                    //ADD TO LIST [USED TO HELP FIND MS_2A/B]
                    XScanNumberList.Add(Convert.ToInt32(measurementhash["ScanNumber"]));
                    XScanTimeList.Add((double)Convert.ToDouble(measurementhash["ScanTime1"]));
                }

                //ADD SCAN TIME TO LIST
                ScanTimeList.Add((double)Convert.ToDouble(measurementhash["ScanTime1"]));

                //ADD ALL SCAN NUMBERS TO LIST [FOR MS_2A/B]
                XAllScanNumberList.Add(Convert.ToInt32(measurementhash["ScanNumber"]));

                //ADD RUNNING SUM TO LIST
                RunningSumList.Add(running_sum);
            }

            //CALCULATE METRIC BY LOOPING THROUGH RUNNING SUM LIST
            for (i = 0; i < RunningSumList.Count; i++)
            {
                //CALC RUNNING SUM
                double drsum = 0;
				if (running_sum > 0)
					drsum = (double)RunningSumList[i] / running_sum;

                int current_scan_time_within_range = 0;                                 //FOR MS_2A/B ... WE ARE WITHIN RANGE IF == 1 ELSE = 0
                int save_prev_scan_time_within_range = prev_scan_time_within_range;     //FOR MS_2A/B ... SAVED SINCE OVERWRITTEN

                //IF WITHIN 25% to 75%
				if ((drsum >= .25) && (drsum <= .75))
                {
                    //ADD TO SCAN RANGE LIST
                    ScanRangeList.Add(ScanTimeList[i]);

                    //SET WITHIN RANGE [FOR MS_2A/B]
                    current_scan_time_within_range = 1;

                    //UPDATE [FOR MS_2A/B]
                    prev_scan_time_within_range = 1;
                }
                else
                {
                    //UPDATE [FOR MS_2A/B] SET TO FALSE
                    prev_scan_time_within_range = 0;
                }

                //IF PREV WAS FALSE AND CURRENT SCAN TIME IS A NUMBER [FOR MS_2A/B]
                if (current_scan_time_within_range == 1 && save_prev_scan_time_within_range == 0 && i > 0)
                {
                    //GO WITH CURRENT SCAN NUMBER [FOR MS_2A/B]
                    XQuartile_Scan.Add(XAllScanNumberList[i]);
                }
                else if (prev_scan_time_within_range == 0 && save_prev_scan_time_within_range == 1 && i > 0)
                {
                    //GO WITH PREVIOUS SCAN NUMBER [FOR MS_2A/B]
                    XQuartile_Scan.Add(XAllScanNumberList[i - 1]);
                }
            }

            //NOW CALCULATE RESULT [FOR MS_2A/B]
            double xmin = XScanTimeList.Min();

            //LOOP THROUGH EACH VALUE SEARCHING FOR MIN [FOR MS_2A/B]
            for (i = 0; i < XScanTimeList.Count; i++)
            {
                //IF FOUND
                if (xmin == XScanTimeList[i])
                {
                    // ADD TO GLOBAL HASH TABLE FOR USE WITH MS_2A/B
					// SCAN_FIRST_FILTER_PASSING_PEPTIDE is the scan number of the first filter-passing peptide
					AddUpdateResultsStorage("SCAN_FIRST_FILTER_PASSING_PEPTIDE", XScanNumberList[i]);
                }
            }

            // CACHE THE SCAN NUMBERS AT THE START AND END OF THE INTEQUARTILE REGION
			AddUpdateResultsStorage("C_2A_REGION_SCAN_START", XQuartile_Scan.Min());
			AddUpdateResultsStorage("C_2A_REGION_SCAN_END", XQuartile_Scan.Max());

            //SORT IN LOW -> HIGH ORDER + DECLARE MIN / MAX
            ScanRangeList.Sort();
            double min = ScanRangeList[0];
            double max = ScanRangeList[ScanRangeList.Count-1];  //-1 AS START FROM 0
            double answer = max - min;

            //STORE IN GLOBAL HASH TABLE FOR C_2B
			AddUpdateResultsStorage("C_2A_ANSWER", answer);

            //CLEAR HASH TABLES
            ScanTimeList.Clear();
            RunningSumList.Clear();
            ScanRangeList.Clear();
            XScanTimeList.Clear();
            XAllScanNumberList.Clear();
            XScanNumberList.Clear();

            return Convert.ToString(answer);
        }


        /// <summary>
		/// C-2B: Fraction of peptides identified more than 4 minutes later than the chromatographic peak apex
        /// </summary>
        /// <returns></returns>
        public String C_2B()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.`Peptide_Expectation_Value_Log`, t1.FragScanNumber as ScanNumber,"
                + "temp_scanstats.ScanTime as ScanTime1 "
                + "FROM temp_xt, temp_scanstats, temp_sicstats as t1 "
                + "WHERE temp_xt.Scan = t1.FragScanNumber "
                + "AND temp_xt.Scan = temp_scanstats.ScanNumber "
                + "AND temp_xt.random_id=" + r_id + " "
                + "AND temp_scanstats.random_id=" + r_id + " "
                + "AND t1.random_id=" + r_id + " "
                + "ORDER BY Scan;");

            int running_sum = 0;                                                                //RUNNING SUM FOR COLUMN H
            List<double> ScanTimeList = new List<double>();                                     //STORES SCAN TIMES
            List<int> RunningSumList = new List<int>();                                         //STORES RUNNING SUM LISTS
            List<double> ScanRangeList = new List<double>();                                    //STORE SCAN TIME VALUES HERE THAT ARE WITHIN RANGE
            int counter = 0;                                                                    //STORE COUNT FOR COLUMN K
            int i = 0;                                                                          //COUNTER

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "ScanNumber", "ScanTime1" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //IF LOG(E) <= -2 ... CALCULATE DIFFERENCE [COLUMN D]
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //FOUND MATCH SO INCREMENT RUNNING SUM
                    running_sum++;
                }

                //ADD SCAN TIME TO LIST
                ScanTimeList.Add((double)Convert.ToDouble(measurementhash["ScanTime1"]));

                //ADD RUNNING SUM TO LIST
                RunningSumList.Add(running_sum);
            }

			string answerText = string.Empty;
			if (running_sum > 0)
			{
				//CALCULATE METRIC BY LOOPING THROUGH RUNNING SUM LIST
				for (i = 0; i < RunningSumList.Count; i++)
				{
					//CALC RUNNING SUM
					decimal drsum = Convert.ToDecimal((double)RunningSumList[i] / running_sum);
					double rsum = Convert.ToDouble(drsum);

					//IF WITHIN 25% to 75%
					if ((rsum >= .25) && (rsum <= .75))
					{
						//ADD TO SCOAN RANGE LIST
						ScanRangeList.Add(ScanTimeList[i]);

						//INC COUNTER
						counter++;
					}
				}

				//DETERMINE ANSWER
				double valC2A = GetStoredValue("C_2A_ANSWER", 0);
				if (valC2A != 0)
				{
					double answer = counter / valC2A;

					//WE NOW HAVE RESULT ... NOW ROUND IT TO 6th DIGIT
					decimal round_me = Convert.ToDecimal(answer);
					round_me = Math.Round(round_me, 6);                        //ROUND MEDIAN
					answerText = Convert.ToString(round_me);
				}
			}

            //CLEAR HASH TABLES
            ScanTimeList.Clear();
            RunningSumList.Clear();
            ScanRangeList.Clear();

			return answerText;
        }

        /// <summary>
		/// C-3A: Median peak width for all peptides
        /// </summary>
        /// <returns></returns>
        public String C_3A()
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
            String prv_Charge = "";                                         //INIT PREV CHARGE TO BLANK [REQUIRED FOR COMPARISON]
            String prv_Peptide_Sequence = "";                               //INIT PREV PEPTIDE SEQUENCE TO BLANK [REQUIRED FOR COMPARISON]
            String prev_Best_Evalue = "";                                   //INIT PREV BEST EVALUE TO BLANK [REQUIRED FOR COMPARISON]
            double median = 0.00;                                           //INIT MEDIAN

            //SET DB QUERY [REQUIRED TO SORT BY PEPTIDE SEQUENCE]
            DBInterface.setQuery("SELECT Scan, Charge, Peptide_Expectation_Value_Log,Peptide_Sequence FROM `temp_xt` "
            + "WHERE temp_xt.random_id=" + r_id + " ORDER BY Peptide_Sequence,Charge,Scan,Scan");

            //DECLARE FIELDS TO READ FROM
            String[] fields1 = { "Scan", "Charge", "Peptide_Expectation_Value_Log", "Peptide_Sequence" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS Q,R NOW

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields1, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //FIND COLUMN Q
                String Best_Evalue = "";

                //IF PREVIOUS PEPTIDE SEQUENCES == EACH OTHER && PREVIOUS CHARGES == EACH OTHER
                if (prv_Peptide_Sequence.Equals(Convert.ToString(measurementhash["Peptide_Sequence"])) && prv_Charge.Equals(Convert.ToString(measurementhash["Charge"])))
                {

                    //TAKE MIN [EITHER PREVIOUS BEST EVALUE OR CURRENT PEPTIDE EXPECTATION VALUE
                    if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) > Convert.ToDouble(prev_Best_Evalue))
                    {
                        Best_Evalue = prev_Best_Evalue;
                    }
                    else
                    {
                        Best_Evalue = Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"]);
                    }
                }
                else
                {
                    Best_Evalue = Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"]);
                }

                //NOW FIND COLUMN R IF COLUMN U IS == TRUE
                if (Best_Evalue.Equals(Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"])))
                {
                    //WE ARE NOW == TRUE FOR THIS COLUMN ... AS TRUE WE ADD THIS TO OUR HASH TABLE/WHATEVER

                    //[ADD HERE]
                    bestscan.Add(Convert.ToInt32(measurementhash["Scan"]));
                }

                //UPDATE PREVIOUS VALUES FOR NEXT LOOP
                prv_Charge = Convert.ToString(measurementhash["Charge"]);
                prv_Peptide_Sequence = Convert.ToString(measurementhash["Peptide_Sequence"]);
                prev_Best_Evalue = Best_Evalue;
            }

            //NOW SORT OUR VALUES SO THEY ARE IN THE CORRECT ORDER-
            bestscan.Sort();

            //SET DB QUERY
            DBInterface.setQuery("SELECT FragScanNumber, FWHMInScans, OptimalPeakApexScanNumber FROM `temp_sicstats` WHERE temp_sicstats.random_id=" + r_id + "");

            //DECLARE FIELDS TO READ FROM
            String[] fields2 = { "FragScanNumber", "FWHMInScans", "OptimalPeakApexScanNumber" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS D-F

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields2, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //ADD VALUES TO OUR TEMP HASH TABLES
                fragscannumber.Add(measurementhash["FragScanNumber"], measurementhash["FragScanNumber"]);
                fwhminscans.Add(measurementhash["FragScanNumber"], measurementhash["FWHMInScans"]);
                optimalpeakapexscannumber.Add(measurementhash["FragScanNumber"], measurementhash["OptimalPeakApexScanNumber"]);
            }

            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_scanstats.ScanNumber, temp_scanstats.ScanTime FROM `temp_scanstats` WHERE temp_scanstats.random_id=" + r_id + "");

            //DECLARE FIELDS TO READ FROM
            String[] fields3 = { "ScanNumber", "ScanTime" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS H-I
            i = 1;
            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields3, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //ADD TO SCANTIME HASH TABLE
                scantime.Add(Convert.ToString(i), measurementhash["ScanTime"]);

                //INCREMENT I POSITION
                i++;
            }

            //NOW START THE ACTUAL MEASUREMENT CALCULATION

            //LOOP THROUGH BESTSCAN
            for (i = 0; i < bestscan.Count; i++)
            {
                //FIND INDEX + OPTIMAL PEAK APEX SCAN +- FWHMIN FOR EACH RESULT [COLUMNS: M,O]
                String index = Convert.ToString(bestscan[i]);
                int OptimalPeakApexScanMinusFWHMIN = Convert.ToInt32(optimalpeakapexscannumber[index]) - Convert.ToInt32(Math.Ceiling(Convert.ToDouble(fwhminscans[index]) / 2));
                int OptimalPeakApexScanPlusFWHMIN = Convert.ToInt32(optimalpeakapexscannumber[index]) + Convert.ToInt32(Math.Ceiling(Convert.ToDouble(fwhminscans[index]) / 2));

                //FIND OTHER COLUMNS [N,P, Q,R,T]
                double start_time = Convert.ToDouble(scantime[Convert.ToString(OptimalPeakApexScanMinusFWHMIN)]);
                double end_time = Convert.ToDouble(scantime[Convert.ToString(OptimalPeakApexScanPlusFWHMIN)]);
                double end_minus_start = end_time - start_time;
                double end_minus_start_in_secs = end_minus_start * 60;
                double running_percent = Convert.ToDouble(running_sum) / Convert.ToDouble(bestscan.Count);

                //ADD end_minus_start_in_secs TO OUR LIST [COLUMN R]
                result.Add(end_minus_start_in_secs);

                //INCREMENT RUNNING SUM [COLUMN S]
                running_sum++;
            }

			string resultText = string.Empty;

			if (result.Count > 0)
			{
				//CALCULATE MEDIAN
				result.Sort();                          //START BY SORTING

				//IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
				if (result.Count % 2 == 1)
				{
					//IF ODD
					int pos = (result.Count / 2);
					median = result[result.Count / 2];
				}
				else
				{
					//IF EVEN
					int pos = (result.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
					median = (result[pos] + result[pos + 1]) / 2;
				}

				//WE NOW HAVE RESULT ... NOW ROUND IT TO 2ND DIGIT
				decimal round_me = Convert.ToDecimal(median);
				round_me = Math.Round(round_me, 2);                        //ROUND MEDIAN

				//Console.WriteLine("C_3A :: RESULT={0}", round_me);

				resultText = Convert.ToString(round_me);
			}

            //CLEAR HASH TABLES
            bestscan.Clear();
            fragscannumber.Clear();
            fwhminscans.Clear();
            optimalpeakapexscannumber.Clear();
            scantime.Clear();
            result.Clear();

            //RETURN RESULT
			return resultText;
        }

        /// <summary>
		/// C-3B: Median peak width during middle 50% of separation
        /// </summary>
        /// <returns></returns>
        public String C_3B()
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
            String prv_Charge = "";                                         //INIT PREV CHARGE TO BLANK [REQUIRED FOR COMPARISON]
            String prv_Peptide_Sequence = "";                               //INIT PREV PEPTIDE SEQUENCE TO BLANK [REQUIRED FOR COMPARISON]
            String prev_Best_Evalue = "";                                   //INIT PREV BEST EVALUE TO BLANK [REQUIRED FOR COMPARISON]
            double median = 0.00;                                           //INIT MEDIAN
            double START_RANGE = 0.25;                                      //FUNCTION START RANGE [REQUIRED + SET BY DEFINED MEASUREMENTS]
            double END_RANGE = 0.75;                                        //FUNCTION END RANGE [REQUIRED + SET BY DEFINED MEASUREMENTS]

            //SET DB QUERY [REQUIRED TO SORT BY PEPTIDE SEQUENCE]
            DBInterface.setQuery("SELECT Scan, Charge, Peptide_Expectation_Value_Log,Peptide_Sequence FROM `temp_xt` "
            + "WHERE temp_xt.random_id=" + r_id + " ORDER BY Peptide_Sequence,Charge,Scan,Scan");

            //DECLARE FIELDS TO READ FROM
            String[] fields1 = { "Scan", "Charge", "Peptide_Expectation_Value_Log", "Peptide_Sequence" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS Q,R NOW

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields1, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //FIND COLUMN Q
                String Best_Evalue = "";

                //IF PREVIOUS PEPTIDE SEQUENCES == EACH OTHER && PREVIOUS CHARGES == EACH OTHER
                if (prv_Peptide_Sequence.Equals(Convert.ToString(measurementhash["Peptide_Sequence"])) && prv_Charge.Equals(Convert.ToString(measurementhash["Charge"])))
                {

                    //TAKE MIN [EITHER PREVIOUS BEST EVALUE OR CURRENT PEPTIDE EXPECTATION VALUE
                    if ( Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) > Convert.ToDouble(prev_Best_Evalue))
                    {
                        Best_Evalue = prev_Best_Evalue;
                    }
                    else
                    {
                        Best_Evalue = Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"]);
                    }
                }
                else
                {
                    Best_Evalue = Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"]);
                }

                //NOW FIND COLUMN R IF COLUMN U IS == TRUE
                if (Best_Evalue.Equals( Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"]) ))
                {
                    //WE ARE NOW == TRUE FOR THIS COLUMN ... AS TRUE WE ADD THIS TO OUR HASH TABLE/WHATEVER

                    //[ADD HERE]
                    bestscan.Add(Convert.ToInt32(measurementhash["Scan"]));
                }

                //UPDATE PREVIOUS VALUES FOR NEXT LOOP
                prv_Charge = Convert.ToString(measurementhash["Charge"]);
                prv_Peptide_Sequence = Convert.ToString(measurementhash["Peptide_Sequence"]);
                prev_Best_Evalue = Best_Evalue;
            }

            //NOW SORT OUR VALUES SO THEY ARE IN THE CORRECT ORDER-
            bestscan.Sort();

            //SET DB QUERY
            DBInterface.setQuery("SELECT FragScanNumber, FWHMInScans, OptimalPeakApexScanNumber FROM `temp_sicstats` WHERE temp_sicstats.random_id=" + r_id + "");

            //DECLARE FIELDS TO READ FROM
            String[] fields2 = { "FragScanNumber", "FWHMInScans", "OptimalPeakApexScanNumber" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS D-F

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields2, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //ADD VALUES TO OUR TEMP HASH TABLES
                fragscannumber.Add(measurementhash["FragScanNumber"], measurementhash["FragScanNumber"]);
                fwhminscans.Add(measurementhash["FragScanNumber"], measurementhash["FWHMInScans"]);
                optimalpeakapexscannumber.Add(measurementhash["FragScanNumber"], measurementhash["OptimalPeakApexScanNumber"]);
            }

            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_scanstats.ScanNumber, temp_scanstats.ScanTime FROM `temp_scanstats` WHERE temp_scanstats.random_id=" + r_id + "");

            //DECLARE FIELDS TO READ FROM
            String[] fields3 = { "ScanNumber", "ScanTime" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS H-I
            i = 1;
            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields3, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //ADD TO SCANTIME HASH TABLE
                scantime.Add(Convert.ToString(i), measurementhash["ScanTime"]);

                //INCREMENT I POSITION
                i++;
            }

            //NOW START THE ACTUAL MEASUREMENT CALCULATION

            //LOOP THROUGH BESTSCAN
            for (i = 0; i < bestscan.Count; i++)
            {
                //FIND INDEX + OPTIMAL PEAK APEX SCAN +- FWHMIN FOR EACH RESULT [COLUMNS: M,O]
                String index = Convert.ToString(bestscan[i]);
                int OptimalPeakApexScanMinusFWHMIN = Convert.ToInt32(optimalpeakapexscannumber[index]) - Convert.ToInt32(Math.Ceiling(Convert.ToDouble(fwhminscans[index]) /2 ));
                int OptimalPeakApexScanPlusFWHMIN = Convert.ToInt32(optimalpeakapexscannumber[index]) + Convert.ToInt32(Math.Ceiling(Convert.ToDouble(fwhminscans[index]) / 2));

                //FIND OTHER COLUMNS [N,P, Q,R,T]
                double start_time = Convert.ToDouble(scantime[Convert.ToString(OptimalPeakApexScanMinusFWHMIN)]);
                double end_time = Convert.ToDouble(scantime[Convert.ToString(OptimalPeakApexScanPlusFWHMIN)]);
                double end_minus_start = end_time - start_time;
                double end_minus_start_in_secs = end_minus_start * 60;
                double running_percent = Convert.ToDouble(running_sum) / Convert.ToDouble(bestscan.Count);

                //CHECK FOR VALID RANGE DATA THEN ADD TO OUR RESULTS
                if (running_percent >= START_RANGE && running_percent <= END_RANGE)
                {
                    //WE ARE WITHING OUR VALID RANGE ... SO ADD end_minus_start_in_secs TO OUR LIST [COLUMN U]
                    result.Add(end_minus_start_in_secs);
                }

                //INCREMENT RUNNING SUM [COLUMN S]
                running_sum++;
            }

			string resultText = string.Empty;

			if (result.Count > 0)
			{
				//CALCULATE MEDIAN
				result.Sort();                          //START BY SORTING

				//IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
				if (result.Count % 2 == 1)
				{
					//IF ODD
					int pos = (result.Count / 2);
					median = result[result.Count / 2];
				}
				else
				{
					//IF EVEN
					int pos = (result.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
					median = (result[pos] + result[pos + 1]) / 2;
				}

				//WE NOW HAVE RESULT ... NOW ROUND IT TO 2ND DIGIT
				decimal round_me = Convert.ToDecimal(median);
				round_me = Math.Round(round_me, 2);                        //ROUND MEDIAN


				//Console.WriteLine("RESULT={0} -- MEDIAN={1}", round_me, median);

				//IMPLEMENTATION NOTES
				/*
				 * result.Count == # OF U COLUMN VALID RESULTS
				 * Console.WriteLine("MEDIAN={0} -- {1} [POS={2}]", result[pos], result[pos + 1], pos); == HELPFUL FOR DEBUGGING
				*/

				resultText = Convert.ToString(round_me);
			}

            //CLEAR HASH TABLES
            bestscan.Clear();
            fragscannumber.Clear();
            fwhminscans.Clear();
            optimalpeakapexscannumber.Clear();
            scantime.Clear();
            result.Clear();

            //RETURN RESULT
			return resultText;
        }

        /// <summary>
		/// C-4A: Median peak width during first 10% of separation
        /// </summary>
        /// <returns></returns>
        public String C_4A()
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
            String prv_Charge = "";                                         //INIT PREV CHARGE TO BLANK [REQUIRED FOR COMPARISON]
            String prv_Peptide_Sequence = "";                               //INIT PREV PEPTIDE SEQUENCE TO BLANK [REQUIRED FOR COMPARISON]
            String prev_Best_Evalue = "";                                   //INIT PREV BEST EVALUE TO BLANK [REQUIRED FOR COMPARISON]
            double median = 0.00;                                           //INIT MEDIAN
            double START_RANGE = 0.00;                                      //FUNCTION START RANGE [REQUIRED + SET BY DEFINED MEASUREMENTS]
            double END_RANGE = 0.10;                                        //FUNCTION END RANGE [REQUIRED + SET BY DEFINED MEASUREMENTS]

            //SET DB QUERY [REQUIRED TO SORT BY PEPTIDE SEQUENCE]
            DBInterface.setQuery("SELECT Scan, Charge, Peptide_Expectation_Value_Log,Peptide_Sequence FROM `temp_xt` "
            + "WHERE temp_xt.random_id=" + r_id + " ORDER BY Peptide_Sequence,Charge,Scan,Scan");

            //DECLARE FIELDS TO READ FROM
            String[] fields1 = { "Scan", "Charge", "Peptide_Expectation_Value_Log", "Peptide_Sequence" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS Q,R NOW

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields1, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //FIND COLUMN Q
                String Best_Evalue = "";

                //IF PREVIOUS PEPTIDE SEQUENCES == EACH OTHER && PREVIOUS CHARGES == EACH OTHER
                if (prv_Peptide_Sequence.Equals(Convert.ToString(measurementhash["Peptide_Sequence"])) && prv_Charge.Equals(Convert.ToString(measurementhash["Charge"])))
                {

                    //TAKE MIN [EITHER PREVIOUS BEST EVALUE OR CURRENT PEPTIDE EXPECTATION VALUE
                    if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) > Convert.ToDouble(prev_Best_Evalue))
                    {
                        Best_Evalue = prev_Best_Evalue;
                    }
                    else
                    {
                        Best_Evalue = Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"]);
                    }
                }
                else
                {
                    Best_Evalue = Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"]);
                }

                //NOW FIND COLUMN R IF COLUMN U IS == TRUE
                if (Best_Evalue.Equals(Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"])))
                {
                    //WE ARE NOW == TRUE FOR THIS COLUMN ... AS TRUE WE ADD THIS TO OUR HASH TABLE/WHATEVER

                    //[ADD HERE]
                    bestscan.Add(Convert.ToInt32(measurementhash["Scan"]));
                }

                //UPDATE PREVIOUS VALUES FOR NEXT LOOP
                prv_Charge = Convert.ToString(measurementhash["Charge"]);
                prv_Peptide_Sequence = Convert.ToString(measurementhash["Peptide_Sequence"]);
                prev_Best_Evalue = Best_Evalue;
            }

            //NOW SORT OUR VALUES SO THEY ARE IN THE CORRECT ORDER-
            bestscan.Sort();

            //SET DB QUERY
            DBInterface.setQuery("SELECT FragScanNumber, FWHMInScans, OptimalPeakApexScanNumber FROM `temp_sicstats` WHERE temp_sicstats.random_id=" + r_id + "");

            //DECLARE FIELDS TO READ FROM
            String[] fields2 = { "FragScanNumber", "FWHMInScans", "OptimalPeakApexScanNumber" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS D-F

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields2, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //ADD VALUES TO OUR TEMP HASH TABLES
                fragscannumber.Add(measurementhash["FragScanNumber"], measurementhash["FragScanNumber"]);
                fwhminscans.Add(measurementhash["FragScanNumber"], measurementhash["FWHMInScans"]);
                optimalpeakapexscannumber.Add(measurementhash["FragScanNumber"], measurementhash["OptimalPeakApexScanNumber"]);
            }

            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_scanstats.ScanNumber, temp_scanstats.ScanTime FROM `temp_scanstats` WHERE temp_scanstats.random_id=" + r_id + "");

            //DECLARE FIELDS TO READ FROM
            String[] fields3 = { "ScanNumber", "ScanTime" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS H-I
            i = 1;
            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields3, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //ADD TO SCANTIME HASH TABLE
                scantime.Add(Convert.ToString(i), measurementhash["ScanTime"]);

                //INCREMENT I POSITION
                i++;
            }

            //NOW START THE ACTUAL MEASUREMENT CALCULATION

            //LOOP THROUGH BESTSCAN
            for (i = 0; i < bestscan.Count; i++)
            {
                //FIND INDEX + OPTIMAL PEAK APEX SCAN +- FWHMIN FOR EACH RESULT [COLUMNS: M,O]
                String index = Convert.ToString(bestscan[i]);
                int OptimalPeakApexScanMinusFWHMIN = Convert.ToInt32(optimalpeakapexscannumber[index]) - Convert.ToInt32(Math.Ceiling(Convert.ToDouble(fwhminscans[index]) / 2));
                int OptimalPeakApexScanPlusFWHMIN = Convert.ToInt32(optimalpeakapexscannumber[index]) + Convert.ToInt32(Math.Ceiling(Convert.ToDouble(fwhminscans[index]) / 2));

                //FIND OTHER COLUMNS [N,P, Q,R,T]
                double start_time = Convert.ToDouble(scantime[Convert.ToString(OptimalPeakApexScanMinusFWHMIN)]);
                double end_time = Convert.ToDouble(scantime[Convert.ToString(OptimalPeakApexScanPlusFWHMIN)]);
                double end_minus_start = end_time - start_time;
                double end_minus_start_in_secs = end_minus_start * 60;
                double running_percent = Convert.ToDouble(running_sum) / Convert.ToDouble(bestscan.Count);

                //CHECK FOR VALID RANGE DATA THEN ADD TO OUR RESULTS
                if (running_percent >= START_RANGE && running_percent <= END_RANGE)
                {
                    //WE ARE WITHING OUR VALID RANGE ... SO ADD end_minus_start_in_secs TO OUR LIST
                    result.Add(end_minus_start_in_secs);
                }

                //INCREMENT RUNNING SUM [COLUMN S]
                running_sum++;
            }

			string resultText = string.Empty;

			if (result.Count > 0)
			{
				//CALCULATE MEDIAN
				result.Sort();                          //START BY SORTING

				//IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
				if (result.Count % 2 == 1)
				{
					//IF ODD
					int pos = (result.Count / 2);
					median = result[result.Count / 2];
				}
				else
				{
					//IF EVEN
					int pos = (result.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
					median = (result[pos] + result[pos + 1]) / 2;
				}

				//WE NOW HAVE RESULT ... NOW ROUND IT TO 2ND DIGIT
				decimal round_me = Convert.ToDecimal(median);
				round_me = Math.Round(round_me, 2);                        //ROUND MEDIAN


				//Console.WriteLine("RESULT={0} -- MEDIAN={1}", round_me, median);

				//IMPLEMENTATION NOTES
				/*
				 * result.Count == # OF U COLUMN VALID RESULTS
				 * Console.WriteLine("MEDIAN={0} -- {1} [POS={2}]", result[pos], result[pos + 1], pos); == HELPFUL FOR DEBUGGING
				*/

				resultText = Convert.ToString(round_me);
			}

            //CLEAR HASH TABLES
            bestscan.Clear();
            fragscannumber.Clear();
            fwhminscans.Clear();
            optimalpeakapexscannumber.Clear();
            scantime.Clear();
            result.Clear();

            //RETURN RESULT
			return resultText;
        }

        /// <summary>
		/// C-4B: Median peak width during last 10% of separation
        /// </summary>
        /// <returns></returns>
        public String C_4B()
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
            String prv_Charge = "";                                         //INIT PREV CHARGE TO BLANK [REQUIRED FOR COMPARISON]
            String prv_Peptide_Sequence = "";                               //INIT PREV PEPTIDE SEQUENCE TO BLANK [REQUIRED FOR COMPARISON]
            String prev_Best_Evalue = "";                                   //INIT PREV BEST EVALUE TO BLANK [REQUIRED FOR COMPARISON]
            double median = 0.00;                                           //INIT MEDIAN
            double START_RANGE = 0.90;                                      //FUNCTION START RANGE [REQUIRED + SET BY DEFINED MEASUREMENTS]
            double END_RANGE = 1.00;                                        //FUNCTION END RANGE [REQUIRED + SET BY DEFINED MEASUREMENTS]

            //SET DB QUERY [REQUIRED TO SORT BY PEPTIDE SEQUENCE]
            DBInterface.setQuery("SELECT Scan, Charge, Peptide_Expectation_Value_Log,Peptide_Sequence FROM `temp_xt` "
            + "WHERE temp_xt.random_id=" + r_id + " ORDER BY Peptide_Sequence,Charge,Scan,Scan");

            //DECLARE FIELDS TO READ FROM
            String[] fields1 = { "Scan", "Charge", "Peptide_Expectation_Value_Log", "Peptide_Sequence" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS Q,R NOW

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields1, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //FIND COLUMN Q
                String Best_Evalue = "";

                //IF PREVIOUS PEPTIDE SEQUENCES == EACH OTHER && PREVIOUS CHARGES == EACH OTHER
                if (prv_Peptide_Sequence.Equals(Convert.ToString(measurementhash["Peptide_Sequence"])) && prv_Charge.Equals(Convert.ToString(measurementhash["Charge"])))
                {

                    //TAKE MIN [EITHER PREVIOUS BEST EVALUE OR CURRENT PEPTIDE EXPECTATION VALUE
                    if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) > Convert.ToDouble(prev_Best_Evalue))
                    {
                        Best_Evalue = prev_Best_Evalue;
                    }
                    else
                    {
                        Best_Evalue = Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"]);
                    }
                }
                else
                {
                    Best_Evalue = Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"]);
                }

                //NOW FIND COLUMN R IF COLUMN U IS == TRUE
                if (Best_Evalue.Equals(Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"])))
                {
                    //WE ARE NOW == TRUE FOR THIS COLUMN ... AS TRUE WE ADD THIS TO OUR HASH TABLE/WHATEVER

                    //[ADD HERE]
                    bestscan.Add(Convert.ToInt32(measurementhash["Scan"]));
                }

                //UPDATE PREVIOUS VALUES FOR NEXT LOOP
                prv_Charge = Convert.ToString(measurementhash["Charge"]);
                prv_Peptide_Sequence = Convert.ToString(measurementhash["Peptide_Sequence"]);
                prev_Best_Evalue = Best_Evalue;
            }

            //NOW SORT OUR VALUES SO THEY ARE IN THE CORRECT ORDER-
            bestscan.Sort();

            //SET DB QUERY
            DBInterface.setQuery("SELECT FragScanNumber, FWHMInScans, OptimalPeakApexScanNumber FROM `temp_sicstats` WHERE temp_sicstats.random_id=" + r_id + "");

            //DECLARE FIELDS TO READ FROM
            String[] fields2 = { "FragScanNumber", "FWHMInScans", "OptimalPeakApexScanNumber" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS D-F

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields2, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //ADD VALUES TO OUR TEMP HASH TABLES
                fragscannumber.Add(measurementhash["FragScanNumber"], measurementhash["FragScanNumber"]);
                fwhminscans.Add(measurementhash["FragScanNumber"], measurementhash["FWHMInScans"]);
                optimalpeakapexscannumber.Add(measurementhash["FragScanNumber"], measurementhash["OptimalPeakApexScanNumber"]);
            }

            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_scanstats.ScanNumber, temp_scanstats.ScanTime FROM `temp_scanstats` WHERE temp_scanstats.random_id=" + r_id + "");

            //DECLARE FIELDS TO READ FROM
            String[] fields3 = { "ScanNumber", "ScanTime" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS H-I
            i = 1;
            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields3, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //ADD TO SCANTIME HASH TABLE
                scantime.Add(Convert.ToString(i), measurementhash["ScanTime"]);

                //INCREMENT I POSITION
                i++;
            }

            //NOW START THE ACTUAL MEASUREMENT CALCULATION

            //LOOP THROUGH BESTSCAN
            for (i = 0; i < bestscan.Count; i++)
            {
                //FIND INDEX + OPTIMAL PEAK APEX SCAN +- FWHMIN FOR EACH RESULT [COLUMNS: M,O]
                String index = Convert.ToString(bestscan[i]);
                int OptimalPeakApexScanMinusFWHMIN = Convert.ToInt32(optimalpeakapexscannumber[index]) - Convert.ToInt32(Math.Ceiling(Convert.ToDouble(fwhminscans[index]) / 2));
                int OptimalPeakApexScanPlusFWHMIN = Convert.ToInt32(optimalpeakapexscannumber[index]) + Convert.ToInt32(Math.Ceiling(Convert.ToDouble(fwhminscans[index]) / 2));

                //FIND OTHER COLUMNS [N,P, Q,R,T]
                double start_time = Convert.ToDouble(scantime[Convert.ToString(OptimalPeakApexScanMinusFWHMIN)]);
                double end_time = Convert.ToDouble(scantime[Convert.ToString(OptimalPeakApexScanPlusFWHMIN)]);
                double end_minus_start = end_time - start_time;
                double end_minus_start_in_secs = end_minus_start * 60;
                double running_percent = Convert.ToDouble(running_sum) / Convert.ToDouble(bestscan.Count);

                //CHECK FOR VALID RANGE DATA THEN ADD TO OUR RESULTS
                if (running_percent >= START_RANGE && running_percent <= END_RANGE)
                {
                    //WE ARE WITHING OUR VALID RANGE ... SO ADD end_minus_start_in_secs TO OUR LIST
                    result.Add(end_minus_start_in_secs);
                }

                //INCREMENT RUNNING SUM [COLUMN S]
                running_sum++;
            }

			string resultText = string.Empty;

			if (result.Count > 0)
			{
				//CALCULATE MEDIAN
				result.Sort();                          //START BY SORTING

				//IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
				if (result.Count % 2 == 1)
				{
					//IF ODD
					int pos = (result.Count / 2);
					median = result[result.Count / 2];
				}
				else
				{
					//IF EVEN
					int pos = (result.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
					median = (result[pos] + result[pos + 1]) / 2;
				}

				//WE NOW HAVE RESULT ... NOW ROUND IT TO 2ND DIGIT
				decimal round_me = Convert.ToDecimal(median);
				round_me = Math.Round(round_me, 2);                        //ROUND MEDIAN


				//Console.WriteLine("RESULT={0} -- MEDIAN={1}", round_me, median);

				//IMPLEMENTATION NOTES
				/*
				 * result.Count == # OF U COLUMN VALID RESULTS
				 * Console.WriteLine("MEDIAN={0} -- {1} [POS={2}]", result[pos], result[pos + 1], pos); == HELPFUL FOR DEBUGGING
				*/
				
				resultText = Convert.ToString(round_me);
			}

            //CLEAR HASH TABLES
            bestscan.Clear();
            fragscannumber.Clear();
            fwhminscans.Clear();
            optimalpeakapexscannumber.Clear();
            scantime.Clear();
            result.Clear();

            //RETURN RESULT
            return resultText;
        }

        /// <summary>
		/// C-4C: Median peak width during middle 10% of separation
        /// </summary>
        /// <returns></returns>
        public String C_4C()
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
            String prv_Charge = "";                                         //INIT PREV CHARGE TO BLANK [REQUIRED FOR COMPARISON]
            String prv_Peptide_Sequence = "";                               //INIT PREV PEPTIDE SEQUENCE TO BLANK [REQUIRED FOR COMPARISON]
            String prev_Best_Evalue = "";                                   //INIT PREV BEST EVALUE TO BLANK [REQUIRED FOR COMPARISON]
            double median = 0.00;                                           //INIT MEDIAN
            double START_RANGE = 0.45;                                      //FUNCTION START RANGE [REQUIRED + SET BY DEFINED MEASUREMENTS]
            double END_RANGE = 0.55;                                        //FUNCTION END RANGE [REQUIRED + SET BY DEFINED MEASUREMENTS]

            //SET DB QUERY [REQUIRED TO SORT BY PEPTIDE SEQUENCE]
            DBInterface.setQuery("SELECT Scan, Charge, Peptide_Expectation_Value_Log,Peptide_Sequence FROM `temp_xt` "
            + "WHERE temp_xt.random_id=" + r_id + " ORDER BY Peptide_Sequence,Charge,Scan,Scan");

            //DECLARE FIELDS TO READ FROM
            String[] fields1 = { "Scan", "Charge", "Peptide_Expectation_Value_Log", "Peptide_Sequence" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS Q,R NOW

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields1, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //FIND COLUMN Q
                String Best_Evalue = "";

                //IF PREVIOUS PEPTIDE SEQUENCES == EACH OTHER && PREVIOUS CHARGES == EACH OTHER
                if (prv_Peptide_Sequence.Equals(Convert.ToString(measurementhash["Peptide_Sequence"])) && prv_Charge.Equals(Convert.ToString(measurementhash["Charge"])))
                {

                    //TAKE MIN [EITHER PREVIOUS BEST EVALUE OR CURRENT PEPTIDE EXPECTATION VALUE
                    if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) > Convert.ToDouble(prev_Best_Evalue))
                    {
                        Best_Evalue = prev_Best_Evalue;
                    }
                    else
                    {
                        Best_Evalue = Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"]);
                    }
                }
                else
                {
                    Best_Evalue = Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"]);
                }

                //NOW FIND COLUMN R IF COLUMN U IS == TRUE
                if (Best_Evalue.Equals(Convert.ToString(measurementhash["Peptide_Expectation_Value_Log"])))
                {
                    //WE ARE NOW == TRUE FOR THIS COLUMN ... AS TRUE WE ADD THIS TO OUR HASH TABLE/WHATEVER

                    //[ADD HERE]
                    bestscan.Add(Convert.ToInt32(measurementhash["Scan"]));
                }

                //UPDATE PREVIOUS VALUES FOR NEXT LOOP
                prv_Charge = Convert.ToString(measurementhash["Charge"]);
                prv_Peptide_Sequence = Convert.ToString(measurementhash["Peptide_Sequence"]);
                prev_Best_Evalue = Best_Evalue;
            }

            //NOW SORT OUR VALUES SO THEY ARE IN THE CORRECT ORDER-
            bestscan.Sort();

            //SET DB QUERY
            DBInterface.setQuery("SELECT FragScanNumber, FWHMInScans, OptimalPeakApexScanNumber FROM `temp_sicstats` WHERE temp_sicstats.random_id=" + r_id + "");

            //DECLARE FIELDS TO READ FROM
            String[] fields2 = { "FragScanNumber", "FWHMInScans", "OptimalPeakApexScanNumber" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS D-F

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields2, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //ADD VALUES TO OUR TEMP HASH TABLES
                fragscannumber.Add(measurementhash["FragScanNumber"], measurementhash["FragScanNumber"]);
                fwhminscans.Add(measurementhash["FragScanNumber"], measurementhash["FWHMInScans"]);
                optimalpeakapexscannumber.Add(measurementhash["FragScanNumber"], measurementhash["OptimalPeakApexScanNumber"]);
            }

            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_scanstats.ScanNumber, temp_scanstats.ScanTime FROM `temp_scanstats` WHERE temp_scanstats.random_id=" + r_id + "");

            //DECLARE FIELDS TO READ FROM
            String[] fields3 = { "ScanNumber", "ScanTime" };

            //INIT READER
            DBInterface.initReader();

            //FETCH COLUMNS H-I
            i = 1;
            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields3, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //ADD TO SCANTIME HASH TABLE
                scantime.Add(Convert.ToString(i), measurementhash["ScanTime"]);

                //INCREMENT I POSITION
                i++;
            }

            //NOW START THE ACTUAL MEASUREMENT CALCULATION

            //LOOP THROUGH BESTSCAN
            for (i = 0; i < bestscan.Count; i++)
            {
                //FIND INDEX + OPTIMAL PEAK APEX SCAN +- FWHMIN FOR EACH RESULT [COLUMNS: M,O]
                String index = Convert.ToString(bestscan[i]);
                int OptimalPeakApexScanMinusFWHMIN = Convert.ToInt32(optimalpeakapexscannumber[index]) - Convert.ToInt32(Math.Ceiling(Convert.ToDouble(fwhminscans[index]) / 2));
                int OptimalPeakApexScanPlusFWHMIN = Convert.ToInt32(optimalpeakapexscannumber[index]) + Convert.ToInt32(Math.Ceiling(Convert.ToDouble(fwhminscans[index]) / 2));

                //FIND OTHER COLUMNS [N,P, Q,R,T]
                double start_time = Convert.ToDouble(scantime[Convert.ToString(OptimalPeakApexScanMinusFWHMIN)]);
                double end_time = Convert.ToDouble(scantime[Convert.ToString(OptimalPeakApexScanPlusFWHMIN)]);
                double end_minus_start = end_time - start_time;
                double end_minus_start_in_secs = end_minus_start * 60;
                double running_percent = Convert.ToDouble(running_sum) / Convert.ToDouble(bestscan.Count);

                //CHECK FOR VALID RANGE DATA THEN ADD TO OUR RESULTS
                if (running_percent >= START_RANGE && running_percent <= END_RANGE)
                {
                    //WE ARE WITHING OUR VALID RANGE ... SO ADD end_minus_start_in_secs TO OUR LIST
                    result.Add(end_minus_start_in_secs);
                }

                //INCREMENT RUNNING SUM [COLUMN S]
                running_sum++;
            }

			string resultText = string.Empty;

			if (result.Count > 0)
			{
				//CALCULATE MEDIAN
				result.Sort();                          //START BY SORTING

				//IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
				if (result.Count % 2 == 1)
				{
					//IF ODD
					int pos = (result.Count / 2);
					median = result[result.Count / 2];
				}
				else
				{
					//IF EVEN
					int pos = (result.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
					median = (result[pos] + result[pos + 1]) / 2;
				}

				//WE NOW HAVE RESULT ... NOW ROUND IT TO 2ND DIGIT
				decimal round_me = Convert.ToDecimal(median);
				round_me = Math.Round(round_me, 2);                        //ROUND MEDIAN


				//Console.WriteLine("RESULT={0} -- MEDIAN={1}", round_me, median);

				//IMPLEMENTATION NOTES
				/*
				 * result.Count == # OF U COLUMN VALID RESULTS
				 * Console.WriteLine("MEDIAN={0} -- {1} [POS={2}]", result[pos], result[pos + 1], pos); == HELPFUL FOR DEBUGGING
				*/

				resultText = Convert.ToString(round_me);
			}

            //CLEAR HASH TABLES
            bestscan.Clear();
            fragscannumber.Clear();
            fwhminscans.Clear();
            optimalpeakapexscannumber.Clear();
            scantime.Clear();
            result.Clear();

            //RETURN RESULT
			return resultText;
        }

        /// <summary>
		/// DS-1A: Count of peptides with one spectrum / count of peptides with two spectra
        /// </summary>
        /// <returns></returns>
        public String DS_1A()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT Peptide_Expectation_Value_Log,Peptide_Sequence,Scan "
                + "FROM `temp_xt` "
                + "WHERE temp_xt.random_id=" + r_id + " "
                + "ORDER BY Peptide_Sequence,Scan;");

            int running_count = 0;                                                                  //RUNNING COUNT FOR COLUMN F
            int num_of_1_peptides = 0;	                                                            //RUNNING COUNT FOR COLUMN J
            int num_of_2_peptides = 0;                                                              //RUNNING COUNT FOR COLUMN K
            int num_of_3_peptides = 0;                                                              //RUNNING COUNT FOR COLUMN L
            decimal result = 0.00M;                                                                 //SOLUTION
            Boolean FILTER;                                                                         //FILTER STATUS FOR COLUMN E
            int i = 0;	                                                                            //TEMP POSITION
            Hashtable Peptide_Exp_Value_Log = new Hashtable();                                      //STORE Peptide_Expectation_Value_Log NUMBERS
            Hashtable Peptide_Sequence = new Hashtable();                                           //STORE Peptide Sequence NUMBERS
            Hashtable Scan = new Hashtable();                                                       //STORE SCAN NUMBERS
            Hashtable RunningCountTable = new Hashtable();                                          //STORE RUNNING COUNT'S IN A HASH TABLE FOR LATER ACCES
            String prv_Peptide_Sequence = "";                                                       //INIT PREV PEPTIDE SEQUENCE TO BLANK [REQUIRED FOR COMPARISON]
            int prv_running_count = 0;                                                              //INIT PREV RUNNING COUNT TO 0 [REQUIRED FOR COMPARISON]
            String prv_highest_filtered_log = "";                                                   //INIT PREV HIGHEST FILTERED LOG TO BLANK [REQUIRED FOR COMPARISON]
            int prv_peptide_count = 1;                                                              //INIT PREV PEPTIDE_COUNT TO BE 1

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Peptide_Expectation_Value_Log", "Peptide_Sequence", "Scan" };

            //INIT READER
            DBInterface.initReader();
            
            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //ADD TO HASH TABLES
                Peptide_Exp_Value_Log.Add(i, measurementhash["Peptide_Expectation_Value_Log"]);
                Peptide_Sequence.Add(i, measurementhash["Peptide_Sequence"]);
                Scan.Add(i, measurementhash["Scan"]);

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
            //for (i = 0; i < 1065; i++)
            {
                //RESET FILTER STATUS
                FILTER = false;
                String highest_filtered_log = "";
                String filtered_log = "";
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
                    else if (Convert.ToDouble(prv_highest_filtered_log) > Convert.ToDouble(Peptide_Exp_Value_Log[i]) )
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
                if (current_peptide_count == 1 && !filtered_log.Equals("") )
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

            //NOW CALCULATE DS_1A + ROUND TO 6 PLACES

            //RETURN 0 IF NUM_OF_2 EQUALS 0
            if (Convert.ToDouble(num_of_2_peptides) == 0)
            {
                result = Convert.ToDecimal(0);
            }
            else
            {
                result = Convert.ToDecimal(Convert.ToDouble(num_of_1_peptides) / Convert.ToDouble(num_of_2_peptides));
            }

            result = Math.Round(result, 6);

            //CLEAR HASH TABLES
            Peptide_Exp_Value_Log.Clear();
            Peptide_Sequence.Clear();
            Scan.Clear();
            RunningCountTable.Clear();

            return Convert.ToString(result);
        }

        /// <summary>
		/// DS-1B: Count of peptides with two spectra / count of peptides with three spectra
        /// </summary>
        /// <returns></returns>
        public String DS_1B()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT Peptide_Expectation_Value_Log,Peptide_Sequence,Scan "
                + "FROM `temp_xt` "
                + "WHERE temp_xt.random_id=" + r_id + " "
                + "ORDER BY Peptide_Sequence,Scan;");

            int running_count = 0;                                                                  //RUNNING COUNT FOR COLUMN F
            int num_of_1_peptides = 0;	                                                            //RUNNING COUNT FOR COLUMN J
            int num_of_2_peptides = 0;                                                              //RUNNING COUNT FOR COLUMN K
            int num_of_3_peptides = 0;                                                              //RUNNING COUNT FOR COLUMN L
            decimal result = 0.00M;                                                                 //SOLUTION
            Boolean FILTER;                                                                         //FILTER STATUS FOR COLUMN E
            int i = 0;	                                                                            //TEMP POSITION
            Hashtable Peptide_Exp_Value_Log = new Hashtable();                                      //STORE Peptide_Expectation_Value_Log NUMBERS
            Hashtable Peptide_Sequence = new Hashtable();                                           //STORE Peptide Sequence NUMBERS
            Hashtable Scan = new Hashtable();                                                       //STORE SCAN NUMBERS
            Hashtable RunningCountTable = new Hashtable();                                          //STORE RUNNING COUNT'S IN A HASH TABLE FOR LATER ACCES
            String prv_Peptide_Sequence = "";                                                       //INIT PREV PEPTIDE SEQUENCE TO BLANK [REQUIRED FOR COMPARISON]
            int prv_running_count = 0;                                                              //INIT PREV RUNNING COUNT TO 0 [REQUIRED FOR COMPARISON]
            String prv_highest_filtered_log = "";                                                   //INIT PREV HIGHEST FILTERED LOG TO BLANK [REQUIRED FOR COMPARISON]
            int prv_peptide_count = 1;                                                              //INIT PREV PEPTIDE_COUNT TO BE 1

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Peptide_Expectation_Value_Log", "Peptide_Sequence", "Scan" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //ADD TO HASH TABLES
                Peptide_Exp_Value_Log.Add(i, measurementhash["Peptide_Expectation_Value_Log"]);
                Peptide_Sequence.Add(i, measurementhash["Peptide_Sequence"]);
                Scan.Add(i, measurementhash["Scan"]);

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
            //for (i = 0; i < 1065; i++)
            {
                //RESET FILTER STATUS
                FILTER = false;
                String highest_filtered_log = "";
                String filtered_log = "";
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

            //NOW CALCULATE DS_1A + ROUND TO 6 PLACES

			//RETURN 0 IF NUM_OF_3 EQUALS 0
            if (Convert.ToDouble(num_of_3_peptides) == 0)
            {
                result = Convert.ToDecimal(0);
            }
            else
            {
                result = Convert.ToDecimal(Convert.ToDouble(num_of_2_peptides) / Convert.ToDouble(num_of_3_peptides));
            }
            result = Math.Round(result, 6);

            //CLEAR HASH TABLES
            Peptide_Exp_Value_Log.Clear();
            Peptide_Sequence.Clear();
            Scan.Clear();
            RunningCountTable.Clear();

            return Convert.ToString(result);

 
        }

        /// <summary>
		/// DS-2A: Number of MS1 scans taken over middle 50% of separation
        /// </summary>
        /// <returns></returns>
        public String DS_2A()
        {
			int msLevel = 1;
			return DS_2_Shared(msLevel).ToString();
        }

        /// <summary>
		/// DS-2B: Number of MS2 scans taken over middle 50% of separation
        /// </summary>
        /// <returns></returns>
        public String DS_2B()
        {
			int msLevel = 2;
			return DS_2_Shared(msLevel).ToString();
        }

		protected int DS_2_Shared(int msLevel)
		{
			//SET DB QUERY
			DBInterface.setQuery("SELECT ScanNumber, ScanType "
				+ "FROM `temp_scanstats` "
				+ "WHERE temp_scanstats.random_id=" + r_id + " "
				+ "ORDER BY ScanNumber;");

			//DECLARE VARIABLES
			int valC2ARegionScanStart = GetStoredValueInt("C_2A_REGION_SCAN_START", 0);
			int valC2ARegionScanEnd = GetStoredValueInt("C_2A_REGION_SCAN_END", 0);
			
			int intScanCount = 0;
			int scanNumber;
			int scanType;

			//DECLARE FIELDS TO READ FROM
			String[] fields = { "ScanNumber", "ScanType" };

			//INIT READER
			DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
			{
				scanNumber = Convert.ToInt32(measurementhash["ScanNumber"]);
				scanType = Convert.ToInt32(measurementhash["ScanType"]);

				//IF IS WITHIN RANGE
				if (scanType == msLevel && scanNumber >= valC2ARegionScanStart && scanNumber <= valC2ARegionScanEnd)
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
        public String IS_2()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT Scan,Peptide_Expectation_Value_Log,Peptide_MH,Charge "
                + "FROM `temp_xt` "
                + "WHERE temp_xt.random_id=" + r_id + " "
                + ";");

            //DECLARE VARIABLES
            double MINUS_CONSTANT = 1.00727649;                                                 //REQUIRED CONSTANT TO SUBTRACT BY
            List<double> MZ_List = new List<double>();                                          //MZ LIST
            List<double> MZ_Final;// = new List<double>();                                      //MZ Final List
            Dictionary<double, int> tempDict = new Dictionary<double, int>();                   //TEMP ... TO REMOVE DUPLICATES
            double median = 0.00;                                                               //STORE MEDIAN

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Peptide_MH", "Charge" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //IF PEPTIDE EXP VALUE LOG <= -2 SET FILTER TO TRUE
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //COMPUTE MZ VALUE
                    double temp_mz = (Convert.ToDouble(measurementhash["Peptide_MH"])-MINUS_CONSTANT)/(Convert.ToDouble(measurementhash["Charge"]) );

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
            MZ_Final.Sort();

            //IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
            if (MZ_Final.Count % 2 == 1)
            {
                //IF ODD
                int pos = (MZ_Final.Count / 2);
                median = MZ_Final[MZ_Final.Count / 2];
            }
            else
            {
                //IF EVEN
                int pos = (MZ_Final.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
                median = (MZ_Final[pos] + MZ_Final[pos + 1]) / 2;
            }

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 4TH DIGIT
            decimal round_me = Convert.ToDecimal(median);
            round_me = Math.Round(round_me, 4);                        //ROUND MEDIAN

            //CLEAR HASH TABLES
            MZ_List.Clear();
            MZ_Final.Clear();
            tempDict.Clear();

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// IS-3A: Count of 1+ peptides / count of 2+ peptides
        /// </summary>
        /// <returns></returns>
        public String IS_3A()
        {
			//DECLARE VARIABLES
			int count_ones = 0;                                                             //TOTAL # OF 1's
			int count_twos = 0;                                                             //TOTAL # OF 2's
			int count_threes = 0;                                                           //TOTAL # OF 3's
			int count_fours = 0;                                                            //TOTAL # OF 4's
			decimal result = 0.00M;                                                         //RESULT OF MEASUREMENT

			IS3_Shared(out count_ones, out count_twos, out count_threes, out count_fours);

            //CALC MEASUREMENT
			if (count_twos > 0)
				result = Convert.ToDecimal(count_ones) / Convert.ToDecimal(count_twos);

            //ROUND
            result = Math.Round(result, 6);

            return Convert.ToString(result);
        }

        /// <summary>
		/// IS-3B: Count of 3+ peptides / count of 2+ peptides
        /// </summary>
        /// <returns></returns>
        public String IS_3B()
        {
			//DECLARE VARIABLES
			int count_ones = 0;                                                             //TOTAL # OF 1's
			int count_twos = 0;                                                             //TOTAL # OF 2's
			int count_threes = 0;                                                           //TOTAL # OF 3's
			int count_fours = 0;                                                            //TOTAL # OF 4's
			decimal result = 0.00M;                                                         //RESULT OF MEASUREMENT

			IS3_Shared(out count_ones, out count_twos, out count_threes, out count_fours);

            //CALC MEASUREMENT
			if (count_twos > 0)
				result = Convert.ToDecimal(count_threes) / Convert.ToDecimal(count_twos);

            //ROUND
            result = Math.Round(result, 6);

            return Convert.ToString(result);
        }

        /// <summary>
		/// IS-3C: Count of 4+ peptides / count of 2+ peptides
        /// </summary>
        /// <returns></returns>
        public String IS_3C()
        {
            //DECLARE VARIABLES
            int count_ones = 0;                                                             //TOTAL # OF 1's
            int count_twos = 0;                                                             //TOTAL # OF 2's
            int count_threes = 0;                                                           //TOTAL # OF 3's
            int count_fours = 0;                                                            //TOTAL # OF 4's
            decimal result = 0.00M;                                                         //RESULT OF MEASUREMENT

			IS3_Shared(out count_ones, out count_twos, out count_threes, out count_fours);

            //CALC MEASUREMENT
			if (count_twos > 0)
				result = Convert.ToDecimal(count_fours) / Convert.ToDecimal(count_twos);

            //ROUND
            result = Math.Round(result, 6);

            return Convert.ToString(result);
        }

		protected void IS3_Shared(out int count_ones, out int count_twos, out int count_threes, out int count_fours)
		{
			//SET DB QUERY
			DBInterface.setQuery("SELECT Scan,Peptide_Expectation_Value_Log,Charge "
				+ "FROM `temp_xt` "
				+ "WHERE temp_xt.random_id=" + r_id + " "
				+ "Order by Scan;");

			//DECLARE VARIABLES
			count_ones = 0;                                                             //TOTAL # OF 1's
			count_twos = 0;                                                             //TOTAL # OF 2's
			count_threes = 0;                                                           //TOTAL # OF 3's
			count_fours = 0;                                                            //TOTAL # OF 4's

			//DECLARE FIELDS TO READ FROM
			String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Charge" };

			//INIT READER
			DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
			{
				//IF PEPTIDE EXP VALUE LOG <= -2 SET FILTER TO TRUE
				if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					//CONVERT CHARGE TO INT FOR SWITCH()
					int charge = Convert.ToInt32(measurementhash["Charge"]);

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
        public String MS1_1()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_scanstats.ScanNumber, temp_scanstats.ScanType, temp_scanstatsex.Ion_Injection_Time "
                + "FROM `temp_scanstats`, `temp_scanstatsex` "
                + "WHERE temp_scanstats.ScanNumber=temp_scanstatsex.ScanNumber "
                + "AND temp_scanstatsex.random_id=" + r_id + " "
                + "AND temp_scanstats.random_id=" + r_id + " "
                + "ORDER BY temp_scanstats.ScanNumber;");

            //DECLARE VARIABLES
            List<double> Filter = new List<double>();                               //FILTER LIST
            double median = 0.00;                                                   //RESULT

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "ScanNumber", "ScanType", "Ion_Injection_Time" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //SCAN TYPE == 1
                if (Convert.ToDouble(measurementhash["ScanType"]) == 1)
                {
                    //ADD TO FILTER LIST
                    Filter.Add(Convert.ToDouble(measurementhash["Ion_Injection_Time"]));
                }
            }

			if (Filter.Count > 0)
			{
				//SORT LIST
				Filter.Sort();

				//IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
				if (Filter.Count % 2 == 1)
				{
					//IF ODD
					int pos = (Filter.Count / 2);
					median = Filter[Filter.Count / 2];
				}
				else
				{
					//IF EVEN
					int pos = (Filter.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
					median = (Filter[pos] + Filter[pos + 1]) / 2;
				}
			}

            return Convert.ToString(median);
        }

        /// <summary>
		/// MS1_2A: Median S/N value for MS1 spectra from run start through middle 50% of separation
        /// </summary>
        /// <returns></returns>
        public String MS1_2A()
        {
            //DECLARE VARIABLES
			List<double> List_BPSTNR_C_2A;
			List<double> List_TII_C_2A;
			double median = 0.00;                                               //RESULT

			MS1_2_Shared(out List_BPSTNR_C_2A, out List_TII_C_2A);

			if (List_BPSTNR_C_2A.Count > 0)
			{
				//CALC MEDIAN OF COLUMN J
				//IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NO NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
				if (List_BPSTNR_C_2A.Count % 2 == 1)
				{
					//IF ODD
					int pos = (List_BPSTNR_C_2A.Count / 2);
					median = List_BPSTNR_C_2A[List_BPSTNR_C_2A.Count / 2];
				}
				else
				{
					//IF EVEN
					int pos = (List_BPSTNR_C_2A.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
					median = (List_BPSTNR_C_2A[pos] + List_BPSTNR_C_2A[pos + 1]) / 2;
				}
			}

            return Convert.ToString(median);
        }

        /// <summary>
		/// MS1_2B: Median TIC value for identified peptides from run start through middle 50% of separation
        /// </summary>
        /// <returns></returns>
        public String MS1_2B()
        {
			//DECLARE VARIABLES
			List<double> List_BPSTNR_C_2A;
			List<double> List_TII_C_2A;
            double median = 0.00;                                               //RESULT

			MS1_2_Shared(out List_BPSTNR_C_2A, out List_TII_C_2A);

			if (List_TII_C_2A.Count > 0)
			{
				//CALC MEDIAN OF COLUMN K
				//IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NO NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
				if (List_TII_C_2A.Count % 2 == 1)
				{
					//IF ODD
					int pos = (List_TII_C_2A.Count / 2);
					median = List_TII_C_2A[List_TII_C_2A.Count / 2];
				}
				else
				{
					//IF EVEN
					int pos = (List_TII_C_2A.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
					median = (List_TII_C_2A[pos] + List_TII_C_2A[pos + 1]) / 2;
				}
			}

            //DIVIDE BY 1000
            median = median / 1000;

            return Convert.ToString(median);
        }

		protected void MS1_2_Shared(out List<double> List_BPSTNR_C_2A, out List<double> List_TII_C_2A)
		{
			//SET DB QUERY
			DBInterface.setQuery("SELECT ScanNumber, ScanType, BasePeakSignalToNoiseRatio, TotalIonIntensity "
				+ "FROM `temp_scanstats` "
				+ "WHERE temp_scanstats.random_id=" + r_id + " "
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
			int i = 0;                                                          //COUNTER

			//DECLARE FIELDS TO READ FROM
			String[] fields = { "ScanNumber", "ScanType", "BasePeakSignalToNoiseRatio", "TotalIonIntensity" };

			//INIT READER
			DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
			{
				//ADD TO HASH TABLES
				List_ScanNumber.Add(Convert.ToInt32(measurementhash["ScanNumber"]));
				List_ScanType.Add(Convert.ToInt32(measurementhash["ScanType"]));
				List_BasePeakSignalToNoiseRatio.Add(Convert.ToDouble(measurementhash["BasePeakSignalToNoiseRatio"]));
				List_TotalIonIntensity.Add(Convert.ToDouble(measurementhash["TotalIonIntensity"]));

				//CLEAR HASH TABLE
				measurementhash.Clear();

				//INC
				i++;
			}

			int max_scannumber = List_ScanNumber.Max();

			int valScanFirstPeptide = GetStoredValueInt("SCAN_FIRST_FILTER_PASSING_PEPTIDE", 0);
			int valC2ARegionScanEnd = GetStoredValueInt("C_2A_REGION_SCAN_END", 0);

			//LOOP THROUGH ALL
			for (i = 0; i < List_ScanNumber.Count; i++)
			{				
				//SCAN TYPE == 1 && List_ScanNumber[i]>=STORAGE["SCAN_FIRST_FILTER_PASSING_PEPTIDE"] && List_ScanNumber[i]<=STORAGE["C_2A_REGION_SCAN_END"]
				if ((List_ScanType[i] == 1) && (List_ScanNumber[i] >= valScanFirstPeptide) && (List_ScanNumber[i] <= valC2ARegionScanEnd))
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
        public String MS1_3A()
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
            decimal round_me = Convert.ToDecimal(final);
            round_me = Math.Round(round_me, 3);                        //ROUND MEDIAN

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// MS1_3B: Median peak apex intensity for all peptides
        /// </summary>
        /// <returns></returns>
        public String MS1_3B()
        {
			double PMI_5PC;
			double PMI_95PC;
			List<double> result;
			double median = 0.00;

			MS1_3_Shared(out PMI_5PC, out PMI_95PC, out result);

			if (result.Count > 0)
			{
				//NOW CALCULATE MEDIAN
				result.Sort();                          //START BY SORTING

				//IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
				if (result.Count % 2 == 1)
				{
					//IF ODD
					int pos = (result.Count / 2);
					median = result[result.Count / 2];
				}
				else
				{
					//IF EVEN
					int pos = (result.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
					median = (result[pos] + result[pos + 1]) / 2;
				}
			}

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 3RD DIGIT
            decimal round_me = Convert.ToDecimal(median);
            round_me = Math.Round(round_me, 3);                        //ROUND MEDIAN			      

            return Convert.ToString(round_me);
        }

		protected void MS1_3_Shared(out double PMI_5PC, out double PMI_95PC, out List<double> result)
		{
			//SET DB QUERY
			DBInterface.setQuery("SELECT Result_ID, FragScanNumber, PeakMaxIntensity, Peptide_Expectation_Value_Log  "
				+ "FROM `temp_sicstats`, `temp_xt` "
				+ "WHERE `temp_sicstats`.FragScanNumber=`temp_xt`.Scan "
				+ "AND temp_sicstats.random_id=" + r_id + " "
				+ "AND temp_xt.random_id=" + r_id + " "
				+ "ORDER BY PeakMaxIntensity, Result_ID DESC;");

			//DECLARE VARIABLES
			result = new List<double>();																		//STORES FILTER LIST [COLUMN D]
			List<double> MPI_list = new List<double>();                                                         //STORES MAX PEAK INTENSITY FOR 5-95%
			List<double> temp_list_mpi = new List<double>();                                                    //STORES PeakMaxIntensity FOR FUTURE CALCULATIONS
			List<int> temp_list_running_sum = new List<int>();                                                  //STORES RUNNING SUM FOR FUTURE CALCULATIONS
			int max_running_sum = 0;                                                                            //STORES THE LARGEST/MAX RUNNING SUM OF COLUMN E


			//DECLARE FIELDS TO READ FROM
			String[] fields = { "Result_ID", "FragScanNumber", "PeakMaxIntensity", "Peptide_Expectation_Value_Log" };

			//INIT READER
			DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
			{
				//IF PEPTIDE EXP VALUE LOG <= -2 SET FILTER TO TRUE
				if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
				{
					//INC TOTAL RUNNING SUM
					max_running_sum++;

					//ADD TO FILTER LIST
					result.Add(Convert.ToDouble(measurementhash["PeakMaxIntensity"]));

					//ADD TO TEMP LIST TO PROCESS LATER AS WE FIRST NEED TO FIND THE MAX RUNNING SUM WHICH IS DONE AT THE END
					temp_list_mpi.Add(Convert.ToDouble(measurementhash["PeakMaxIntensity"]));                   //ADD MPI
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
        public String DS_3A()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT Result_ID, FragScanNumber, ParentIonIntensity, PeakMaxIntensity, Peptide_Expectation_Value_Log  "
                + "FROM `temp_sicstats`, `temp_xt` "
                + "WHERE `temp_sicstats`.FragScanNumber=`temp_xt`.Scan "
                + "AND temp_sicstats.random_id=" + r_id + " "
                + "AND temp_xt.random_id=" + r_id + " "
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
            String[] fields = { "Result_ID", "FragScanNumber", "ParentIonIntensity", "PeakMaxIntensity", "Peptide_Expectation_Value_Log" };

            //INIT READER
            DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //STORE RESULT_ID, SCAN [ALLOWS FOR US TO HAVE DUPLICATES WITHOUT CRASHING]
                Lookup_Table.Add(Convert.ToInt32(measurementhash["Result_ID"]), Convert.ToInt32(measurementhash["FragScanNumber"]));

                //STORE RESULT_ID, VALUE [SO WE CAN THEN SORT BY VALUE]
				parentIonIntensity = Convert.ToDouble(measurementhash["ParentIonIntensity"]);
				if (parentIonIntensity > 0)
					ratioPeakMaxToParentIonIntensity = Convert.ToDouble(measurementhash["PeakMaxIntensity"]) / parentIonIntensity;
				else
					ratioPeakMaxToParentIonIntensity = 0;

				Lookup_Table_KV.Add(Convert.ToInt32(measurementhash["Result_ID"]), ratioPeakMaxToParentIonIntensity);

                //IF PEPTIDE EXP VALUE LOG <= -2 SET FILTER TO TRUE
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //SET FILTER TO 1
                    Filter_Result.Add(Convert.ToInt32(measurementhash["Result_ID"]), 1);

                    //INCREMENT MAX RUNNING SUM
                    max_running_sum++;
                }
                else
                {
                    //SET FILTER TO 1
                    Filter_Result.Add(Convert.ToInt32(measurementhash["Result_ID"]), 0);
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
						if ((Convert.ToDouble(running_sum) / Convert.ToDouble(max_running_sum)) <= 0.5)
						{
							//ADD TO FILTERED LIST FOR COLUMN N
							result_VBPMIPII_Filtered.Add(Convert.ToDouble(Lookup_Table_KV[key]));
						}
					}
                }
            }

			if (result_PMIPII_Filtered.Count > 0)
			{
				//NOW CALCULATE MEDIAN
				result_PMIPII_Filtered.Sort();                          //START BY SORTING

				//IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
				if (result_PMIPII_Filtered.Count % 2 == 1)
				{
					//IF ODD
					int pos = (result_PMIPII_Filtered.Count / 2);
					median = result_PMIPII_Filtered[result_PMIPII_Filtered.Count / 2];
				}
				else
				{
					//IF EVEN
					int pos = (result_PMIPII_Filtered.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
					median = (result_PMIPII_Filtered[pos] + result_PMIPII_Filtered[pos + 1]) / 2;
				}
			}

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 3RD DIGIT
            decimal round_me = Convert.ToDecimal(median);
            round_me = Math.Round(round_me, 3);                        //ROUND MEDIAN

            //CLEAR HASH TABLE
            Lookup_Table.Clear();
            Filter_Result.Clear();
            Lookup_Table_KV.Clear();
            result_PMIPII_Filtered.Clear();
            result_VBPMIPII_Filtered.Clear();

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// DS_3B: Median of MS1 max / MS1 sampled abundance; limit to bottom 50% of peptides by abundance
        /// </summary>
        /// <returns></returns>
        public String DS_3B()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT Result_ID, FragScanNumber, ParentIonIntensity, PeakMaxIntensity, Peptide_Expectation_Value_Log  "
                + "FROM `temp_sicstats`, `temp_xt` "
                + "WHERE `temp_sicstats`.FragScanNumber=`temp_xt`.Scan "
                + "AND temp_sicstats.random_id=" + r_id + " "
                + "AND temp_xt.random_id=" + r_id + " "
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
            String[] fields = { "Result_ID", "FragScanNumber", "ParentIonIntensity", "PeakMaxIntensity", "Peptide_Expectation_Value_Log" };

            //INIT READER
            DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //STORE RESULT_ID, SCAN [ALLOWS FOR US TO HAVE DUPLICATES WITHOUT CRASHING]
                Lookup_Table.Add(Convert.ToInt32(measurementhash["Result_ID"]), Convert.ToInt32(measurementhash["FragScanNumber"]));

                //STORE RESULT_ID, VALUE [SO WE CAN THEN SORT BY VALUE]
				parentIonIntensity = Convert.ToDouble(measurementhash["ParentIonIntensity"]);
				if (parentIonIntensity > 0)
					ratioPeakMaxToParentIonIntensity = Convert.ToDouble(measurementhash["PeakMaxIntensity"]) / parentIonIntensity;
				else
					ratioPeakMaxToParentIonIntensity = 0;

				Lookup_Table_KV.Add(Convert.ToInt32(measurementhash["Result_ID"]), ratioPeakMaxToParentIonIntensity);

                //IF PEPTIDE EXP VALUE LOG <= -2 SET FILTER TO TRUE
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //SET FILTER TO 1
                    Filter_Result.Add(Convert.ToInt32(measurementhash["Result_ID"]), 1);

                    //INCREMENT MAX RUNNING SUM
                    max_running_sum++;
                }
                else
                {
                    //SET FILTER TO 1
                    Filter_Result.Add(Convert.ToInt32(measurementhash["Result_ID"]), 0);
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
						if ((Convert.ToDouble(running_sum) / Convert.ToDouble(max_running_sum)) <= 0.5)
						{
							//ADD TO FILTERED LIST FOR COLUMN N
							result_VBPMIPII_Filtered.Add(Convert.ToDouble(Lookup_Table_KV[key]));
						}
					}
                }
            }

			if (result_VBPMIPII_Filtered.Count > 0)
			{
				//NOW CALCULATE MEDIAN
				result_VBPMIPII_Filtered.Sort();                          //START BY SORTING

				//IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
				if (result_VBPMIPII_Filtered.Count % 2 == 1)
				{
					//IF ODD
					int pos = (result_VBPMIPII_Filtered.Count / 2);
					median = result_VBPMIPII_Filtered[result_VBPMIPII_Filtered.Count / 2];
				}
				else
				{
					//IF EVEN
					int pos = (result_VBPMIPII_Filtered.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
					median = (result_VBPMIPII_Filtered[pos] + result_VBPMIPII_Filtered[pos + 1]) / 2;
				}
			}

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 3RD DIGIT
            decimal round_me = Convert.ToDecimal(median);
            round_me = Math.Round(round_me, 3);                        //ROUND MEDIAN

            //CLEAR HASH TABLE
            Lookup_Table.Clear();
            Filter_Result.Clear();
            Lookup_Table_KV.Clear();
            result_PMIPII_Filtered.Clear();
            result_VBPMIPII_Filtered.Clear();

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// IS_1A: Occurrences of MS1 jumping >10x
        /// </summary>
        /// <returns></returns>
        public String IS_1A()
        {
			int sum_is_1a;
			int sum_is_1b;

			IS_1_Shared(out sum_is_1a, out sum_is_1b);
            return Convert.ToString(sum_is_1a);
        }

        /// <summary>
		/// IS_1B: Occurrences of MS1 falling >10x
        /// </summary>
        /// <returns></returns>
        public String IS_1B()
        {
			int sum_is_1a;
			int sum_is_1b;

			IS_1_Shared(out sum_is_1a, out sum_is_1b);
            return Convert.ToString(sum_is_1b);
        }

		protected void IS_1_Shared(out int sum_is_1a, out int sum_is_1b)
		{
			//SET DB QUERY
			DBInterface.setQuery("SELECT ScanNumber, ScanType, BasePeakIntensity  "
				+ "FROM `temp_scanstats` "
				+ "WHERE temp_scanstats.random_id=" + r_id + " "
				+ ";");

			//DECLARE VARIABLES
			int fold_change_threshold = 10;                                             //CONSTANT DEFINED VALUE
			int prv_ScanType = -1;
			double prv_BasePeakIntensity = -1;
			double prv_Compare_Only_MS1 = -1;
			int intensity_fall;
			int intensity_rise;
			int intensity_fall_ms1;
			int intensity_rise_ms1;
			double Compare_Only_MS1;
			int line_num = 0;                                                           //KEEP TRACK OF LINE_NUM

			sum_is_1a = 0;                                                          //STORE SUM FOR IS_1A
			sum_is_1b = 0;                                                          //STORE SUM FOR IS_1B

			//DECLARE FIELDS TO READ FROM
			String[] fields = { "ScanNumber", "ScanType", "BasePeakIntensity" };

			//INIT READER
			DBInterface.initReader();

			//LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
			while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
			{
				//DETERMINE Compare_Only_MS1
				if (Convert.ToInt32(measurementhash["ScanType"]) == 1)
				{
					//SET COMPARE ONLY MS1 TO BPI
					Compare_Only_MS1 = Convert.ToDouble(measurementhash["BasePeakIntensity"]);
				}
				else
				{
					//SET COMPARE ONLY MS1 TO PREV COMPARE_ONLY_MS1
					if (line_num == 0)
					{
						//MEANS THIS SHOULD BE A FALSE VALUE ... BUT INSTEAD JUST MARK AS -1
						Compare_Only_MS1 = -1;
					}
					else
					{
						//VALID TO USE PREV VALUE
						Compare_Only_MS1 = prv_Compare_Only_MS1;
					}
				}


				//IF PREV+CURRENT SCANTYPE == 1 AND PREV_BASEPEAKINTENSITY/CURRENT_BPI >= 10 [COLUMN E]
				if (Convert.ToInt32(measurementhash["ScanType"]) == 1 && prv_ScanType == 1 && (prv_BasePeakIntensity / Convert.ToDouble(measurementhash["BasePeakIntensity"])) >= 10)
				{
					//SET IR = 1
					intensity_rise = 1;
				}
				else
				{
					//SET IR = 0
					intensity_rise = 0;
				}

				//IF PREV+CURRENT SCANTYPE == 1 AND CURRENT_BPI/PREV_BASEPEAKINTENSITY >= 10 [COLUMN F]
				if (Convert.ToInt32(measurementhash["ScanType"]) == 1 && prv_ScanType == 1 && (Convert.ToDouble(measurementhash["BasePeakIntensity"]) / prv_BasePeakIntensity) >= 10)
				{
					//SET IF = 1
					intensity_fall = 1;
				}
				else
				{
					//SET IF = 0
					intensity_fall = 0;
				}

				//IF PREV_BASEPEAKINTENSITY/CURRENT_BPI (MS1) >= fold_change_threshold [COLUMN H]
				if ((prv_Compare_Only_MS1 / Compare_Only_MS1) >= fold_change_threshold)
				{
					//SET IF = 1
					intensity_fall_ms1 = 1;

					//ADD TO SUM FOR IS_1A
					sum_is_1a++;
				}
				else
				{
					//SET IF = 0
					intensity_fall_ms1 = 0;
				}

				//IF CURRENT_BPI/PREV_BASEPEAKINTENSITY (MS1) >= fold_change_threshold [COLUMN I]
				if ((Compare_Only_MS1 / prv_Compare_Only_MS1) >= fold_change_threshold)
				{
					//SET IR = 1
					intensity_rise_ms1 = 1;

					//ADD TO SUM FOR IS_1B
					sum_is_1b++;
				}
				else
				{
					//SET IR = 0
					intensity_rise_ms1 = 0;
				}

				//UPDATE PREV VALUES
				prv_ScanType = Convert.ToInt32(measurementhash["ScanType"]);
				prv_BasePeakIntensity = Convert.ToDouble(measurementhash["BasePeakIntensity"]);
				prv_Compare_Only_MS1 = Compare_Only_MS1;

				//INC line_num
				line_num++;
			}
			
		}

        /// <summary>
		/// MS1_5A: Median of precursor mass error (Th)
        /// </summary>
        /// <returns></returns>
        public String MS1_5A()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Scan,temp_xt.Peptide_Expectation_Value_Log,temp_xt.Peptide_MH,temp_xt.Charge, temp_sicstats.MZ "
                + "FROM `temp_xt`,`temp_sicstats` "
                + "WHERE temp_sicstats.FragScanNumber=temp_xt.Scan AND temp_sicstats.random_id=" + r_id + " AND temp_xt.random_id=" + r_id + " "
                + "ORDER BY temp_xt.Scan;");

            //DECLARE VARIABLES
            double massHydrogen = 1.00727649;                                                 //REQUIRED BY MEASUREMENT
            List<double> FilteredArray = new List<double>();                                    //STORE FILTERED VALUES [COLUMN G]
            List<double> AbsFilteredArray = new List<double>();                                 //STORE FILTERED VALUES [COLUMN H]
            List<double> PPMList = new List<double>();                                          //STORE FILTERED VALUES [COLUMN I]
            double median = 0.00;                                                               //INIT MEDIAN
            int i = 0;

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Peptide_MH", "Charge", "MZ" };

            //INIT READER
            DBInterface.initReader();

            //CLEAR HASH TABLE [SHOULD NOT BE NEEDED ... BUT JUST IN CASE]
            measurementhash.Clear();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //CALC THEORETICAL VALUE [COLUMN F]
                double theo = ((Convert.ToDouble(measurementhash["Peptide_MH"]) - massHydrogen) + (massHydrogen * Convert.ToDouble(measurementhash["Charge"]))) / (Convert.ToDouble(measurementhash["Charge"]));

                //IF LOG(E) <= -2 ... CALC FILTERED AND ABS FILTERED
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //CALC FILTERED ARRAY
                    FilteredArray.Add(Convert.ToDouble(measurementhash["MZ"]) - theo);

                    //NOW TAKE THE ABS VALUE OF OUR FILTERED ARRAY
                    AbsFilteredArray.Add(Math.Abs(FilteredArray.Last()));

                    //ADD TO PPM LIST
                    PPMList.Add(FilteredArray.Last() / (theo / 1000000));
                }

                //INC
                i++;

                //CLEAR HASH TABLE
                measurementhash.Clear();
            }

            //NOW CALCULATE MEDIAN
            FilteredArray.Sort();                          //START BY SORTING

            //IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
            if (FilteredArray.Count % 2 == 1)
            {
                //IF ODD
                int pos = (FilteredArray.Count / 2);
                median = FilteredArray[FilteredArray.Count / 2];
            }
            else
            {
                //IF EVEN
                int pos = (FilteredArray.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
                median = (FilteredArray[pos] + FilteredArray[pos + 1]) / 2;
            }

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 6th DIGIT
            decimal round_me = Convert.ToDecimal(median);
            round_me = Math.Round(round_me, 6);                        //ROUND MEDIAN

            //CLEAR HASH TABLE
            FilteredArray.Clear();
            AbsFilteredArray.Clear();
            PPMList.Clear();

            //Console.WriteLine("MS1_5A={0}", round_me);

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// MS1_5B: Median of absolute value of precursor mass error (Th)
        /// </summary>
        /// <returns></returns>
        public String MS1_5B()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Scan,temp_xt.Peptide_Expectation_Value_Log,temp_xt.Peptide_MH,temp_xt.Charge, temp_sicstats.MZ "
                + "FROM `temp_xt`,`temp_sicstats` "
                + "WHERE temp_sicstats.FragScanNumber=temp_xt.Scan AND temp_sicstats.random_id=" + r_id + " AND temp_xt.random_id=" + r_id + " "
                + "ORDER BY temp_xt.Scan;");

            //DECLARE VARIABLES
            double massHydrogen = 1.00727649;                                                 //REQUIRED BY MEASUREMENT
            List<double> FilteredArray = new List<double>();                                    //STORE FILTERED VALUES [COLUMN G]
            List<double> AbsFilteredArray = new List<double>();                                 //STORE FILTERED VALUES [COLUMN H]
            List<double> PPMList = new List<double>();                                          //STORE FILTERED VALUES [COLUMN I]
            double average = 0.00;                                                              //INIT AVERAGE
            int i = 0;

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Peptide_MH", "Charge", "MZ" };

            //INIT READER
            DBInterface.initReader();

            //CLEAR HASH TABLE [SHOULD NOT BE NEEDED ... BUT JUST IN CASE]
            measurementhash.Clear();
            int t = 0;
            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //CALC THEORETICAL VALUE [COLUMN F]
                double theo = ((Convert.ToDouble(measurementhash["Peptide_MH"]) - massHydrogen) + (massHydrogen * Convert.ToDouble(measurementhash["Charge"]))) / (Convert.ToDouble(measurementhash["Charge"]));

                //IF LOG(E) <= -2 ... CALC FILTERED AND ABS FILTERED
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //CALC FILTERED ARRAY
                    FilteredArray.Add(Convert.ToDouble(measurementhash["MZ"]) - theo);

                    //NOW TAKE THE ABS VALUE OF OUR FILTERED ARRAY
                    AbsFilteredArray.Add(Math.Abs(FilteredArray.Last()));
                    t++;
                    //ADD TO PPM LIST
                    PPMList.Add(FilteredArray.Last() / (theo / 1000000));
                }

                //INC
                i++;

                //CLEAR HASH TABLE
                measurementhash.Clear();
            }

            //CALCULATE AVERAGE
            average = AbsFilteredArray.Average();

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 6th DIGIT
            decimal round_me = Convert.ToDecimal(average);
            round_me = Math.Round(round_me, 6);                        //ROUND MEDIAN

            //CLEAR HASH TABLE
            FilteredArray.Clear();
            AbsFilteredArray.Clear();
            PPMList.Clear();

            //Console.WriteLine("MS1_5B={0}", round_me);

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// MS1_5C: Median of precursor mass error (ppm)
        /// </summary>
        /// <returns></returns>
        public String MS1_5C()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Scan,temp_xt.Peptide_Expectation_Value_Log,temp_xt.Peptide_MH,temp_xt.Charge, temp_sicstats.MZ "
                + "FROM `temp_xt`,`temp_sicstats` "
                + "WHERE temp_sicstats.FragScanNumber=temp_xt.Scan AND temp_sicstats.random_id=" + r_id + " AND temp_xt.random_id=" + r_id + " "
                + "ORDER BY temp_xt.Scan;");

            //DECLARE VARIABLES
            double massHydrogen = 1.00727649;                                                 //REQUIRED BY MEASUREMENT
            List<double> FilteredArray = new List<double>();                                    //STORE FILTERED VALUES [COLUMN G]
            List<double> AbsFilteredArray = new List<double>();                                 //STORE FILTERED VALUES [COLUMN H]
            List<double> PPMList = new List<double>();                                          //STORE FILTERED VALUES [COLUMN I]
            double median = 0.00;                                                               //INIT MEDIAN
            int i = 0;

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Peptide_MH", "Charge", "MZ" };

            //INIT READER
            DBInterface.initReader();

            //CLEAR HASH TABLE [SHOULD NOT BE NEEDED ... BUT JUST IN CASE]
            measurementhash.Clear();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //CALC THEORETICAL VALUE [COLUMN F]
                double theo = ((Convert.ToDouble(measurementhash["Peptide_MH"]) - massHydrogen) + (massHydrogen * Convert.ToDouble(measurementhash["Charge"]))) / (Convert.ToDouble(measurementhash["Charge"]));

                //IF LOG(E) <= -2 ... CALC FILTERED AND ABS FILTERED
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //CALC FILTERED ARRAY
                    FilteredArray.Add(Convert.ToDouble(measurementhash["MZ"]) - theo);

                    //NOW TAKE THE ABS VALUE OF OUR FILTERED ARRAY
                    AbsFilteredArray.Add(Math.Abs(FilteredArray.Last()));

                    //ADD TO PPM LIST
                    PPMList.Add(FilteredArray.Last() / (theo / 1000000));
                }

                //INC
                i++;

                //CLEAR HASH TABLE
                measurementhash.Clear();
            }

            //NOW CALCULATE MEDIAN
            PPMList.Sort();                          //START BY SORTING

            //IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
            if (PPMList.Count % 2 == 1)
            {
                //IF ODD
                int pos = (PPMList.Count / 2);
                median = PPMList[PPMList.Count / 2];
            }
            else
            {
                //IF EVEN
                int pos = (PPMList.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
                median = (PPMList[pos] + PPMList[pos + 1]) / 2;
            }

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 6th DIGIT
            decimal round_me = Convert.ToDecimal(median);
            round_me = Math.Round(round_me, 6);                        //ROUND MEDIAN

            //CLEAR HASH TABLE
            FilteredArray.Clear();
            AbsFilteredArray.Clear();
            PPMList.Clear();

            //Console.WriteLine("MS1_5C={0}", round_me);

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// MS1_5D: Interquartile distance in ppm-based precursor mass error
        /// </summary>
        /// <returns></returns>
        public String MS1_5D()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Scan,temp_xt.Peptide_Expectation_Value_Log,temp_xt.Peptide_MH,temp_xt.Charge, temp_sicstats.MZ "
                + "FROM `temp_xt`,`temp_sicstats` "
                + "WHERE temp_sicstats.FragScanNumber=temp_xt.Scan AND temp_sicstats.random_id=" + r_id + " AND temp_xt.random_id=" + r_id + " "
                + "ORDER BY temp_xt.Scan;");

            //DECLARE VARIABLES
            double massHydrogen = 1.00727649;                                                 //REQUIRED BY MEASUREMENT
            List<double> FilteredArray = new List<double>();                                    //STORE FILTERED VALUES [COLUMN G]
            List<double> AbsFilteredArray = new List<double>();                                 //STORE FILTERED VALUES [COLUMN H]
            List<double> PPMList = new List<double>();                                          //STORE FILTERED VALUES [COLUMN I]
            List<double> PPMErrorsList = new List<double>();                                    //STORE ERRORS FROM PPMList [COLUMN M]
            double median = 0.00;                                                               //INIT MEDIAN
            int INTER_QUARTILE_START = 0;                                                       //REQUIRED FOR MEASUREMENT
            int INTER_QUARTILE_END = 0;                                                         //REQUIRED FOR MEASUREMENT
            int i = 0;

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Peptide_MH", "Charge", "MZ" };

            //INIT READER
            DBInterface.initReader();

            //CLEAR HASH TABLE [SHOULD NOT BE NEEDED ... BUT JUST IN CASE]
            measurementhash.Clear();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //CALC THEORETICAL VALUE [COLUMN F]
                double theo = ((Convert.ToDouble(measurementhash["Peptide_MH"]) - massHydrogen) + (massHydrogen * Convert.ToDouble(measurementhash["Charge"]))) / (Convert.ToDouble(measurementhash["Charge"]));

                //IF LOG(E) <= -2 ... CALC FILTERED AND ABS FILTERED
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //CALC FILTERED ARRAY
                    FilteredArray.Add(Convert.ToDouble(measurementhash["MZ"]) - theo);

                    //NOW TAKE THE ABS VALUE OF OUR FILTERED ARRAY
                    AbsFilteredArray.Add(Math.Abs(FilteredArray.Last()));

                    //ADD TO PPM LIST
                    PPMList.Add(FilteredArray.Last() / (theo / 1000000));
                }

                //INC
                i++;

                //CLEAR HASH TABLE
                measurementhash.Clear();
            }

            //NOW FILTER PPM PASSED VALUES [COLUMN K] + START COUNT
            PPMList.Sort();
            int count = 0;
            
            //CALCULATE INTER_QUARTILE_START AND INTER_QUARTILE_END
            INTER_QUARTILE_START = Convert.ToInt32(Math.Round(0.25 * Convert.ToDouble(PPMList.Count)));
            INTER_QUARTILE_END = Convert.ToInt32(Math.Round(0.75 * Convert.ToDouble(PPMList.Count)));

            //LOOP THROUGH EACH ITEM IN LIST
            foreach(double item in PPMList)
            {
                //INC count
                count++;

                //IF COUNT >= INTER_QUARTILE_START && COUNT <= INTER_QUARTILE_END
                if ((count >= INTER_QUARTILE_START) && (count <= INTER_QUARTILE_END))
                {
                    //ADD TO LIST [COLUMN M]
                    PPMErrorsList.Add(item);
                }
            }

            //NOW CALCULATE MEDIAN
            PPMErrorsList.Sort();                          //START BY SORTING

            //IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
            if (PPMErrorsList.Count % 2 == 1)
            {
                //IF ODD
                int pos = (PPMErrorsList.Count / 2);
                median = PPMErrorsList[PPMErrorsList.Count / 2];
            }
            else
            {
                //IF EVEN
                int pos = (PPMErrorsList.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
                median = (PPMErrorsList[pos] + PPMErrorsList[pos + 1]) / 2;
            }

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 4th DIGIT
            decimal round_me = Convert.ToDecimal(median);
            round_me = Math.Round(round_me, 4);                        //ROUND MEDIAN

            //CLEAR HASH TABLE
            FilteredArray.Clear();
            AbsFilteredArray.Clear();
            PPMList.Clear();
            PPMErrorsList.Clear();

            //Console.WriteLine("MS1_5D={0}", round_me);

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// MS2_1: Median MS2 ion injection time
        /// </summary>
        /// <returns></returns>
        public String MS2_1()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.Peptide_Expectation_Value_Log, temp_scanstatsex.Ion_Injection_Time "
                + "FROM `temp_xt`, `temp_scanstatsex` "
                + "WHERE temp_xt.Scan=temp_scanstatsex.ScanNumber AND temp_xt.random_id=" + r_id + " AND temp_scanstatsex.random_id=" + r_id + " "
                + "ORDER BY temp_xt.Scan;");

            //DECLARE VARIABLES
            List<double> FilterList = new List<double>();                                       //FILTERED LIST [COLUMN P]
            double median = 0.00;                                                               //STORE MEDIAN

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Ion_Injection_Time" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //CALCULATE COLUMN P
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //ADD TO FILTERED LIST
                    FilterList.Add(Convert.ToDouble(measurementhash["Ion_Injection_Time"]));
                }

                //CLEAR HASH TABLE
                measurementhash.Clear();
            }

            //NOW CALCULATE MEDIAN
            FilterList.Sort();                          //START BY SORTING

            //IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
            if (FilterList.Count % 2 == 1)
            {
                //IF ODD
                int pos = (FilterList.Count / 2);
                median = FilterList[FilterList.Count / 2];
            }
            else
            {
                //IF EVEN
                int pos = (FilterList.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
                median = (FilterList[pos] + FilterList[pos + 1]) / 2;
            }

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 3rd DIGIT
            decimal round_me = Convert.ToDecimal(median);
            round_me = Math.Round(round_me, 3);                        //ROUND MEDIAN

            //CLEAR HASH TABLE
            FilterList.Clear();

            //Console.WriteLine("MS2_1={0}", round_me);

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// MS2_2: Median S/N value for identified MS2 spectra
        /// </summary>
        /// <returns></returns>
        public String MS2_2()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.Peptide_Expectation_Value_Log, temp_scanstats.BasePeakSignalToNoiseRatio "
                + "FROM `temp_xt`, `temp_scanstats` "
                + "WHERE temp_xt.Scan=temp_scanstats.ScanNumber AND temp_xt.random_id=" + r_id + " AND temp_scanstats.random_id=" + r_id + " "
                + "ORDER BY temp_xt.Scan;");

            //DECLARE VARIABLES
            List<double> FilterList = new List<double>();                                       //FILTERED LIST [COLUMN H]
            List<double> FinishedList = new List<double>();                                     //FINISHED LIST [COLUMN J]
            double median = 0.00;                                                               //STORE MEDIAN
            int current_count = 0;                                                              //CURRENT COUNTER

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "BasePeakSignalToNoiseRatio" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //CALCULATE COLUMN P
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //ADD TO FILTERED LIST
                    FilterList.Add(Convert.ToDouble(measurementhash["BasePeakSignalToNoiseRatio"]));
                }

                //CLEAR HASH TABLE
                measurementhash.Clear();
            }

            //LOOP THROUGH FILTERED LIST
            for (int i = 0; i < FilterList.Count; i++)
            {
                //INC COUNTER
                current_count++;

                //CALCULATE IF <= 0.75
                if(current_count <= (FilterList.Count*0.75) )
                {
                    //ADD TO FINISHED FILTERED LIST
                    FinishedList.Add(FilterList[i]);
                }
            }

            //NOW CALCULATE MEDIAN
            FinishedList.Sort();                          //START BY SORTING

            //IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
            if (FinishedList.Count % 2 == 1)
            {
                //IF ODD
                int pos = (FinishedList.Count / 2);
                median = FinishedList[FinishedList.Count / 2];
            }
            else
            {
                //IF EVEN
                int pos = (FinishedList.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
                median = (FinishedList[pos] + FinishedList[pos + 1]) / 2;
            }

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 4th DIGIT
            decimal round_me = Convert.ToDecimal(median);
            round_me = Math.Round(round_me, 4);                        //ROUND MEDIAN

            //Console.WriteLine("MS1_2={0}", round_me);

            //CLEAR HASH TABLE
            FilterList.Clear();
            FinishedList.Clear();

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// MS2_3: Median number of peaks in all MS2 spectra
        /// </summary>
        /// <returns></returns>
        public String MS2_3()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.Peptide_Expectation_Value_Log, temp_scanstats.IonCountRaw "
                + "FROM `temp_xt`, `temp_scanstats` "
                + "WHERE temp_xt.Scan=temp_scanstats.ScanNumber AND temp_xt.random_id=" + r_id + " AND temp_scanstats.random_id=" + r_id + " "
                + "ORDER BY temp_xt.Scan;");

            //DECLARE VARIABLES
            List<double> FilterList = new List<double>();                                       //FILTERED LIST [COLUMN M]
            double median = 0.00;                                                               //STORE MEDIAN

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "IonCountRaw" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //CALCULATE COLUMN M
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //ADD TO FILTERED LIST
                    FilterList.Add(Convert.ToDouble(measurementhash["IonCountRaw"]));
                }

                //CLEAR HASH TABLE
                measurementhash.Clear();
            }

            //NOW CALCULATE MEDIAN
            FilterList.Sort();                          //START BY SORTING

            //IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
            if (FilterList.Count % 2 == 1)
            {
                //IF ODD
                int pos = (FilterList.Count / 2);
                median = FilterList[FilterList.Count / 2];
            }
            else
            {
                //IF EVEN
                int pos = (FilterList.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
                median = (FilterList[pos] + FilterList[pos + 1]) / 2;
            }

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 3rd DIGIT
            decimal round_me = Convert.ToDecimal(median);
            round_me = Math.Round(round_me, 3);                        //ROUND MEDIAN

            //CLEAR HASH TABLE
            FilterList.Clear();

            //Console.WriteLine("MS2_3={0}", round_me);

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// MS2_4A: Fraction of all MS2 spectra identified; high abundance quartile (determined using MS1 intensity of identified peptides)
        /// </summary>
        /// <returns></returns>
        public String MS2_4A()
        {
            //SET DB QUERY [TO FIND MAX NUMBER OF ROWS]
            DBInterface.setQuery("SELECT COUNT(*) as MaxRows "
                + "FROM `temp_xt`, `temp_sicstats` "
                + "WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + r_id + " AND temp_sicstats.random_id=" + r_id + " "
                + "ORDER BY temp_sicstats.PeakMaxIntensity;");

            //DECLARE FIELDS TO READ FROM
            String[] fields_temp = { "MaxRows" };

            //INIT READER
            DBInterface.initReader();

            //READ LINE
            DBInterface.readSingleLine(fields_temp, ref measurementhash);

            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.Peptide_Expectation_Value_Log, temp_sicstats.PeakMaxIntensity "
                + "FROM `temp_xt`, `temp_sicstats` "
                + "WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + r_id + " AND temp_sicstats.random_id=" + r_id + " "
                + "ORDER BY temp_sicstats.PeakMaxIntensity;");

            //DECLARE VARIABLES
            List<double> FilterList = new List<double>();                                       //FILTERED LIST [COLUMN G]
            List<int> FoundFirstQuartileList = new List<int>();                                 //FOUND FOR FIRST QUARTILE LIST [COLUMN I]
            List<int> FoundSecondQuartileList = new List<int>();                                //FOUND FOR SECOND QUARTILE LIST [COLUMN K]
            List<int> FoundThirdQuartileList = new List<int>();                                 //FOUND FOR THIRD QUARTILE LIST [COLUMN M]
            List<int> FoundFourthQuartileList = new List<int>();                                //FOUND FOR FOURTH QUARTILE LIST [COLUMN O]
            List<int> IdentifiedFirstQuartileList = new List<int>();                            //Identified FOR FIRST QUARTILE LIST [COLUMN J]
            List<int> IdentifiedSecondQuartileList = new List<int>();                           //Identified FOR SECOND QUARTILE LIST [COLUMN L]
            List<int> IdentifiedThirdQuartileList = new List<int>();                            //Identified FOR THIRD QUARTILE LIST [COLUMN N]
            List<int> IdentifiedFourthQuartileList = new List<int>();                           //Identified FOR FOURTH QUARTILE LIST [COLUMN P]
            int max_rows = Convert.ToInt32(measurementhash["MaxRows"]);                         //STORE TOTAL ROWS
            int scan_count = 1;                                                                 //RUNNING SCAN COUNT [COLUMN H]

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "PeakMaxIntensity" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //DID IT PASS OUR FILTER?
                Boolean passed_filter = false;

                //CALCULATE COLUMN G
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //ADD TO FILTERED LIST
                    FilterList.Add(Convert.ToDouble(measurementhash["PeakMaxIntensity"]));

                    passed_filter = true;
                }

                //IF SCAN IN FIRST QUARTILE
                if (scan_count < (max_rows*0.25) )
                {
                    //FOUND SO ADD AS 1
                    FoundFirstQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedFirstQuartileList.Add(1);
                    }
                }

                //IF SCAN IN SECOND QUARTILE
                if (scan_count >= (max_rows * 0.25) && scan_count < (max_rows * 0.5))
                {
                    //FOUND SO ADD AS 1
                    FoundSecondQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedSecondQuartileList.Add(1);
                    }
                }

                //IF SCAN IN THIRD QUARTILE
                if (scan_count >= (max_rows * 0.5) && scan_count < (max_rows * 0.75))
                {
                    //FOUND SO ADD AS 1
                    FoundThirdQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedThirdQuartileList.Add(1);
                    }
                }

                //IF SCAN IN FOURTH QUARTILE
                if (scan_count >= (max_rows * 0.75))
                {
                    //FOUND SO ADD AS 1
                    FoundFourthQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedFourthQuartileList.Add(1);
                    }
                }

                //INC
                scan_count++;
            }

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 4th DIGIT
            decimal round_me = Convert.ToDecimal( Convert.ToDouble(IdentifiedFirstQuartileList.Sum()) / Convert.ToDouble(FoundFirstQuartileList.Sum()) );
            round_me = Math.Round(round_me, 4);                        //ROUND MEDIAN

            //CLEAR HASH TABLE
            FilterList.Clear();
            FoundFirstQuartileList.Clear();
            FoundSecondQuartileList.Clear();
            FoundThirdQuartileList.Clear();
            FoundFourthQuartileList.Clear();
            IdentifiedFirstQuartileList.Clear();
            IdentifiedSecondQuartileList.Clear();
            IdentifiedThirdQuartileList.Clear();
            IdentifiedFourthQuartileList.Clear();

            //Console.WriteLine("MS2_4A: {0}", round_me);

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// MS2_4B: Fraction of all MS2 spectra identified; second quartile (determined using MS1 intensity of identified peptides)
        /// </summary>
        /// <returns></returns>
        public String MS2_4B()
        {
            //SET DB QUERY [TO FIND MAX NUMBER OF ROWS]
            DBInterface.setQuery("SELECT COUNT(*) as MaxRows "
                + "FROM `temp_xt`, `temp_sicstats` "
                + "WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + r_id + " AND temp_sicstats.random_id=" + r_id + " "
                + "ORDER BY temp_sicstats.PeakMaxIntensity;");

            //DECLARE FIELDS TO READ FROM
            String[] fields_temp = { "MaxRows" };

            //INIT READER
            DBInterface.initReader();

            //READ LINE
            DBInterface.readSingleLine(fields_temp, ref measurementhash);

            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.Peptide_Expectation_Value_Log, temp_sicstats.PeakMaxIntensity "
                + "FROM `temp_xt`, `temp_sicstats` "
                + "WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + r_id + " AND temp_sicstats.random_id=" + r_id + " "
                + "ORDER BY temp_sicstats.PeakMaxIntensity;");

            //DECLARE VARIABLES
            List<double> FilterList = new List<double>();                                       //FILTERED LIST [COLUMN G]
            List<int> FoundFirstQuartileList = new List<int>();                                 //FOUND FOR FIRST QUARTILE LIST [COLUMN I]
            List<int> FoundSecondQuartileList = new List<int>();                                //FOUND FOR SECOND QUARTILE LIST [COLUMN K]
            List<int> FoundThirdQuartileList = new List<int>();                                 //FOUND FOR THIRD QUARTILE LIST [COLUMN M]
            List<int> FoundFourthQuartileList = new List<int>();                                //FOUND FOR FOURTH QUARTILE LIST [COLUMN O]
            List<int> IdentifiedFirstQuartileList = new List<int>();                            //Identified FOR FIRST QUARTILE LIST [COLUMN J]
            List<int> IdentifiedSecondQuartileList = new List<int>();                           //Identified FOR SECOND QUARTILE LIST [COLUMN L]
            List<int> IdentifiedThirdQuartileList = new List<int>();                            //Identified FOR THIRD QUARTILE LIST [COLUMN N]
            List<int> IdentifiedFourthQuartileList = new List<int>();                           //Identified FOR FOURTH QUARTILE LIST [COLUMN P]
            int max_rows = Convert.ToInt32(measurementhash["MaxRows"]);                         //STORE TOTAL ROWS
            int scan_count = 1;                                                                 //RUNNING SCAN COUNT [COLUMN H]

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "PeakMaxIntensity" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //DID IT PASS OUR FILTER?
                Boolean passed_filter = false;

                //CALCULATE COLUMN G
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //ADD TO FILTERED LIST
                    FilterList.Add(Convert.ToDouble(measurementhash["PeakMaxIntensity"]));

                    passed_filter = true;
                }

                //IF SCAN IN FIRST QUARTILE
                if (scan_count < (max_rows * 0.25))
                {
                    //FOUND SO ADD AS 1
                    FoundFirstQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedFirstQuartileList.Add(1);
                    }
                }

                //IF SCAN IN SECOND QUARTILE
                if (scan_count >= (max_rows * 0.25) && scan_count < (max_rows * 0.5))
                {
                    //FOUND SO ADD AS 1
                    FoundSecondQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedSecondQuartileList.Add(1);
                    }
                }

                //IF SCAN IN THIRD QUARTILE
                if (scan_count >= (max_rows * 0.5) && scan_count < (max_rows * 0.75))
                {
                    //FOUND SO ADD AS 1
                    FoundThirdQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedThirdQuartileList.Add(1);
                    }
                }

                //IF SCAN IN FOURTH QUARTILE
                if (scan_count >= (max_rows * 0.75))
                {
                    //FOUND SO ADD AS 1
                    FoundFourthQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedFourthQuartileList.Add(1);
                    }
                }

                //INC
                scan_count++;
            }

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 4th DIGIT
            decimal round_me = Convert.ToDecimal(Convert.ToDouble(IdentifiedSecondQuartileList.Sum()) / Convert.ToDouble(FoundSecondQuartileList.Sum()));
            round_me = Math.Round(round_me, 4);                        //ROUND MEDIAN

            //CLEAR HASH TABLE
            FilterList.Clear();
            FoundFirstQuartileList.Clear();
            FoundSecondQuartileList.Clear();
            FoundThirdQuartileList.Clear();
            FoundFourthQuartileList.Clear();
            IdentifiedFirstQuartileList.Clear();
            IdentifiedSecondQuartileList.Clear();
            IdentifiedThirdQuartileList.Clear();
            IdentifiedFourthQuartileList.Clear();

            //Console.WriteLine("MS2_4B: {0}", round_me);

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// MS2_4C: Fraction of all MS2 spectra identified; third quartile (determined using MS1 intensity of identified peptides)
        /// </summary>
        /// <returns></returns>
        public String MS2_4C()
        {
            //SET DB QUERY [TO FIND MAX NUMBER OF ROWS]
            DBInterface.setQuery("SELECT COUNT(*) as MaxRows "
                + "FROM `temp_xt`, `temp_sicstats` "
                + "WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + r_id + " AND temp_sicstats.random_id=" + r_id + " "
                + "ORDER BY temp_sicstats.PeakMaxIntensity;");

            //DECLARE FIELDS TO READ FROM
            String[] fields_temp = { "MaxRows" };

            //INIT READER
            DBInterface.initReader();

            //READ LINE
            DBInterface.readSingleLine(fields_temp, ref measurementhash);

            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.Peptide_Expectation_Value_Log, temp_sicstats.PeakMaxIntensity "
                + "FROM `temp_xt`, `temp_sicstats` "
                + "WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + r_id + " AND temp_sicstats.random_id=" + r_id + " "
                + "ORDER BY temp_sicstats.PeakMaxIntensity;");

            //DECLARE VARIABLES
            List<double> FilterList = new List<double>();                                       //FILTERED LIST [COLUMN G]
            List<int> FoundFirstQuartileList = new List<int>();                                 //FOUND FOR FIRST QUARTILE LIST [COLUMN I]
            List<int> FoundSecondQuartileList = new List<int>();                                //FOUND FOR SECOND QUARTILE LIST [COLUMN K]
            List<int> FoundThirdQuartileList = new List<int>();                                 //FOUND FOR THIRD QUARTILE LIST [COLUMN M]
            List<int> FoundFourthQuartileList = new List<int>();                                //FOUND FOR FOURTH QUARTILE LIST [COLUMN O]
            List<int> IdentifiedFirstQuartileList = new List<int>();                            //Identified FOR FIRST QUARTILE LIST [COLUMN J]
            List<int> IdentifiedSecondQuartileList = new List<int>();                           //Identified FOR SECOND QUARTILE LIST [COLUMN L]
            List<int> IdentifiedThirdQuartileList = new List<int>();                            //Identified FOR THIRD QUARTILE LIST [COLUMN N]
            List<int> IdentifiedFourthQuartileList = new List<int>();                           //Identified FOR FOURTH QUARTILE LIST [COLUMN P]
            int max_rows = Convert.ToInt32(measurementhash["MaxRows"]);                         //STORE TOTAL ROWS
            int scan_count = 1;                                                                 //RUNNING SCAN COUNT [COLUMN H]

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "PeakMaxIntensity" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //DID IT PASS OUR FILTER?
                Boolean passed_filter = false;

                //CALCULATE COLUMN G
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //ADD TO FILTERED LIST
                    FilterList.Add(Convert.ToDouble(measurementhash["PeakMaxIntensity"]));

                    passed_filter = true;
                }

                //IF SCAN IN FIRST QUARTILE
                if (scan_count < (max_rows * 0.25))
                {
                    //FOUND SO ADD AS 1
                    FoundFirstQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedFirstQuartileList.Add(1);
                    }
                }

                //IF SCAN IN SECOND QUARTILE
                if (scan_count >= (max_rows * 0.25) && scan_count < (max_rows * 0.5))
                {
                    //FOUND SO ADD AS 1
                    FoundSecondQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedSecondQuartileList.Add(1);
                    }
                }

                //IF SCAN IN THIRD QUARTILE
                if (scan_count >= (max_rows * 0.5) && scan_count < (max_rows * 0.75))
                {
                    //FOUND SO ADD AS 1
                    FoundThirdQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedThirdQuartileList.Add(1);
                    }
                }

                //IF SCAN IN FOURTH QUARTILE
                if (scan_count >= (max_rows * 0.75))
                {
                    //FOUND SO ADD AS 1
                    FoundFourthQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedFourthQuartileList.Add(1);
                    }
                }

                //INC
                scan_count++;;
            }

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 4th DIGIT
            decimal round_me = Convert.ToDecimal(Convert.ToDouble(IdentifiedThirdQuartileList.Sum()) / Convert.ToDouble(FoundThirdQuartileList.Sum()));
            round_me = Math.Round(round_me, 4);                        //ROUND MEDIAN

            //CLEAR HASH TABLE
            FilterList.Clear();
            FoundFirstQuartileList.Clear();
            FoundSecondQuartileList.Clear();
            FoundThirdQuartileList.Clear();
            FoundFourthQuartileList.Clear();
            IdentifiedFirstQuartileList.Clear();
            IdentifiedSecondQuartileList.Clear();
            IdentifiedThirdQuartileList.Clear();
            IdentifiedFourthQuartileList.Clear();

            //Console.WriteLine("MS2_4C: {0}", round_me);

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// MS2_4D: Fraction of all MS2 spectra identified; low abundance quartile (determined using MS1 intensity of identified peptides)
        /// </summary>
        /// <returns></returns>
        public String MS2_4D()
        {
            //SET DB QUERY [TO FIND MAX NUMBER OF ROWS]
            DBInterface.setQuery("SELECT COUNT(*) as MaxRows "
                + "FROM `temp_xt`, `temp_sicstats` "
                + "WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + r_id + " AND temp_sicstats.random_id=" + r_id + " "
                + "ORDER BY temp_sicstats.PeakMaxIntensity;");

            //DECLARE FIELDS TO READ FROM
            String[] fields_temp = { "MaxRows" };

            //INIT READER
            DBInterface.initReader();

            //READ LINE
            DBInterface.readSingleLine(fields_temp, ref measurementhash);

            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Scan, temp_xt.Peptide_Expectation_Value_Log, temp_sicstats.PeakMaxIntensity "
                + "FROM `temp_xt`, `temp_sicstats` "
                + "WHERE temp_xt.Scan=temp_sicstats.FragScanNumber AND temp_xt.random_id=" + r_id + " AND temp_sicstats.random_id=" + r_id + " "
                + "ORDER BY temp_sicstats.PeakMaxIntensity;");

            //DECLARE VARIABLES
            List<double> FilterList = new List<double>();                                       //FILTERED LIST [COLUMN G]
            List<int> FoundFirstQuartileList = new List<int>();                                 //FOUND FOR FIRST QUARTILE LIST [COLUMN I]
            List<int> FoundSecondQuartileList = new List<int>();                                //FOUND FOR SECOND QUARTILE LIST [COLUMN K]
            List<int> FoundThirdQuartileList = new List<int>();                                 //FOUND FOR THIRD QUARTILE LIST [COLUMN M]
            List<int> FoundFourthQuartileList = new List<int>();                                //FOUND FOR FOURTH QUARTILE LIST [COLUMN O]
            List<int> IdentifiedFirstQuartileList = new List<int>();                            //Identified FOR FIRST QUARTILE LIST [COLUMN J]
            List<int> IdentifiedSecondQuartileList = new List<int>();                           //Identified FOR SECOND QUARTILE LIST [COLUMN L]
            List<int> IdentifiedThirdQuartileList = new List<int>();                            //Identified FOR THIRD QUARTILE LIST [COLUMN N]
            List<int> IdentifiedFourthQuartileList = new List<int>();                           //Identified FOR FOURTH QUARTILE LIST [COLUMN P]
            int max_rows = Convert.ToInt32(measurementhash["MaxRows"]);                         //STORE TOTAL ROWS
            int scan_count = 1;                                                                 //RUNNING SCAN COUNT [COLUMN H]

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "PeakMaxIntensity" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //DID IT PASS OUR FILTER?
                Boolean passed_filter = false;

                //CALCULATE COLUMN G
                if (Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]) <= -2)
                {
                    //ADD TO FILTERED LIST
                    FilterList.Add(Convert.ToDouble(measurementhash["PeakMaxIntensity"]));

                    passed_filter = true;
                }

                //IF SCAN IN FIRST QUARTILE
                if (scan_count < (max_rows * 0.25))
                {
                    //FOUND SO ADD AS 1
                    FoundFirstQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedFirstQuartileList.Add(1);
                    }
                }

                //IF SCAN IN SECOND QUARTILE
                if (scan_count >= (max_rows * 0.25) && scan_count < (max_rows * 0.5))
                {
                    //FOUND SO ADD AS 1
                    FoundSecondQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedSecondQuartileList.Add(1);
                    }
                }

                //IF SCAN IN THIRD QUARTILE
                if (scan_count >= (max_rows * 0.5) && scan_count < (max_rows * 0.75))
                {
                    //FOUND SO ADD AS 1
                    FoundThirdQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedThirdQuartileList.Add(1);
                    }
                }

                //IF SCAN IN FOURTH QUARTILE
                if (scan_count >= (max_rows * 0.75))
                {
                    //FOUND SO ADD AS 1
                    FoundFourthQuartileList.Add(1);

                    //IF PASSED FILTER
                    if (passed_filter)
                    {
                        //ADD SINCE PASSED FILTER
                        IdentifiedFourthQuartileList.Add(1);
                    }
                }

                //INC
                scan_count++;
            }

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 4th DIGIT
            decimal round_me = Convert.ToDecimal(Convert.ToDouble(IdentifiedFourthQuartileList.Sum()) / Convert.ToDouble(FoundFourthQuartileList.Sum()));
            round_me = Math.Round(round_me, 4);                        //ROUND MEDIAN

            //CLEAR HASH TABLE
            FilterList.Clear();
            FoundFirstQuartileList.Clear();
            FoundSecondQuartileList.Clear();
            FoundThirdQuartileList.Clear();
            FoundFourthQuartileList.Clear();
            IdentifiedFirstQuartileList.Clear();
            IdentifiedSecondQuartileList.Clear();
            IdentifiedThirdQuartileList.Clear();
            IdentifiedFourthQuartileList.Clear();

            //Console.WriteLine("MS2_4D: {0}", round_me);

            return Convert.ToString(round_me);
        }

		/// <summary>
		/// P_1A: Median peptide ID score (X!Tandem hyperscore)
		/// </summary>
		/// <returns></returns>
        public String P_1A()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT Scan, Peptide_Hyperscore, Peptide_Expectation_Value_Log "
                + "FROM `temp_xt` "
                + "WHERE temp_xt.random_id=" + r_id + " "
                + "ORDER BY Scan;");

            //DECLARE VARIABLES
            List<double> Peptide_Hyperscore_List = new List<double>();                          //STORE PEPTIDE HYPERSCORE LIST
            double median = 0.00;                                                               //INIT MEDIAN VALUE

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Hyperscore", "Peptide_Expectation_Value_Log" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //CALCULATE COLUMN B + ADD TO LIST
                Peptide_Hyperscore_List.Add(Convert.ToDouble(measurementhash["Peptide_Hyperscore"]));
            }

            //NOW CALCULATE MEDIAN
            Peptide_Hyperscore_List.Sort();                          //START BY SORTING

            //IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
            if (Peptide_Hyperscore_List.Count % 2 == 1)
            {
                //IF ODD
                int pos = (Peptide_Hyperscore_List.Count / 2);
                median = Peptide_Hyperscore_List[Peptide_Hyperscore_List.Count / 2];
            }
            else
            {
                //IF EVEN
                int pos = (Peptide_Hyperscore_List.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
                median = (Peptide_Hyperscore_List[pos] + Peptide_Hyperscore_List[pos + 1]) / 2;
            }

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 3rd DIGIT
            decimal round_me = Convert.ToDecimal(median);
            round_me = Math.Round(round_me, 3);                        //ROUND MEDIAN

            //Console.WriteLine("P_1A: {0}", round_me);

            //CLEAR HASH TABLE
            Peptide_Hyperscore_List.Clear();

            return Convert.ToString(round_me);
        }

		/// <summary>
		/// P_1B: Median peptide ID score (X!Tandem Peptide_Expectation_Value_Log(e))
		/// </summary>
		/// <returns></returns>
        public String P_1B()
        {
            //SET DB QUERY
            DBInterface.setQuery("SELECT Scan, Peptide_Hyperscore, Peptide_Expectation_Value_Log "
                + "FROM `temp_xt` "
                + "WHERE temp_xt.random_id=" + r_id + " "
                + "ORDER BY Scan;");

            //DECLARE VARIABLES
            List<double> Peptide_Expectation_List = new List<double>();                         //STORE PEPTIDE EXP LIST
            double median = 0.00;                                                               //INIT MEDIAN VALUE

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Hyperscore", "Peptide_Expectation_Value_Log" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
                //CALCULATE COLUMN C + ADD TO LIST
                Peptide_Expectation_List.Add(Convert.ToDouble(measurementhash["Peptide_Expectation_Value_Log"]));
            }

            //NOW CALCULATE MEDIAN
            Peptide_Expectation_List.Sort();                          //START BY SORTING

            //IF ODD # OF RESULTS WE MUST DIVIDE BY 2 AND TAKE RESULT [NOT NEED TO ADD 1 DUE TO STARTING AT 0 POSITION LIKE YOU NORMALLY WOULD IF A MEDIAN HAS ODD TOTAL #]
            if (Peptide_Expectation_List.Count % 2 == 1)
            {
                //IF ODD
                int pos = (Peptide_Expectation_List.Count / 2);
                median = Peptide_Expectation_List[Peptide_Expectation_List.Count / 2];
            }
            else
            {
                //IF EVEN
                int pos = (Peptide_Expectation_List.Count / 2) - 1; //-1 DUE TO STARTING AT 0 INSTEAD OF 1
                median = (Peptide_Expectation_List[pos] + Peptide_Expectation_List[pos + 1]) / 2;
            }

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 3rd DIGIT
            decimal round_me = Convert.ToDecimal(median);
            round_me = Math.Round(round_me, 3);                        //ROUND MEDIAN

            //Console.WriteLine("P_1B: {0}", round_me);

            //CLEAR HASH TABLE
            Peptide_Expectation_List.Clear();

            return Convert.ToString(round_me);
        }

        /// <summary>
		/// P_2A: Number of tryptic peptides; total spectra count
        /// </summary>
        /// <returns></returns>
        public String P_2A()
        {
            //BUILD RESULT_ID, UNIQUE_SEQ_TABLE
			System.Collections.Generic.Dictionary<int, int> ResultID_to_Unique_Seq_ID_Table;
			ResultID_to_Unique_Seq_ID_Table = GetResultIDToSeqIDTable();

			// BUILD UNIQUE_SEQ_TABLE, CLEAVAGE STATE TABLE            
			// Populate a dictionary object via a single query to the database
			System.Collections.Generic.Dictionary<int, int> Seq_ID_to_Cleavage_State_Table;
			Seq_ID_to_Cleavage_State_Table = GetSeqIDToCleavageStateTable();

            //SET DB QUERY
            DBInterface.setQuery("SELECT Scan,Peptide_Expectation_Value_Log, Charge, temp_xt.Result_ID, Peptide_Sequence, temp_xt_seqtoproteinmap.Cleavage_State "
                + "FROM `temp_xt`, `temp_xt_resulttoseqmap` "
                + "JOIN `temp_xt_seqtoproteinmap` ON `temp_xt_resulttoseqmap`.Unique_Seq_ID=`temp_xt_seqtoproteinmap`.Unique_Seq_ID "
                + "WHERE temp_xt.Peptide_Expectation_Value_Log <= -2.00 "
                + "AND temp_xt.Result_ID=temp_xt_resulttoseqmap.Result_ID "
                + "AND temp_xt.random_id=" + r_id + " "
                + "AND temp_xt_resulttoseqmap.random_id=" + r_id + " "
                + "GROUP BY temp_xt_resulttoseqmap.Result_ID "
                + "ORDER BY temp_xt.Peptide_Sequence, temp_xt.Group_ID;");

            //DECLARE VARIABLES
            int cleavage_state_1_count = 0;                                                         //COLUMN H
            int cleavage_state_2_count = 0;                                                         //COLUMN I
            int unique_cleavage_state_2_count = 0;                                                  //COLUMN J
            int cleavage_state_2_charge_1 = 0;                                                      //COLUMN K
            int line_count = 0;                                                                     //LINE COUNTER
            String prv_peptide_sequence = "";                                                       //PRV PEPTIDE SEQUENCE

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Charge", "Result_ID", "Peptide_Sequence", "Cleavage_State" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
				int resultID;
				int seqID;
				int cleavage_state = 0;

				// DETERMINE THE CLEAVAGE STATE
				if (int.TryParse(measurementhash["Result_ID"].ToString(), out resultID))
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
                    if (Convert.ToInt32(measurementhash["Charge"]) == 1)
                    {
                        cleavage_state_2_charge_1 = 1;
                    }

                }
                else
                {
                    //LINE COUNT IS NOT THE FIRST LINE

                    //FOR unique_cleavage_state_2_count IF CURRENT PEPTIDE SEQUENCE != PRV PEPTIDE SEQUENCE [COLUMN J]
                    if (cleavage_state == 2 && !Convert.ToString(measurementhash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
                    {
                        unique_cleavage_state_2_count += 1;
                    }

                    //FOR cleavage_state_2_charge_1 IF CURRENT PEPTIDE SEQUENCE != PRV PEPTIDE SEQUENCE [COLUMN K]
                    if (Convert.ToInt32(measurementhash["Charge"]) == 1 && !Convert.ToString(measurementhash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
                    {
                        cleavage_state_2_charge_1 += 1;
                    }

                }

                //UPDATE PREVIOUS VALUES FOR NEXT LOOP
                prv_peptide_sequence = Convert.ToString(measurementhash["Peptide_Sequence"]);
            }

            //SET ANSWER
            int answer = cleavage_state_2_count;

            //CLEAR HASH TABLE
            //Console.WriteLine("P_2A:: {0}", answer);

            return Convert.ToString(answer);
        }

        /// <summary>
		/// P_2B: Number of tryptic peptides; unique peptide & charge count
        /// </summary>
        /// <returns></returns>
        public String P_2B()
        {
            //BUILD RESULT_ID, UNIQUE_SEQ_TABLE
			System.Collections.Generic.Dictionary<int, int> ResultID_to_Unique_Seq_ID_Table;
			ResultID_to_Unique_Seq_ID_Table = GetResultIDToSeqIDTable();

			// BUILD UNIQUE_SEQ_TABLE, CLEAVAGE STATE TABLE            
			// Populate a dictionary object via a single query to the database
			System.Collections.Generic.Dictionary<int, int> Seq_ID_to_Cleavage_State_Table;
			Seq_ID_to_Cleavage_State_Table = GetSeqIDToCleavageStateTable();

            //SET DB QUERY
            DBInterface.setQuery("SELECT temp_xt.Result_ID, temp_xt.Scan, temp_xt.Peptide_Expectation_Value_Log, temp_xt.Charge, temp_xt.Peptide_Sequence, temp_xt_seqtoproteinmap.Cleavage_State, temp_xt_seqtoproteinmap.Unique_Seq_ID "
                + "FROM `temp_xt`, `temp_xt_resulttoseqmap` "
                + "INNER JOIN `temp_xt_seqtoproteinmap` ON `temp_xt_resulttoseqmap`.Unique_Seq_ID=`temp_xt_seqtoproteinmap`.Unique_Seq_ID "
                + "WHERE Peptide_Expectation_Value_Log <= -2.00 "
                + "AND temp_xt.Result_ID=temp_xt_resulttoseqmap.Result_ID "
                + "AND temp_xt.random_id=" + r_id + " "
                + "AND temp_xt_resulttoseqmap.random_id=" + r_id + " "
                + "GROUP BY temp_xt_resulttoseqmap.Result_ID "
                + "ORDER BY Charge, Peptide_Sequence, Scan;");

            //DECLARE VARIABLES
            String prv_peptide_sequence = "";                                                       //STORE PREVIOUS PEPTIDE SEQUENCE
            String prv_prv_peptide_sequence = "";                                                   //STORE PREVIOUS PREVIOUS PEPTIDE SEQUENCE
            int prv_cleavage_state = 0;                                                             //STORE PREVIOUS CLEAVAGE STATE
            int count_with_different_charges = 0;                                                   //COUNTS WITH DIFFERENT CHARGES [COLUMN F]
            Boolean is_first_line = true;                                                           //IS FIRST LINE 

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Result_ID", "Scan", "Peptide_Expectation_Value_Log", "Charge", "Peptide_Sequence", "Cleavage_State", "Unique_Seq_ID" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
				int resultID;
				int seqID;
				int cleavage_state = 0;

				// DETERMINE THE CLEAVAGE STATE
				if (int.TryParse(measurementhash["Result_ID"].ToString(), out resultID))
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
                    if (cleavage_state == 2 && !Convert.ToString(measurementhash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
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
                prv_peptide_sequence = Convert.ToString(measurementhash["Peptide_Sequence"]);
                prv_cleavage_state = cleavage_state;
            }

            //CLEAR HASH TABLE
            //Console.WriteLine("P_2B:: {0}", count_with_different_charges);

            return Convert.ToString(count_with_different_charges);
        }

        /// <summary>
		/// P_2C: Number of tryptic peptides; unique peptide count
        /// </summary>
        /// <returns></returns>
        public String P_2C()
        {
			//BUILD RESULT_ID, UNIQUE_SEQ_TABLE
			System.Collections.Generic.Dictionary<int, int> ResultID_to_Unique_Seq_ID_Table;
			ResultID_to_Unique_Seq_ID_Table = GetResultIDToSeqIDTable();

			// BUILD UNIQUE_SEQ_TABLE, CLEAVAGE STATE TABLE            
			// Populate a dictionary object via a single query to the database
			System.Collections.Generic.Dictionary<int, int> Seq_ID_to_Cleavage_State_Table;
			Seq_ID_to_Cleavage_State_Table = GetSeqIDToCleavageStateTable();

            //SET DB QUERY
            DBInterface.setQuery("SELECT Scan,Peptide_Expectation_Value_Log, Charge, temp_xt.Result_ID, Peptide_Sequence, temp_xt_seqtoproteinmap.Cleavage_State, temp_xt_seqtoproteinmap.Unique_Seq_ID "
                + "FROM `temp_xt`, `temp_xt_resulttoseqmap` "
                + "JOIN `temp_xt_seqtoproteinmap` ON `temp_xt_resulttoseqmap`.Unique_Seq_ID=`temp_xt_seqtoproteinmap`.Unique_Seq_ID "
                + "WHERE temp_xt.Peptide_Expectation_Value_Log <= -2.00 "
                + "AND temp_xt.Result_ID=temp_xt_resulttoseqmap.Result_ID "
                + "AND temp_xt.random_id=" + r_id + " "
                + "AND temp_xt_resulttoseqmap.random_id=" + r_id + " "
                + "GROUP BY temp_xt_resulttoseqmap.Result_ID "
                + "ORDER BY temp_xt.Peptide_Sequence, temp_xt.Group_ID;");

            //DECLARE VARIABLES
            int cleavage_state_1_count = 0;                                                         //COLUMN H
            int cleavage_state_2_count = 0;                                                         //COLUMN I
            int unique_cleavage_state_2_count = 0;                                                  //COLUMN J
            int cleavage_state_2_charge_1 = 0;                                                      //COLUMN K
            int line_count = 0;                                                                     //LINE COUNTER
            String prv_peptide_sequence = "";                                                       //PRV PEPTIDE SEQUENCE

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Charge", "Result_ID", "Peptide_Sequence", "Cleavage_State", "Unique_Seq_ID" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
				int resultID;
				int seqID;
				int cleavage_state = 0;

				// DETERMINE THE CLEAVAGE STATE
				if (int.TryParse(measurementhash["Result_ID"].ToString(), out resultID))
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
                    if (Convert.ToInt32(measurementhash["Charge"]) == 1)
                    {
                        cleavage_state_2_charge_1 = 1;
                    }

                }
                else
                {
                    //LINE COUNT IS NOT THE FIRST LINE

                    //FOR unique_cleavage_state_2_count IF CURRENT PEPTIDE SEQUENCE != PRV PEPTIDE SEQUENCE [COLUMN J]
                    if (cleavage_state == 2 && !Convert.ToString(measurementhash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
                    {
                        unique_cleavage_state_2_count += 1;
                    }

                    //FOR cleavage_state_2_charge_1 IF CURRENT PEPTIDE SEQUENCE != PRV PEPTIDE SEQUENCE [COLUMN K]
                    if (Convert.ToInt32(measurementhash["Charge"]) == 1 && !Convert.ToString(measurementhash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
                    {
                        cleavage_state_2_charge_1 += 1;
                    }

                }

                //UPDATE PREVIOUS VALUES FOR NEXT LOOP
                prv_peptide_sequence = Convert.ToString(measurementhash["Peptide_Sequence"]);
            }

            //SET ANSWER
            int answer = unique_cleavage_state_2_count;

            //CLEAR HASH TABLE
            //Console.WriteLine("P_2C:: {0}", answer);

            return Convert.ToString(answer);
        }

        /// <summary>
		/// P_3: Ratio of semi-tryptic / fully tryptic peptides
        /// </summary>
        /// <returns></returns>
        public String P_3()
        {
			//BUILD RESULT_ID, UNIQUE_SEQ_TABLE
			System.Collections.Generic.Dictionary<int, int> ResultID_to_Unique_Seq_ID_Table;
			ResultID_to_Unique_Seq_ID_Table = GetResultIDToSeqIDTable();

			// BUILD UNIQUE_SEQ_TABLE, CLEAVAGE STATE TABLE            
			// Populate a dictionary object via a single query to the database
			System.Collections.Generic.Dictionary<int, int> Seq_ID_to_Cleavage_State_Table;
			Seq_ID_to_Cleavage_State_Table = GetSeqIDToCleavageStateTable();

            //SET DB QUERY
            DBInterface.setQuery("SELECT Scan,Peptide_Expectation_Value_Log, Charge, temp_xt.Result_ID, Peptide_Sequence, temp_xt_seqtoproteinmap.Cleavage_State "
                + "FROM `temp_xt`, `temp_xt_resulttoseqmap` "
                + "JOIN `temp_xt_seqtoproteinmap` ON `temp_xt_resulttoseqmap`.Unique_Seq_ID=`temp_xt_seqtoproteinmap`.Unique_Seq_ID "
                + "WHERE temp_xt.Peptide_Expectation_Value_Log <= -2.00 "
                + "AND temp_xt.Result_ID=temp_xt_resulttoseqmap.Result_ID "
                + "AND temp_xt.random_id=" + r_id + " "
                + "AND temp_xt_resulttoseqmap.random_id=" + r_id + " "
                + "GROUP BY temp_xt_resulttoseqmap.Result_ID "
                + "ORDER BY temp_xt.Peptide_Sequence, temp_xt.Group_ID;");

            //DECLARE VARIABLES
            int cleavage_state_1_count = 0;                                                         //COLUMN H
            int cleavage_state_2_count = 0;                                                         //COLUMN I
            int unique_cleavage_state_2_count = 0;                                                  //COLUMN J
            int cleavage_state_2_charge_1 = 0;                                                      //COLUMN K
            int line_count = 0;                                                                     //LINE COUNTER
            String prv_peptide_sequence = "";                                                       //PRV PEPTIDE SEQUENCE

            //DECLARE FIELDS TO READ FROM
            String[] fields = { "Scan", "Peptide_Expectation_Value_Log", "Charge", "Result_ID", "Peptide_Sequence", "Cleavage_State" };

            //INIT READER
            DBInterface.initReader();

            //LOOP READING + CLEARING HASH TABLE AS LONG AS THERE ARE ROWS TO READ FROM
            while ((DBInterface.readLines(fields, ref measurementhash)) && (measurementhash.Count > 0))
            {
				int resultID;
				int seqID;
				int cleavage_state = 0;

                // DETERMINE THE CLEAVAGE STATE
				if (int.TryParse(measurementhash["Result_ID"].ToString(), out resultID))
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
                    if (Convert.ToInt32(measurementhash["Charge"]) == 1)
                    {
                        cleavage_state_2_charge_1 = 1;
                    }

                }
                else
                {
                    //LINE COUNT IS NOT THE FIRST LINE

                    //FOR unique_cleavage_state_2_count IF CURRENT PEPTIDE SEQUENCE != PRV PEPTIDE SEQUENCE [COLUMN J]
                    if (cleavage_state == 2 && !Convert.ToString(measurementhash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
                    {
                            unique_cleavage_state_2_count += 1;
                    }

                    //FOR cleavage_state_2_charge_1 IF CURRENT PEPTIDE SEQUENCE != PRV PEPTIDE SEQUENCE [COLUMN K]
                    if (Convert.ToInt32(measurementhash["Charge"]) == 1 && !Convert.ToString(measurementhash["Peptide_Sequence"]).Equals(prv_peptide_sequence))
                    {
                        cleavage_state_2_charge_1 += 1;
                    }

                }

                //UPDATE PREVIOUS VALUES FOR NEXT LOOP
                prv_peptide_sequence = Convert.ToString(measurementhash["Peptide_Sequence"]);

            }

            //SET ANSWER
            double answer = Convert.ToDouble(cleavage_state_1_count) / Convert.ToDouble(cleavage_state_2_count);

            //WE NOW HAVE RESULT ... NOW ROUND IT TO 6th DIGIT
            decimal round_me = Convert.ToDecimal(answer);
            round_me = Math.Round(round_me, 6);                        //ROUND MEDIAN

            //CLEAR HASH TABLE
            //Console.WriteLine("P_3:: {0}", round_me);

            return Convert.ToString(round_me);
        }

/*
TRUNCATE `temp_scanstats`;
TRUNCATE `temp_scanstatsex`;
TRUNCATE `temp_sicstats`;
TRUNCATE `temp_xt`;
TRUNCATE `temp_xt_resulttoseqmap`;
TRUNCATE `temp_xt_seqtoproteinmap`;
*/

    }
}
