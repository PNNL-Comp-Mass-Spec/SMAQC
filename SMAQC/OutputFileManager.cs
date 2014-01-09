using System;
using System.Collections.Generic;
using System.IO;

namespace SMAQC
{
    class OutputFileManager
    {
        //DECLARE VARIABLES
        private readonly DBWrapper DBWrapper;                                                                //REF DB INTERFACE OBJECT
        private Boolean first_use;																			 //IS THE FIRST USE?
        private readonly string SMAQC_VERSION;                                                               //SMAQC VERSION
        private readonly string[] fields;                                                                    //SMAQC FIELDS

        //CONSTRUCTOR
        public OutputFileManager(ref DBWrapper DBWrapper, string ProgVersion, string[] ProgFields)
        {
            this.DBWrapper = DBWrapper;
            first_use = true;
            SMAQC_VERSION = ProgVersion;
            fields = ProgFields;
        }

        //SAVE DATA HANDLER
        public void SaveData(string dataset, string filename, int scan_id, int dataset_number)
        {
            //IF THIS IS THE FIRST USE CREATE FILE + WRITE TO IT
            if (first_use)
            {
                //CREATE THE FILE + APPEND FIRST SET OF METRICS
                CreateOutputFileForFirstTimeUse(dataset, filename, scan_id, dataset_number);

                //SET FIRST_USE TO FALSE
                first_use = false;
            }
            else
            {
                //APPEND TO THE FILE
                AppendAdditionalMeasurementsToOutputFile(dataset, filename, scan_id, dataset_number);
            }
        }

        //CREATE THE FILE + ADD METRICS FOR FIRST TIME USE
        private void CreateOutputFileForFirstTimeUse(string dataset, string filename, int scan_id, int dataset_number)
        {
            //DECLARE VARIABLES
			var dctResults = new Dictionary<string, string>();                                             // SCAN RESULTS
            var dctValidResults = new SortedDictionary<string, string>();

            //CALCULATE RELATIVE RESULT_ID
            int result_id = scan_id + dataset_number;

            //SET QUERY TO RETRIEVE SCAN RESULTS
            DBWrapper.setQuery("SELECT * FROM scan_results WHERE result_id='" + result_id + "' LIMIT 1;");

            //INIT READER
            DBWrapper.initReader();

            //READ IT INTO OUR HASH TABLE
			DBWrapper.readSingleLine(fields, ref dctResults);

            //GET COUNT
			int count = dctResults.Count;

            //LINE TO SAVE TO
            string line = "";

            //ENSURE THERE IS DATA!
            if (count > 0)
            {
                //OPEN TEMP FILE
	            using (var file = new StreamWriter(filename))
	            {

		            line += "SMAQC SCANNER RESULTS\r\n";
		            line += "-----------------------------------------------------------\r\n";
		            line += "SMAQC Version: " + SMAQC_VERSION + "\r\n";
		            // line += "Results from Scan ID: " + scan_id + "\r\n";
		            line += "Instrument ID: " + dctResults["instrument_id"] + "\r\n";
		            line += "Scan Date: " + dctResults["scan_date"] + "\r\n";
		            line += "[Data]\r\n";
		            line += "Dataset, Measurement Name, Measurement Value\r\n";

		            //REMOVE FROM HASH TABLE
		            dctResults.Remove("instrument_id");
		            dctResults.Remove("scan_date");
		            dctResults.Remove("scan_id");
		            dctResults.Remove("random_id");

		            //LOOP THROUGH ALL THAT SHOULD BE LEFT [OUR MEASUREMENTS]
		            foreach (string key in dctResults.Keys)
		            {
			            //ENSURE THAT ALL KEYS HAVE DATA [THIS IS REALLY A FIX FOR SQLITE DUE TO NOT SUPPORTING NULLS PROPERLY]
			            if (!string.IsNullOrEmpty(dctResults[key]))
			            {
				            //ADD TO SORTED DICTIONARY
				            dctValidResults.Add(key, dctResults[key]);
			            }
		            }

		            //LOOP THROUGH EACH SORTED DICTIONARY
		            foreach (var pair in dctValidResults)
		            {
			            //ADD:: Dataset, Measurement Name,
			            //line += String.Format("" + dataset + ", " + pair.Key + ", " + pair.Value + "\r\n");
			            line += String.Format("" + dataset + ", " + pair.Key + ",");

			            //IF THERE IS A NON-NULL VALUE
			            if (!pair.Value.Equals("Null"))
			            {
				            line += " " + pair.Value;
			            }

			            //NOW ADD RETURN + NEWLINE CHAR
			            line += "\r\n";
		            }

		            //APPEND BLANK LINE
		            line += "\r\n";

		            //WRITE FILE
		            file.Write(line);

		            //CLOSE FILE
	            }
            }
            else
            {
	            Console.WriteLine("Error: The scan id provided either does not exist, or has no results!");
            }
	       
        }

        //APPEND ADDITIONAL MEASUREMENT DATA TO OUTPUT FILE
        private void AppendAdditionalMeasurementsToOutputFile(string dataset, string filename, int scan_id, int dataset_number)
        {
            //DECLARE VARIABLES
			var dctResults = new Dictionary<string, string>();                                             //HASH TABLE FOR SCAN RESULTS
            var dctValidResults = new SortedDictionary<string, string>();

            //CALCULATE RELATIVE RESULT_ID
            int result_id = scan_id + dataset_number;

            //SET QUERY TO RETRIEVE SCAN RESULTS
            DBWrapper.setQuery("SELECT * FROM scan_results WHERE result_id='" + result_id + "' LIMIT 1;");

            //INIT READER
            DBWrapper.initReader();

            //READ IT INTO OUR HASH TABLE
			DBWrapper.readSingleLine(fields, ref dctResults);

            //GET COUNT
			int count = dctResults.Count;

            //LINE TO SAVE TO
            string line = "";

            //ENSURE THERE IS DATA!
            if (count > 0)
            {
                //OPEN TEMP FILE
                StreamWriter file = File.AppendText(filename);// new StreamWriter(filename);

                //REMOVE FROM HASH TABLE
				dctResults.Remove("instrument_id");
				dctResults.Remove("scan_date");
				dctResults.Remove("scan_id");
				dctResults.Remove("random_id");

                //LOOP THROUGH ALL THAT SHOULD BE LEFT [OUR MEASUREMENTS]
				foreach (string key in dctResults.Keys)
                {
                    //ENSURE THAT ALL KEYS HAVE DATA [THIS IS REALLY A FIX FOR SQLITE DUE TO NOT SUPPORTING NULLS PROPERLY]
					if (!string.IsNullOrEmpty(dctResults[key]))
                    {
                        //ADD TO SORTED DICTIONARY
						dctValidResults.Add(key, dctResults[key]);
                    }
                }

                //LOOP THROUGH EACH SORTED DICTIONARY
                foreach (var pair in dctValidResults)
                {
                    //ADD:: Dataset, Measurement Name,
                    //line += String.Format("" + dataset + ", " + pair.Key + ", " + pair.Value + "\r\n");
                    line += String.Format("" + dataset + ", " + pair.Key + ",");

                    //IF THERE IS A NON-NULL VALUE
                    if (!pair.Value.Equals("Null"))
                    {
                        line += " " + pair.Value;
                    }

                    //NOW ADD RETURN + NEWLINE CHAR
                    line += "\r\n";
                }


                //APPEND BLANK LINE
                line += "\r\n";

                //WRITE FILE
                file.Write(line);

                //CLOSE FILE
                file.Close();
            }
            else
            {
	            Console.WriteLine("Error: The scan id provided either does not exist, or has no results!");	            
            }

        }


    }
}
