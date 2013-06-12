using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using System.IO;

namespace SMAQC
{
    class OutputFileManager
    {
        //DECLARE VARIABLES
        private DBWrapper DBWrapper;                                                                //REF DB INTERFACE OBJECT
        private Boolean first_use;                                                                  //IS THE FIRST USE?
        private string SMAQC_VERSION;                                                               //SMAQC VERSION
        private string SMAQC_BUILD_DATE;                                                            //SMAQC BUILD DATE
        private string[] fields;                                                                    //SMAQC FIELDS

        //CONSTRUCTOR
        public OutputFileManager(ref DBWrapper DBWrapper, string ProgVersion, string ProgBuildDate, string[] ProgFields)
        {
            this.DBWrapper = DBWrapper;
            first_use = true;
            this.SMAQC_VERSION = ProgVersion;
            this.SMAQC_BUILD_DATE = ProgBuildDate;
            this.fields = ProgFields;
        }

        //DESTRUCTOR
        ~OutputFileManager()
        {

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
        private int CreateOutputFileForFirstTimeUse(string dataset, string filename, int scan_id, int dataset_number)
        {
            //DECLARE VARIABLES
            Hashtable scandata = new Hashtable();                                             //HASH TABLE FOR SCAN RESULTS
            SortedDictionary<string, string> d = new SortedDictionary<string, string>();

            //CALCULATE RELATIVE RESULT_ID
            int result_id = scan_id + dataset_number;

            //SET QUERY TO RETRIEVE SCAN RESULTS
            DBWrapper.setQuery("SELECT * FROM `scan_results` WHERE `result_id`='" + result_id + "' LIMIT 1;");

            //INIT READER
            DBWrapper.initReader();

            //READ IT INTO OUR HASH TABLE
            DBWrapper.readSingleLine(fields, ref scandata);

            //GET COUNT
            int count = scandata.Count;

            //LINE TO SAVE TO
            string line = "";

            //ENSURE THERE IS DATA!
            if (count > 0)
            {
                //OPEN TEMP FILE
                StreamWriter file = new System.IO.StreamWriter(filename);

                line += "SMAQC SCANNER RESULTS\r\n";
                line += "-----------------------------------------------------------\r\n";
                line += "SMAQC Version: " + SMAQC_VERSION + "\r\n";
                line += "Results from Scan ID: " + scan_id + "\r\n";
                line += "Instrument ID: " + scandata["instrument_id"] + "\r\n";
                line += "Scan Date: " + scandata["scan_date"] + "\r\n";
                line += "[Data]\r\n";
                line += "Dataset, Measurement Name, Measurement Value\r\n";

                //REMOVE FROM HASH TABLE
                scandata.Remove("instrument_id");
                scandata.Remove("scan_date");
                scandata.Remove("scan_id");
                scandata.Remove("random_id");

                //LOOP THROUGH ALL THAT SHOULD BE LEFT [OUR MEASUREMENTS]
                foreach (string key in scandata.Keys)
                {
                    //ENSURE THAT ALL KEYS HAVE DATA [THIS IS REALLY A FIX FOR SQLITE DUE TO NOT SUPPORTING NULLS PROPERLY]
                    if (!scandata[key].Equals(""))
                    {
                        //ADD TO SORTED DICTIONARY
                        d.Add(Convert.ToString(key), Convert.ToString(scandata[key]));
                    }
                }

                //LOOP THROUGH EACH SORTED DICTIONARY
                foreach (var pair in d)
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
                return -1;
            }

            //Console.WriteLine("LINE={0}", line);
            return 0;
        }

        //APPEND ADDITIONAL MEASUREMENT DATA TO OUTPUT FILE
        private int AppendAdditionalMeasurementsToOutputFile(string dataset, string filename, int scan_id, int dataset_number)
        {
            //DECLARE VARIABLES
            Hashtable scandata = new Hashtable();                                             //HASH TABLE FOR SCAN RESULTS
            SortedDictionary<string, string> d = new SortedDictionary<string, string>();

            //CALCULATE RELATIVE RESULT_ID
            int result_id = scan_id + dataset_number;

            //SET QUERY TO RETRIEVE SCAN RESULTS
            DBWrapper.setQuery("SELECT * FROM `scan_results` WHERE `result_id`='" + result_id + "' LIMIT 1;");

            //INIT READER
            DBWrapper.initReader();

            //READ IT INTO OUR HASH TABLE
            DBWrapper.readSingleLine(fields, ref scandata);

            //GET COUNT
            int count = scandata.Count;

            //LINE TO SAVE TO
            string line = "";

            //ENSURE THERE IS DATA!
            if (count > 0)
            {
                //OPEN TEMP FILE
                StreamWriter file = File.AppendText(filename);// new System.IO.StreamWriter(filename);

                //REMOVE FROM HASH TABLE
                scandata.Remove("instrument_id");
                scandata.Remove("scan_date");
                scandata.Remove("scan_id");
                scandata.Remove("random_id");

                //LOOP THROUGH ALL THAT SHOULD BE LEFT [OUR MEASUREMENTS]
                foreach (string key in scandata.Keys)
                {
                    //ENSURE THAT ALL KEYS HAVE DATA [THIS IS REALLY A FIX FOR SQLITE DUE TO NOT SUPPORTING NULLS PROPERLY]
                    if (!scandata[key].Equals(""))
                    {
                        //ADD TO SORTED DICTIONARY
                        d.Add(Convert.ToString(key), Convert.ToString(scandata[key]));
                    }
                }

                //LOOP THROUGH EACH SORTED DICTIONARY
                foreach (var pair in d)
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
                return -1;
            }

            //Console.WriteLine("LINE={0}", line);
            return 0;
        }


    }
}
