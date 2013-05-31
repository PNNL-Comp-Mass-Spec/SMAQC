using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace SMAQC
{
    class Filter
    {
        //DECLARE VARIABLES
        public DBWrapper mDBWrapper;                                                                //CREATE DB INTERFACE OBJECT
        public String instrument_id;                                                                //INSTRUMENT ID
        public int random_id;                                                                       //RANDOM ID
        public DataFileFormatter DFF = new DataFileFormatter();                                     //DFF OBJECT
		SystemLogManager m_SystemLogManager;

        //CONSTRUCTOR
		public Filter(ref DBWrapper DBInterface, String instrument_id, int random_id, ref SystemLogManager systemLogManager)
        {
            this.mDBWrapper = DBInterface;
            this.instrument_id = instrument_id;
            this.random_id = random_id;
			this.m_SystemLogManager = systemLogManager;

			// Attach the event handler
			this.mDBWrapper.ErrorEvent += new DBWrapper.DBErrorEventHandler(DBWrapper_ErrorEvent);
        }

        //DESTRUCTOR
        ~Filter()
        {
        }

        //THIS FUNCTION RETURNS WHETHER OR NOT WE ARE CURRENTLY WORKING WITH _SCANSTATSEX.TXT
        public Boolean ScanStatsExBugFixer(String file_to_load)
        {
            int value = file_to_load.IndexOf("_ScanStatsEx.txt", StringComparison.OrdinalIgnoreCase);

            //IF FOUND RETURN TRUE
            if (value >= 0)
            {
                return true;
            }

            //ELSE RETURN FALSE
            return false;
        }

        //CREATE MYSQL BULK INSERT COMPATIBLE FILE
        public void parse_and_filter(String temp_file, String file_to_load)
        {
            //DECLARE VARIABLES
            string line;
            int line_num = 0;
            string query_info = "";
            Boolean bug_flag = false;

            //OPEN TEMP srInFile
            System.IO.StreamWriter swOutFile = new System.IO.StreamWriter(temp_file);

            //Console.WriteLine("WRITE TO: {0} ... LOAD FROM: {1}", temp_file, file_to_load);

            //CHECK TO SEE IF WE ARE NOW TRYING TO LOAD ScanStatsEx.txt
            if (ScanStatsExBugFixer(file_to_load))
            {
                //bug_flag = true;
            }

            StreamReader srInFile = new StreamReader(file_to_load);
            while ((line = srInFile.ReadLine()) != null)
            {
                //NEW LINE SO CLEAR QUERY_INFO
                query_info = "";
                
                //IF THE FIRST LINE ... SKIP OVER
                //THIS MEANS WE ARE SEEING OUR DB FIELDS [INSERT INTO `TABLE` ( `FIELD` ... ) VALUES
                if (line_num == 0)
                {
                    query_info += "instrument_id,random_id,";
                    //line_num++;
                    //continue;
                }



                //SPLIT GIVEN DATA FILES BY TAB
                char[] delimiters = new char[] { '\t' };

                //DO SPLIT OPERATION
                string[] parts = line.Split(delimiters, StringSplitOptions.None);

                //ADD INSTRUMENT ID + RANDOM ID
                if (line_num != 0)
                {
                    query_info += instrument_id + "," + random_id + ",";
                }

                //LOOP THROUGH ALL FIELDS (FORMATING CORRECTLY ALSO)
                for (int i = 0; i < parts.Length; i++)
                {
                    //IF WE ARE NOW ON ScanStatsEx.txt [==10 MEANS COLLISION MODE IS MISSING, i==7 == FIELD WE NEED TO ADD BEFORE]
                    if (bug_flag && parts.Length == 10 && i==7)
                    {
                        //HANDLE PARTS[] DIFFERENTLY
                        //APPEND ',' [NULL TO QUERY_INFO]
                        query_info += ",";

                        //Console.WriteLine("TEMP={0} && FULL={1} && LINE={2}", parts[7], parts.Length, line);
                        //Console.ReadLine()
                    }

                    if (parts[i].Equals("[PAD]"))
                    {
                        query_info += ",";
                    }
                    else
                    {
                        //HERE WE SEE OUR CONTENT TO BE INSERTED
                        query_info += parts[i] + ",";
                    }

                    //IF AT END ... REMOVE + APPEND
                    if (i == (parts.Length - 1))
                    {
                        //REMOVE END "," CHARACTER
                        query_info = query_info.Substring(0, query_info.LastIndexOf(","));
                        
                        //ADD \r\n [USED FOR BULK MYSQL INSERT EXPLODE -> KEEP]
                        query_info += "\r\n";
                    }
                }

                //WRITE RECORD LINE TO srInFile
                swOutFile.Write(query_info);

                //INCREMENT OUR LINE #
                line_num++;
            }

            //CLOSE THE FILE HANDLES
            swOutFile.Close();
            srInFile.Close();
        }

        //THIS FUNCTION:
        //1. LOOPS THROUGH A VALID FILE LIST
        //2. Calls another function that loads that file and rewrites the \tab as ',' separated.
        //3. From the filename, determines the correct table to insert into, appends temp
        //4. Calls our bulk insert function
        public void LoadFilesAndInsertIntoDB(List<String> FileList, String[] valid_file_tables, String dataset)
        {

            //LOOP THROUGH EACH FILE
            for (int i = 0; i < FileList.Count; i++)
            {
                //STORES THE FULL PATH TO FILE + TEMP FILE NAME
                String file_info = FileList[i];							//FILENAME WE WANT TO LOAD INTO DB
				String temp_file = System.IO.Path.GetTempFileName();	//WRITE TO THIS FILE [TEMP FILE]
                String query_table = "temp";							//USED AS PREFIX PORTION OF TABLE

                //DETERMINE IF WE HAVE A TABLE TO INSERT INTO DEPENDING ON OUR INPUT FILENAME
                int j = return_file_table_position(file_info, valid_file_tables);

                //DOES THIS FILE NEED TO BE REFORMATED [VARIABLE COLUMN SUPPORT]
                if (DFF.handleFile(file_info, dataset))
                {
                    //YES

					//REBUILD [SAVE TO DFF.TempFilePath BY DEFAULT]
                    //DFF.handleRebuild(FileList[i]);

                    //SET FILE_INFO TO OUR REBUILT FILE NOW
					file_info = DFF.TempFilePath;
                }

                //PARSE + FORMAT FILE CORRECTLY FOR MYSQL BULK INSERT QUERIES
                parse_and_filter(temp_file, file_info);

                //IF VALID INSERT TABLE
                if (j >= 0)
                {
                    //WE NOW HAVE A ACCESS TO valid_file_tables[j] which starts with the prefix '_'
                    //APPEND temp [DB PREFIX] to this.
                    query_table += valid_file_tables[j];
                    Console.WriteLine("Populating Table {0}", query_table);

                    //INSERT INTO DB
                    mDBWrapper.BulkInsert(query_table, temp_file);
                }
                else
                {
                    //NOT A VALID .TXT FILE FROM OUR LIST!
                    //Console.WriteLine("ERROR file_table_pos={0}", file_info);
                }
                //DELETE TEMP FILE
                File.Delete(temp_file);
            }

        }

        //FUNCTION WILL SEARCH THROUGH A FILE NAME, ENSURING IT IS A VALID TABLE EXTENSION AND RETURNING
        //THE POSITION SO THAT IT CAN BE PASSED TO OUR DBINTERFACE/OTHER CLASSES FOR PROCESSING
        public int return_file_table_position(String filename, String[] valid_file_tables)
        {
            int x;

            //LOOP THROUGH ALL VALID FILE/TABLE EXTENSIONS
            for (int i = 0; i < valid_file_tables.Length; i++)
            {
                //IGNORE CASE SENSITIVE CHARS ... LOOP FOR A MATCH IN STRING
                // C:\ .... _ScanStat.txt = Start Index of _ScanStats.txt found, ignoring case-sensitivity
                x = filename.IndexOf(valid_file_tables[i], StringComparison.OrdinalIgnoreCase);

                //IF WE HAVE FOUND A VALID FILE EXTENSION ... X > 0
                if (x >= 0)
                {
                    //WE HAVE NOW FOUND SOMETHING LIKE ... '_ScanStats.txt', '_ScanStatsEx.txt' ...
                    String temp_string = filename.Substring(x);

                    //WE NEED TO FIND .txt
                    //TEMP STUFF TRYING TO FIND RIGHT FILE
                    int y = temp_string.LastIndexOf(".txt");

                    //ENSURE FOUND [ALWAYS SHOULD BE FOUND]
                    if (y >= 0)
                    {
                        //REMOVE .txt
                        temp_string = temp_string.Remove(y);

                        //NOW WE HAVE THE SAME INFO IN OUR temp_string as we should for valid_file_tables[i], except our temp_string has Caps

                        //IF WE FOUND A MATCH ... IGNORE CASE SENSITIVITY
                        if (valid_file_tables[i].Equals(temp_string, StringComparison.OrdinalIgnoreCase))
                        {
                            //Console.WriteLine("LOCATED ({0})! {1} -- {2}", y, i, temp_string);

                            //RETURN THE POSITION ID IN OUR FILE/TABLE LIST
                            return i;
                        }
                    }
                }
            }
            return -1;
        }

		protected void DBWrapper_ErrorEvent(string errorMessage)
		{
			m_SystemLogManager.addApplicationLog(errorMessage);
		}

    }
}
