using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.SQLite;
using System.IO;
using System.Data.Common;

namespace SMAQC
{
    class DBSQLite : DBInterface
    {
        //DECLARE VARIABLES
        private readonly SQLiteConnection conn;                                                  //SQLITE CONNECTION
        private string query;                                                           //QUERY TO RUN
        private SQLiteDataReader reader;                                                //SQLITE READER
        private readonly DBSQLiteTools SQLiteTools = new DBSQLiteTools();                        //CREATE DBSQLITE TOOLS OBJECT

		private int errorMsgCount;
		private Dictionary<string, int> dctErrorMessages;

		SQLiteCommand m_PHRPInsertCommand;
		Dictionary<string, int> m_PHRPFieldsForInsert;


		//EVENT
		public event DBWrapper.DBErrorEventHandler ErrorEvent;

        //CONSTRUCTOR
        public DBSQLite(string datasource)
        {

            // Make sure the SQLiteDB exists and that it contains the correct tables
			if (!File.Exists(datasource))
			{
				// Create the file, along with the tables
				SQLiteTools.create_tables(datasource);
			}

            conn = new SQLiteConnection("Data Source=" + datasource);
            
            // Open a connection to the database
            Open();

			// Create any missing tables
			SQLiteTools.create_missing_tables(conn);

        }

        //DESTRUCTOR
        ~DBSQLite()
        {
            try
            {
                conn.Close();
            }
            catch (NullReferenceException ex)
            {
				Console.WriteLine("Error closing the SQLite DB: " + ex.Message);
            }
        }

		/// <summary>
		/// Clear DB Temp Tables for all data
		/// </summary>
		/// <param name="db_tables"></param>
		public void ClearTempTables(string[] db_tables)
		{
			//LOOP THROUGH EACH TEMP TABLE
			foreach (string tableName in db_tables)
			{
				if (DBSQLiteTools.TableExists(conn, tableName))
				{
					//CREATE QUERY
					string temp_string = "DELETE FROM " + tableName + ";";

					//SET QUERY
					setQuery(temp_string);

					//CALL QUERY FUNCTION
					QueryNonQuery();
				}
			}
		}

	    /// <summary>
		/// Clear DB Temp Tables for given Random_ID value
		/// </summary>
		/// <param name="random_id"></param>
		/// <param name="db_tables"></param>
        public void ClearTempTables(string[] db_tables, int random_id)
	    {
		    //LOOP THROUGH EACH TEMP TABLE
		    foreach (string tableName in db_tables)
		    {
			    if (DBSQLiteTools.TableExists(conn, tableName))
			    {
				    //CREATE QUERY
					string temp_string = "DELETE FROM " + tableName + " WHERE random_id='" + random_id + "';";

				    //SET QUERY
				    setQuery(temp_string);

				    //CALL QUERY FUNCTION
				    QueryNonQuery();
			    }
		    }
	    }

	    public void setQuery(string myquery)
        {
            //SET QUERY TO PARAM
            query = myquery;
        }

        //FOR QUERIES THAT RETURN ROWS
        public SQLiteDataReader QueryReader()
        {
            var cmd = new SQLiteCommand(conn)
            {
	            CommandText = query
            };
	        var dbReader = cmd.ExecuteReader();

            return dbReader;
        }

        //FOR QUERIES SUCH AS INSERT/DELETE/UPDATE
        public Boolean QueryNonQuery()
        {            
            var cmd = new SQLiteCommand(conn)
            {
	            CommandText = query
            };
	        cmd.ExecuteNonQuery();

            return true;
        }

        //FOR QUERIES THAT RETURN A SINGLE VALUE
        public void QueryScalar()
        {
            var cmd = new SQLiteCommand(conn)
            {
	            CommandText = query
            };
	        cmd.ExecuteScalar();

        }

        //THIS FUNCTION OPENS A CONNECTION TO THE DB
        public void Open()
        {
            //OPEN SQLite CONN
            conn.Open();
        }

        public void BulkInsert(string insert_into_table, string file_to_read_from)
        {
            //FETCH FIELDS
            List<string> fieldNames = SQLiteBulkInsert_Fields(file_to_read_from);

			errorMsgCount = 0;
			if (dctErrorMessages == null)
				dctErrorMessages = new Dictionary<string, int>();
			else
				dctErrorMessages.Clear();

            //BUILD SQL LINE
            string sql = SQLiteBulkInsert_BuildSQL_Line(insert_into_table, fieldNames);
			string previousLine = String.Empty;

			using (SQLiteCommand mycommand = conn.CreateCommand())
			{
				mycommand.CommandText = "PRAGMA synchronous=OFF";
				ExecuteCommand(mycommand, -1);
			}

            using (DbTransaction dbTrans = conn.BeginTransaction())
            {
                using (SQLiteCommand mycommand = conn.CreateCommand())
                {

                    mycommand.CommandText = sql;

	                using (var file = new StreamReader(file_to_read_from))
	                {
		                string line;
		                int line_num = 0;
		                while ((line = file.ReadLine()) != null)
		                {
			                line_num++;

			                if (line_num == 1)
			                {
				                // HEADER LINE, SKIP IT
				                continue;
			                }

			                if (String.CompareOrdinal(line, previousLine) == 0)
			                {
				                // Duplicate line; skip it
				                continue;
			                }

			                //Console.WriteLine("LINE [{0}]", line);
			                //FETCH VALUES
			                string[] values = SQLiteBulkInsert_TokenizeLine(line);

			                //LOOP THROUGH FIELD LISTING + SET PARAMETERS
			                for (int i = 0; i < fieldNames.Count; i++)
			                {

				                mycommand.Parameters.AddWithValue("@" + i, values[i]);
			                }

			                //NOW THAT ALL FIELDS + VALUES ARE IN OUR SYSTEM

			                ExecuteCommand(mycommand, line_num);

			                previousLine = String.Copy(line);

		                }
	                }

                }
                dbTrans.Commit();

				if (dctErrorMessages.Count > 0)
				{
					string firstErrorMsg = String.Empty;
					int totalErrorRows = 0;

					foreach (KeyValuePair<string, int> kvEntry in dctErrorMessages)
					{
						totalErrorRows += kvEntry.Value;
						OnErrorEvent("Error message count = " + kvEntry.Value + " for '" + kvEntry.Key + "'");
						if (String.IsNullOrEmpty(firstErrorMsg))
							firstErrorMsg = String.Copy(kvEntry.Key);
					}

					string msg = "Errors during BulkInsert from file " + Path.GetFileName(file_to_read_from) + "; problem with " + totalErrorRows + " row";
					if (totalErrorRows != 1)
						msg += "s";

					msg += "; " + firstErrorMsg;

					throw new Exception(msg);
				}
            }
        }

		protected bool ExecuteCommand(SQLiteCommand mycommand, int line_num)
		{
			try
			{
				mycommand.ExecuteNonQuery();
			}
			catch (Exception ex)
			{
				string msg = ex.Message.Replace("\r\n", ": ");

				errorMsgCount = 1;
				if (dctErrorMessages.TryGetValue(msg, out errorMsgCount))
					dctErrorMessages[msg] = errorMsgCount + 1;
				else
					dctErrorMessages.Add(msg, 1);

				if (errorMsgCount < 10)
					OnErrorEvent("Error inserting row " + line_num + ": " + msg);

				return false;
			}

			return true;
		}

        //INIT READER [WHENEVER WE WANT TO READ A ROW]
        public void initReader()
        {
            //CALL QUERY READER
            reader = QueryReader();
        }


		public bool InitPHRPInsertCommand(out DbTransaction dbTrans)
		{
			m_PHRPFieldsForInsert = new Dictionary<string, int>();

			m_PHRPInsertCommand = conn.CreateCommand();

			var fields = new List<string>
			{
				"instrument_id",
				"random_id",
				"Result_ID",
				"Scan",
				"CollisionMode",
				"Charge",
				"Peptide_MH",
				"Peptide_Sequence",
				"DelM_Da",
				"DelM_PPM",
				"MSGFSpecProb",
				"Unique_Seq_ID",
				"Cleavage_State",
				"Phosphopeptide"
			};

			m_PHRPInsertCommand.CommandText = SQLiteBulkInsert_BuildSQL_Line("temp_PSMs", fields);

			for (int i = 0; i < fields.Count; i++)
			{
				m_PHRPFieldsForInsert.Add(fields[i], i);
			}

			errorMsgCount = 0;
			if (dctErrorMessages == null)
				dctErrorMessages = new Dictionary<string, int>();
			else
				dctErrorMessages.Clear();

			using (SQLiteCommand mycommand = conn.CreateCommand())
			{
				mycommand.CommandText = "PRAGMA synchronous=OFF";
				ExecuteCommand(mycommand, -1);
			}

			dbTrans = conn.BeginTransaction();

			return true;

		}

		public void ExecutePHRPInsert(Dictionary<string, string> dctData, int line_num)
		{
			m_PHRPInsertCommand.Parameters.Clear();

			// Update insertCommand to have the data value for each field
			foreach (KeyValuePair<string, int> item in m_PHRPFieldsForInsert)
			{
				string dataValue;
				if (!dctData.TryGetValue(item.Key, out dataValue))
				{
					dataValue = string.Empty;
				}

				m_PHRPInsertCommand.Parameters.AddWithValue("@" + item.Value, dataValue);
			}

			// Run the command
			ExecuteCommand(m_PHRPInsertCommand, line_num);

		}


        //READ SINGLE DB ROW [DIFFERENT FROM readLines() as here we close reader afterward]
        //[RETURNS FALSE IF NO FURTHER ROWS TO READ]
		public Boolean readSingleLine(string[] fields, ref Dictionary<string, string> dctData)
        {
			//READ LINE
            bool status = reader.Read();

            //IF RETURNED FALSE ... NO ROWS TO RETURN
            if (!status)
            {
                //CLOSE READER
                reader.Close();

                //RETURN FALSE AS NO MORE TO READ
                return false;
            }

            //DECLARE VARIABLES
            foreach (string fieldName in fields)
            {
	            try
	            {
					int value = reader.GetOrdinal(fieldName);
					dctData.Add(fieldName, reader.GetValue(value).ToString());
	            }
	            catch (System.Data.SqlTypes.SqlNullValueException)
	            {

	            }
            }

			//CLOSE READER
            reader.Close();

            //RETURN TRUE SINCE READ == OK
            return true;
        }

        //READ DB ROW(s) [RETURNS FALSE IF NO FURTHER ROWS TO READ]
		public Boolean readLines(string[] fields, ref Dictionary<string, string> dctData)
        {
			//READ LINE
            bool status = reader.Read();

            //IF RETURNED FALSE ... NO ROWS TO RETURN
            if (!status)
            {
                //CLOSE READER
                reader.Close();

                //RETURN FALSE AS NO MORE TO READ
                return false;
            }

            //DECLARE VARIABLES
            foreach (string fieldName in fields)
            {
	            //READ + STORE FIELD [Value, Result]
	            try
	            {
					int value = reader.GetOrdinal(fieldName);
					dctData.Add(fieldName, reader.GetValue(value).ToString());
	            }
	            catch (System.Data.SqlTypes.SqlNullValueException)
	            {

	            }
            }

			return true;
        }

		List<string> SQLiteBulkInsert_Fields(string filename)
        {
            var file = new StreamReader(filename);
            string line = file.ReadLine();
            file.Close();

			if (string.IsNullOrWhiteSpace(line))
				return new List<string>();

            //SPLIT GIVEN DATA FILES BY TAB
            var delimiters = new[] { '\t' };

            //DO SPLIT OPERATION
			List<string> parts = line.Split(delimiters, StringSplitOptions.None).ToList();

            for (int i = 0; i < parts.Count; i++)
            {
                parts[i] = SQLiteBulkInsert_CleanFields(parts[i]);
            }

            return parts;
        }

        string[] SQLiteBulkInsert_TokenizeLine(string line)
        {
            //SPLIT GIVEN DATA FILES BY TAB
            var delimiters = new[] { '\t' };

            //IF LINE CONTAINS "\t\t", THIS MEANS AN EMPTY FIELD; REPLACE WITH "\t \t"
            while (line.Contains("\t\t"))
            {
                line = line.Replace("\t\t", "\t \t");
            }

            //DO SPLIT OPERATION
            string[] parts = line.Split(delimiters, StringSplitOptions.None);

            return parts;
        }

		string SQLiteBulkInsert_BuildSQL_Line(string table, List<string> fields)
        {
            var sbSql = new System.Text.StringBuilder();

            //BUILD BASE
            sbSql.Append("INSERT INTO " + table + " (");

            //BUILD COMMANDS
			for (int i = 0; i < fields.Count; i++)
			{
				sbSql.Append("`" + fields[i] + "`");
				if (i < fields.Count - 1)
					sbSql.Append(",");					
            }

            //ADD END OF COMMANDS
            sbSql.Append(") VALUES (");

            //BUILD VALUE LIST
            for (int i = 0; i < fields.Count; i++)
            {
                sbSql.Append("@" + i);
				if (i < fields.Count - 1)
					sbSql.Append(",");
            }

            //ADD END OF VALUES
            sbSql.Append(");");

			return sbSql.ToString();
        }

        string SQLiteBulkInsert_CleanFields(string field)
        {
            string field_line = field;

            //IF == SPACE
            if (field_line.Contains(" "))
            {
                //REPLACE BLANKS WITH _
                field_line = field_line.Replace(" ", "_");
            }

            //REPLACE ALL INSTANCES OF _(
            while (field_line.IndexOf("_(") > 0)
            {
                field_line = field_line.Remove(field_line.IndexOf("_("));
            }

            //REPLACE ALL INSTANCES OF (
            while (field_line.IndexOf("(") > 0)
            {
                field_line = field_line.Remove(field_line.IndexOf("("));
            }

            return field_line;
        }

        public string getDateTime()
        {
            return "strftime('%Y-%m-%d %H:%M:%S','now', 'localtime')";
        }

		void OnErrorEvent(string errorMessage)
		{
			if (ErrorEvent != null)
			{
				ErrorEvent(errorMessage);
			}
		}

	}
}
