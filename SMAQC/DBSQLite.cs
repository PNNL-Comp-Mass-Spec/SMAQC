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
        private readonly SQLiteConnection mConnection;                                         // Sqlite connection
        private string query;                                                           // Query to run
        private SQLiteDataReader reader;                                                // Sqlite reader
        private readonly DBSQLiteTools SQLiteTools = new DBSQLiteTools();               // Create dbsqlite tools object

        private int errorMsgCount;
        private Dictionary<string, int> dctErrorMessages;

        SQLiteCommand m_PHRPInsertCommand;
        Dictionary<string, int> m_PHRPFieldsForInsert;


        // Event
        public event DBWrapper.DBErrorEventHandler ErrorEvent;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasource"></param>
        public DBSQLite(string datasource)
        {

            // Make sure the sqlitedb exists and that it contains the correct tables
            if (!File.Exists(datasource))
            {
                // Create the file, along with the tables
                SQLiteTools.CreateTables(datasource);
            }

            mConnection = new SQLiteConnection("Data Source=" + datasource, true);
            
            // Open a connection to the database
            Open();

            // Create any missing tables and add any missing columns
            SQLiteTools.create_missing_tables(mConnection);

        }       

        // / <Summary>
        // / Clear db temp tables for all data
        // / </Summary>
        // / <Param name="db_tables"></param>
        public void ClearTempTables(string[] db_tables)
        {
            // Loop through each temp table
            foreach (var tableName in db_tables)
            {
                if (!DBSQLiteTools.TableExists(mConnection, tableName))
                    continue;

                var sql = "DELETE FROM " + tableName + ";";

                SetQuery(sql);

                ExecuteNonQuery();
            }
        }

        // / <Summary>
        // / Clear db temp tables for given random_id value
        // / </Summary>
        // / <Param name="random_id"></param>
        // / <Param name="db_tables"></param>
        public void ClearTempTables(string[] db_tables, int random_id)
        {
            // Loop through each temp table
            foreach (var tableName in db_tables)
            {
                if (!DBSQLiteTools.TableExists(mConnection, tableName))
                    continue;

                var sql = "DELETE FROM " + tableName + " WHERE random_id='" + random_id + "';";

                SetQuery(sql);

                ExecuteNonQuery();
            }
        }

        public void SetQuery(string myquery)
        {
            // Set query to param
            query = myquery;
        }

        // For queries that return rows
        public SQLiteDataReader QueryReader()
        {
            var cmd = new SQLiteCommand(mConnection)
            {
                CommandText = query
            };
            var dbReader = cmd.ExecuteReader();

            return dbReader;
        }

        // For queries such as insert/delete/update
        public bool ExecuteNonQuery()
        {            
            var cmd = new SQLiteCommand(mConnection)
            {
                CommandText = query
            };
            cmd.ExecuteNonQuery();

            return true;
        }

        // For queries that return a single value
        public void QueryScalar()
        {
            var cmd = new SQLiteCommand(mConnection)
            {
                CommandText = query
            };
            cmd.ExecuteScalar();

        }

        // This function opens a connection to the db
        public void Open()
        {
            // Open sqlite conn
            mConnection.Open();
        }

        public void BulkInsert(string targetTable, string sourceFile, List<string> excludedFieldNameSuffixes)
        {
            // Keys in this dictionary are field index values (0, 1, 2, etc.)
            // Values are True if the field should be used and false if it should not be used
            Dictionary<int, bool> fieldEnabledByIndex;

            // Fetch fields
            var fieldNames = SQLiteBulkInsert_Fields(sourceFile, excludedFieldNameSuffixes, out fieldEnabledByIndex);

            if (string.Equals(targetTable, "temp_reporterions", StringComparison.InvariantCultureIgnoreCase))
            {
                // Auto-add the reporter ion columns
                var ionColumns = (from item in fieldNames where item.ToLower().StartsWith("ion_") select item).ToList();

                if (ionColumns.Count > 0)
                {
                    SQLiteTools.AssureColumnsExist(mConnection, "temp_reporterions", ionColumns, "FLOAT");
                }
            }

            errorMsgCount = 0;
            if (dctErrorMessages == null)
                dctErrorMessages = new Dictionary<string, int>();
            else
                dctErrorMessages.Clear();

            // Build sql line
            var sql = SQLiteBulkInsert_BuildSQL_Line(targetTable, fieldNames);
            var previousLine = string.Empty;

            using (var mycommand = mConnection.CreateCommand())
            {
                mycommand.CommandText = "PRAGMA synchronous=OFF";
                ExecuteCommand(mycommand, -1);
            }

            using (DbTransaction dbTrans = mConnection.BeginTransaction())
            {
                using (var mycommand = mConnection.CreateCommand())
                {

                    mycommand.CommandText = sql;

                    using (var file = new StreamReader(sourceFile))
                    {
                        var line_num = 0;
                        while (!file.EndOfStream)
                        {
                            var line = file.ReadLine();
                            if (string.IsNullOrEmpty(line))
                                continue;

                            line_num++;

                            if (line_num == 1)
                            {
                                // Header line, skip it
                                continue;
                            }

                            if (string.CompareOrdinal(line, previousLine) == 0)
                            {
                                // Duplicate line; skip it
                                continue;
                            }

                            // Console.writeline("line [{0}]", line);
                            // Fetch values
                            var values = SQLiteBulkInsert_TokenizeLine(line);

                            // Loop through field listing + set parameters
                            for (var i = 0; i < fieldNames.Count; i++)
                            {
                                if (fieldEnabledByIndex[i])
                                {
                                    mycommand.Parameters.AddWithValue("@" + i, values[i]);
                                }
                            }

                            // Now that all fields + values are in our system

                            ExecuteCommand(mycommand, line_num);

                            previousLine = string.Copy(line);

                        }
                    }

                }
                dbTrans.Commit();

                if (dctErrorMessages.Count > 0)
                {
                    var firstErrorMsg = string.Empty;
                    var totalErrorRows = 0;

                    foreach (var kvEntry in dctErrorMessages)
                    {
                        totalErrorRows += kvEntry.Value;
                        OnErrorEvent("Error message count = " + kvEntry.Value + " for '" + kvEntry.Key + "'");
                        if (string.IsNullOrEmpty(firstErrorMsg))
                            firstErrorMsg = string.Copy(kvEntry.Key);
                    }

                    var msg = "Errors during BulkInsert from file " + Path.GetFileName(sourceFile) + "; problem with " + totalErrorRows + " row";
                    if (totalErrorRows != 1)
                        msg += "s";

                    msg += "; " + firstErrorMsg;

                    throw new Exception(msg);
                }
            }
        }

        private void ExecuteCommand(SQLiteCommand mycommand, int line_num)
        {
            try
            {
                mycommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                var msg = ex.Message.Replace(Environment.NewLine, ": ");

                errorMsgCount = 1;
                if (dctErrorMessages.TryGetValue(msg, out errorMsgCount))
                    dctErrorMessages[msg] = errorMsgCount + 1;
                else
                    dctErrorMessages.Add(msg, 1);

                if (errorMsgCount < 10)
                    OnErrorEvent("Error inserting row " + line_num + ": " + msg);
                
            }

        }

        /// <summary>
        /// Return the columns defined for the given table
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public List<string> GetTableColumns(string tableName)
        {
            var columns = new List<string>();

            using (var cmd = mConnection.CreateCommand())
            {
                cmd.CommandText = "SELECT * FROM [" + tableName + "] LIMIT 1";

                using (var sqlReader = cmd.ExecuteReader())
                {
                    for (var i = 0; i < sqlReader.FieldCount; i++)
                    {
                        columns.Add(sqlReader.GetName(i));
                    }
                }
            }

            return columns;
        }

        // Init reader [whenever we want to read a row]
        public void InitReader()
        {
            // Call query reader
            reader = QueryReader();
        }


        public bool InitPHRPInsertCommand(out DbTransaction dbTrans)
        {
            m_PHRPFieldsForInsert = new Dictionary<string, int>();

            m_PHRPInsertCommand = mConnection.CreateCommand();

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
                "Phosphopeptide",
                "Keratinpeptide",
                "MissedCleavages",
                "Trypsinpeptide"
            };

            m_PHRPInsertCommand.CommandText = SQLiteBulkInsert_BuildSQL_Line("temp_PSMs", fields);

            for (var i = 0; i < fields.Count; i++)
            {
                m_PHRPFieldsForInsert.Add(fields[i], i);
            }

            errorMsgCount = 0;
            if (dctErrorMessages == null)
                dctErrorMessages = new Dictionary<string, int>();
            else
                dctErrorMessages.Clear();

            using (var mycommand = mConnection.CreateCommand())
            {
                mycommand.CommandText = "PRAGMA synchronous=OFF";
                ExecuteCommand(mycommand, -1);
            }

            dbTrans = mConnection.BeginTransaction();

            return true;

        }

        public void ExecutePHRPInsert(Dictionary<string, string> dctData, int line_num)
        {
            m_PHRPInsertCommand.Parameters.Clear();

            // Update insertcommand to have the data value for each field
            foreach (var item in m_PHRPFieldsForInsert)
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


        // Read single db row [different from readlines() as here we close reader afterward]
        // [Returns false if no further rows to read]
        public bool ReadSingleLine(string[] fields, ref Dictionary<string, string> dctData)
        {
            // Read line
            var status = reader.Read();

            // If returned false ... no rows to return
            if (!status)
            {
                // Close reader
                reader.Close();

                // Return false as no more to read
                return false;
            }

            foreach (var fieldName in fields)
            {
                try
                {
                    var value = reader.GetOrdinal(fieldName);
                    dctData.Add(fieldName, reader.GetValue(value).ToString());
                }
                catch (System.Data.SqlTypes.SqlNullValueException)
                {

                }
            }

            // Close reader
            reader.Close();

            // Return true since read == ok
            return true;
        }

        // Read db row(s) [returns false if no further rows to read]
        public bool ReadLines(string[] fields, ref Dictionary<string, string> dctData)
        {
            // Read line
            var status = reader.Read();

            // If returned false ... no rows to return
            if (!status)
            {
                // Close reader
                reader.Close();

                // Return false as no more to read
                return false;
            }

            foreach (var fieldName in fields)
            {
                // Read + store field [value, result]
                try
                {
                    var value = reader.GetOrdinal(fieldName);
                    dctData.Add(fieldName, reader.GetValue(value).ToString());
                }
                catch (System.Data.SqlTypes.SqlNullValueException)
                {

                }
            }

            return true;
        }

        List<string> SQLiteBulkInsert_Fields(
            string filename, 
            List<string> excludedFieldNameSuffixes, 
            out Dictionary<int, bool> fieldEnabledByIndex)
        {
            var file = new StreamReader(filename);
            var line = file.ReadLine();
            file.Close();

            // Keys in this dictionary are field index values (0, 1, 2, etc.)
            // Values are True if the field should be used and false if it should not be used
            fieldEnabledByIndex = new Dictionary<int, bool>();

            if (string.IsNullOrWhiteSpace(line))
                return new List<string>();

            // Split given data files by tab
            var delimiters = new[] { '\t' };

            // Do split operation
            var parts = line.Split(delimiters, StringSplitOptions.None).ToList();


            var filteredList = new List<string>();
            for (var fieldIndex = 0; fieldIndex < parts.Count; fieldIndex++)
            {
                var fieldName = parts[fieldIndex];

                var addField = true;
                foreach (var suffix in excludedFieldNameSuffixes)
                {
                    if (fieldName.EndsWith(suffix, StringComparison.InvariantCultureIgnoreCase))
                    {
                        addField = false;
                        break;
                    }
                }

                if (addField)
                {
                    filteredList.Add(fieldName);
                    fieldEnabledByIndex[fieldIndex] = true;
                }
            }

            // Make sure the field names do not have spaces or parentheses in them
            for (var i = 0; i < filteredList.Count; i++)
            {
                filteredList[i] = SQLiteBulkInsert_CleanFields(filteredList[i]);
            }

            return filteredList;
        }

        List<string> SQLiteBulkInsert_TokenizeLine(string line)
        {
            // Split given data files by tab
            var delimiters = new[] { '\t' };

            // If line contains "\t\t", this means an empty field; replace with "\t \t"
            while (line.Contains("\t\t"))
            {
                line = line.Replace("\t\t", "\t \t");
            }

            // Do split operation
            var parts = line.Split(delimiters, StringSplitOptions.None).ToList();

            return parts;
        }

        string SQLiteBulkInsert_BuildSQL_Line(string table, List<string> fields)
        {
            var sbSql = new System.Text.StringBuilder();

            // Build base
            sbSql.Append("INSERT INTO " + table + " (");

            // Build commands
            for (var i = 0; i < fields.Count; i++)
            {
                sbSql.Append("`" + fields[i] + "`");
                if (i < fields.Count - 1)
                    sbSql.Append(",");					
            }

            // Add end of commands
            sbSql.Append(") VALUES (");

            // Build value list
            for (var i = 0; i < fields.Count; i++)
            {
                sbSql.Append("@" + i);
                if (i < fields.Count - 1)
                    sbSql.Append(",");
            }

            // Add end of values
            sbSql.Append(");");

            return sbSql.ToString();
        }

        // / <Summary>
        // / Replace invalid characters in field names
        // / </Summary>
        // / <Param name="field"></param>
        // / <Returns></returns>
        string SQLiteBulkInsert_CleanFields(string field)
        {
            var field_line = field;

            // If == space
            if (field_line.Contains(" "))
            {
                // Replace blanks with _
                field_line = field_line.Replace(" ", "_");
            }

            // Replace all instances of _(
            while (field_line.Contains("_("))
            {
                field_line = field_line.Remove(field_line.IndexOf("_(", StringComparison.Ordinal));
            }

            // Replace all instances of (
            while (field_line.Contains("("))
            {
                field_line = field_line.Remove(field_line.IndexOf("(", StringComparison.Ordinal));
            }

            return field_line;
        }

        public string GetDateTime()
        {
            return "strftime('%Y-%m-%d %H:%M:%S','now', 'localtime')";
        }

        void OnErrorEvent(string errorMessage)
        {
            ErrorEvent?.Invoke(errorMessage);
        }

    }
}
