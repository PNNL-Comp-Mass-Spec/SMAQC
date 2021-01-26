using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Data.SQLite;
using System.IO;
using System.Data.Common;

namespace SMAQC
{
    internal class DBSQLite : IDBInterface
    {
        // Ignore Spelling: Da, strftime, localtime

        /// <summary>
        /// SQLite connection
        /// </summary>
        private readonly SQLiteConnection mConnection;

        /// <summary>
        /// Query to run
        /// </summary>
        private string mQuery;

        /// <summary>
        /// SQLite reader
        /// </summary>
        private SQLiteDataReader mSQLiteReader;

        /// <summary>
        /// DBSQLite tools object
        /// </summary>
        private readonly DBSQLiteTools mSQLiteTools = new DBSQLiteTools();

        private int mErrorMsgCount;

        private Dictionary<string, int> mErrorMessages;

        private SQLiteCommand mPHRPInsertCommand;

        private Dictionary<string, int> mPHRPFieldsForInsert;

        /// <summary>
        /// Error event
        /// </summary>
        public event DBWrapper.DBErrorEventHandler ErrorEvent;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dbPath">Path to the SQLite database</param>
        public DBSQLite(string dbPath)
        {
            // Make sure the SQLite database exists and that it contains the correct tables
            if (!File.Exists(dbPath))
            {
                // Create the file, along with the tables
                mSQLiteTools.CreateTables(dbPath);
            }

            mConnection = new SQLiteConnection("Data Source=" + dbPath, true);

            // Open a connection to the database
            Open();

            // Create any missing tables and add any missing columns
            mSQLiteTools.CreateMissingTables(mConnection);
        }

        /// <summary>
        /// Clear database temp tables for all data
        /// </summary>
        /// <param name="tableNames"></param>
        public void ClearTempTables(string[] tableNames)
        {
            foreach (var tableName in tableNames)
            {
                if (!DBSQLiteTools.TableExists(mConnection, tableName))
                    continue;

                var sql = "DELETE FROM " + tableName + ";";

                SetQuery(sql);

                ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Clear database temp tables for given randomId value
        /// </summary>
        /// <param name="tableNames">Table names</param>
        /// <param name="randomId">Random ID for this analysis</param>
        public void ClearTempTables(string[] tableNames, int randomId)
        {
            foreach (var tableName in tableNames)
            {
                if (!DBSQLiteTools.TableExists(mConnection, tableName))
                    continue;

                var sql = "DELETE FROM " + tableName + " WHERE random_id='" + randomId + "';";

                SetQuery(sql);

                ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Define the query to run
        /// </summary>
        /// <param name="query"></param>
        public void SetQuery(string query)
        {
            mQuery = query;
        }

        /// <summary>
        /// Run a query that does not return results (insert/delete/update)
        /// </summary>
        /// <returns>Always returns true</returns>
        /// <remarks>Call SetQuery prior to calling this method</remarks>
        public bool ExecuteNonQuery()
        {
            if (string.IsNullOrWhiteSpace(mQuery))
            {
                throw new Exception("Call SetQuery prior to calling ExecuteNonQuery");
            }

            var cmd = new SQLiteCommand(mConnection)
            {
                CommandText = mQuery
            };
            cmd.ExecuteNonQuery();

            // Clear mQuery, for safety (to prevent the same query being run twice)
            mQuery = string.Empty;

            return true;
        }

        /// <summary>
        /// This function opens a connection to the database
        /// </summary>
        public void Open()
        {
            mConnection.Open();
        }

        /// <summary>
        /// Bulk insert a set of data from sourceFile
        /// </summary>
        /// <param name="targetTable">Target table</param>
        /// <param name="sourceFile">Source file</param>
        /// <param name="excludedColumnNameSuffixes">Columns that end in any of these suffixes will be skipped</param>
        public void BulkInsert(string targetTable, string sourceFile, List<string> excludedColumnNameSuffixes)
        {
            // Keys in this dictionary are field index values (0, 1, 2, etc.)
            // Values are True if the field should be used and false if it should not be used
            var fieldNames = GetColumnsForSQLiteBulkInsert(sourceFile, excludedColumnNameSuffixes, out var fieldEnabledByIndex);

            if (string.Equals(targetTable, "temp_ReporterIons", StringComparison.OrdinalIgnoreCase))
            {
                // Auto-add the reporter ion columns
                var ionColumns = (from item in fieldNames where item.StartsWith("ion_", StringComparison.OrdinalIgnoreCase) select item).ToList();

                if (ionColumns.Count > 0)
                {
                    mSQLiteTools.AssureColumnsExist(mConnection, "temp_ReporterIons", ionColumns, "FLOAT");
                }
            }

            mErrorMsgCount = 0;
            if (mErrorMessages == null)
                mErrorMessages = new Dictionary<string, int>();
            else
                mErrorMessages.Clear();

            // Build SQL line
            var sql = SQLiteBulkInsert_BuildSQL_Line(targetTable, fieldNames);
            var previousLine = string.Empty;

            using (var myCommand = mConnection.CreateCommand())
            {
                myCommand.CommandText = "PRAGMA synchronous=OFF";
                ExecuteCommand(myCommand, -1);
            }

            using (DbTransaction dbTrans = mConnection.BeginTransaction())
            {
                using (var myCommand = mConnection.CreateCommand())
                {
                    myCommand.CommandText = sql;

                    using (var file = new StreamReader(sourceFile))
                    {
                        var lineNumber = 0;
                        while (!file.EndOfStream)
                        {
                            var line = file.ReadLine();
                            if (string.IsNullOrEmpty(line))
                                continue;

                            lineNumber++;

                            if (lineNumber == 1)
                            {
                                // Header line, skip it
                                continue;
                            }

                            if (string.CompareOrdinal(line, previousLine) == 0)
                            {
                                // Duplicate line; skip it
                                continue;
                            }

                            // Console.WriteLine("line [{0}]", line);
                            // Fetch values
                            var values = SQLiteBulkInsert_TokenizeLine(line);

                            // Loop through field listing + set parameters
                            for (var i = 0; i < fieldNames.Count; i++)
                            {
                                if (fieldEnabledByIndex[i])
                                {
                                    myCommand.Parameters.AddWithValue("@" + i, values[i]);
                                }
                            }

                            // Now that all fields + values are in our system

                            ExecuteCommand(myCommand, lineNumber);

                            previousLine = string.Copy(line);
                        }
                    }
                }
                dbTrans.Commit();

                if (mErrorMessages.Count > 0)
                {
                    var firstErrorMsg = string.Empty;
                    var totalErrorRows = 0;

                    foreach (var kvEntry in mErrorMessages)
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

        private void ExecuteCommand(IDbCommand myCommand, int lineNumber)
        {
            try
            {
                myCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                var msg = ex.Message.Replace(Environment.NewLine, ": ");

                mErrorMsgCount = 1;
                if (mErrorMessages.TryGetValue(msg, out mErrorMsgCount))
                    mErrorMessages[msg] = mErrorMsgCount + 1;
                else
                    mErrorMessages.Add(msg, 1);

                if (mErrorMsgCount < 10)
                    OnErrorEvent("Error inserting row " + lineNumber + ": " + msg);
            }
        }

        /// <summary>
        /// Return the columns defined for the given table
        /// </summary>
        /// <param name="tableName"></param>
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

        /// <summary>
        /// Run the query defined by SetQuery, thereby initializing a reader for retrieving the results
        /// </summary>
        /// <remarks>Call SetQuery prior to calling this method</remarks>
        public void InitReader()
        {
            if (string.IsNullOrWhiteSpace(mQuery))
            {
                throw new Exception("Call SetQuery prior to calling InitReader");
            }

            var cmd = new SQLiteCommand(mConnection)
            {
                CommandText = mQuery
            };
            mSQLiteReader = cmd.ExecuteReader();

            // Clear mQuery, for safety (to prevent the same query being run twice)
            mQuery = string.Empty;
        }

        /// <summary>
        /// Initialize the command for inserting PHRP data
        /// </summary>
        /// <param name="dbtrans"></param>
        public bool InitPHRPInsertCommand(out DbTransaction dbTrans)
        {
            mPHRPFieldsForInsert = new Dictionary<string, int>();

            mPHRPInsertCommand = mConnection.CreateCommand();

            var dbColumns = new List<string>
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
                "KeratinPeptide",
                "MissedCleavages",
                "TrypsinPeptide"
            };

            mPHRPInsertCommand.CommandText = SQLiteBulkInsert_BuildSQL_Line("temp_PSMs", dbColumns);

            for (var i = 0; i < dbColumns.Count; i++)
            {
                mPHRPFieldsForInsert.Add(dbColumns[i], i);
            }

            mErrorMsgCount = 0;
            if (mErrorMessages == null)
                mErrorMessages = new Dictionary<string, int>();
            else
                mErrorMessages.Clear();

            using (var myCommand = mConnection.CreateCommand())
            {
                myCommand.CommandText = "PRAGMA synchronous=OFF";
                ExecuteCommand(myCommand, -1);
            }

            dbTrans = mConnection.BeginTransaction();

            return true;
        }

        /// <summary>
        /// Add new PHRP data
        /// </summary>
        /// <param name="dctdata"></param>
        /// <param name="line_num"></param>
        public void ExecutePHRPInsert(Dictionary<string, string> dctData, int lineNumber)
        {
            if (mPHRPInsertCommand == null)
            {
                throw new Exception("Error initializing the parameters for the insert command; mPHRPInsertCommand is null");
            }

            if (mPHRPInsertCommand.Parameters == null ||
                mPHRPInsertCommand.Parameters.Count != mPHRPFieldsForInsert.Count)
            {
                // Create the parameters
                // Names are @0, @1, etc.
                foreach (var item in mPHRPFieldsForInsert)
                {
                    mPHRPInsertCommand.Parameters?.AddWithValue("@" + item.Value, string.Empty);
                }
            }

            if (mPHRPInsertCommand.Parameters == null)
            {
                throw new Exception("Error initializing the parameters for the insert command");
            }

            // Update the values for the insert command parameters
            foreach (var item in mPHRPFieldsForInsert)
            {
                if (!dctData.TryGetValue(item.Key, out var dataValue))
                {
                    dataValue = string.Empty;
                }

                mPHRPInsertCommand.Parameters[item.Value].Value = dataValue;
            }

            // Run the command
            ExecuteCommand(mPHRPInsertCommand, lineNumber);
        }

        /// <summary>
        /// Read a single database row
        /// </summary>
        /// <param name="fields"></param>
        /// <param name="dctData"></param>
        /// <returns>True if success, false if no further rows to read</returns>
        /// <remarks>This method differs from ReadNextRow since here we close the reader after reading a single row of data</remarks>
        public bool ReadSingleLine(string[] fields, out Dictionary<string, string> dctData)
        {
            dctData = new Dictionary<string, string>();

            var status = mSQLiteReader.Read();

            if (!status)
                // No more rows to read
            {
                mSQLiteReader.Close();
                return false;
            }

            foreach (var fieldName in fields)
            {
                try
                {
                    var value = mSQLiteReader.GetOrdinal(fieldName);
                    dctData.Add(fieldName, mSQLiteReader.GetValue(value).ToString());
                }
                catch (System.Data.SqlTypes.SqlNullValueException)
                {
                    // Ignore null values
                }
            }

            mSQLiteReader.Close();

            return true;
        }

        /// <summary>
        /// Read data for one database row
        /// </summary>
        /// <param name="fields"></param>
        /// <param name="dctData"></param>
        /// <returns>True if success, false if no further rows to read</returns>
        public bool ReadNextRow(string[] fields, out Dictionary<string, string> dctData)
        {
            dctData = new Dictionary<string, string>();

            // Read line
            var status = mSQLiteReader.Read();

            if (!status)
            {
                // No more rows to read
                mSQLiteReader.Close();
                return false;
            }

            foreach (var fieldName in fields)
            {
                try
                {
                    var value = mSQLiteReader.GetOrdinal(fieldName);
                    dctData.Add(fieldName, mSQLiteReader.GetValue(value).ToString());
                }
                catch (System.Data.SqlTypes.SqlNullValueException)
                {
                    // Ignore null values
                }
            }

            return true;
        }

        /// <summary>
        /// Read the header line in the source file and determine the column names to load
        /// </summary>
        /// <param name="filePath">Source file path</param>
        /// <param name="excludedColumnNameSuffixes">Columns that end in any of these suffixes will be skipped</param>
        /// <param name="fieldEnabledByIndex"></param>
        /// <returns>List of database column names</returns>
        private List<string> GetColumnsForSQLiteBulkInsert(
            string filePath,
            IReadOnlyCollection<string> excludedColumnNameSuffixes,
            out Dictionary<int, bool> fieldEnabledByIndex)
        {
            // Keys in this dictionary are field index values (0, 1, 2, etc.)
            // Values are True if the field should be used and false if it should not be used
            fieldEnabledByIndex = new Dictionary<int, bool>();

            string headerLine;

            using (var file = new StreamReader(filePath))
            {
                if (file.EndOfStream)
                {
                    Console.WriteLine("Warning: file is empty; cannot bulk insert from " + filePath);
                    return new List<string>();
                }

                headerLine = file.ReadLine();
            }

            if (string.IsNullOrWhiteSpace(headerLine))
            {
                Console.WriteLine("Warning: file is empty; cannot bulk insert from " + filePath);
                return new List<string>();
            }

            // Split the header line on tabs
            var delimiters = new[] { '\t' };

            var columnNames = headerLine.Split(delimiters, StringSplitOptions.None).ToList();

            var filteredList = new List<string>();
            var columnIndex = -1;
            foreach (var columnName in columnNames)
            {
                columnIndex++;

                var addColumn = excludedColumnNameSuffixes.All(suffix => !columnName.EndsWith(suffix, StringComparison.OrdinalIgnoreCase));

                if (!addColumn)
                    continue;

                filteredList.Add(columnName);
                fieldEnabledByIndex[columnIndex] = true;
            }

            // Make sure the field names do not have spaces or parentheses in them
            for (var i = 0; i < filteredList.Count; i++)
            {
                filteredList[i] = SQLiteBulkInsert_CleanFields(filteredList[i]);
            }

            return filteredList;
        }

        private List<string> SQLiteBulkInsert_TokenizeLine(string line)
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

        private string SQLiteBulkInsert_BuildSQL_Line(string table, IReadOnlyList<string> dbColumns)
        {
            var sbSql = new System.Text.StringBuilder();

            // Build base
            sbSql.AppendFormat("INSERT INTO {0} (", table);

            // Build commands
            for (var i = 0; i < dbColumns.Count; i++)
            {
                sbSql.AppendFormat("`{0}`", dbColumns[i]);
                if (i < dbColumns.Count - 1)
                    sbSql.Append(",");
            }

            // Add end of commands
            sbSql.Append(") VALUES (");

            // Build value list
            for (var i = 0; i < dbColumns.Count; i++)
            {
                sbSql.AppendFormat("@{0}", i);
                if (i < dbColumns.Count - 1)
                    sbSql.Append(",");
            }

            // Add end of values
            sbSql.Append(");");

            return sbSql.ToString();
        }

        /// <summary>
        /// Replace invalid characters in field names
        /// </summary>
        /// <param name="field"></param>
        private string SQLiteBulkInsert_CleanFields(string field)
        {
            var cleanedField = field;

            // Replace blanks with _
            if (cleanedField.Contains(" "))
            {
                cleanedField = cleanedField.Replace(" ", "_");
            }

            // Replace all instances of _(
            while (cleanedField.Contains("_("))
            {
                cleanedField = cleanedField.Remove(cleanedField.IndexOf("_(", StringComparison.Ordinal));
            }

            // Replace all instances of (
            while (cleanedField.Contains("("))
            {
                cleanedField = cleanedField.Remove(cleanedField.IndexOf("(", StringComparison.Ordinal));
            }

            return cleanedField;
        }

        /// <summary>
        /// Get the database function for obtaining date/time as a string (within a SQL query)
        /// </summary>
        /// <returns>Function name, including format codes to convert date and time to a string</returns>
        public string GetDateTime()
        {
            // ReSharper disable once StringLiteralTypo
            return "strftime('%Y-%m-%d %H:%M:%S','now', 'localtime')";
        }

        private void OnErrorEvent(string errorMessage)
        {
            ErrorEvent?.Invoke(errorMessage);
        }
    }
}
