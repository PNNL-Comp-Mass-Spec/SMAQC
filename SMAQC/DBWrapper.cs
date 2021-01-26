using System;
using System.Collections.Generic;
using System.IO;

namespace SMAQC
{
    internal class DBWrapper
    {
        // Ignore Spelling: xt

        // Delegate function for error events
        public delegate void DBErrorEventHandler(string errorMessage);
        public event DBErrorEventHandler ErrorEvent;

        private readonly IDBInterface dbConn;
        private readonly string[] db_tables = {
            "temp_ScanStats", "temp_ScanStatsEx", "temp_SICStats",
            "temp_xt", "temp_xt_ResultToSeqMap", "temp_xt_SeqToProteinMap",
            "temp_PSMs", "temp_ReporterIons" };

        private readonly bool mShowQueryText;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="databaseDirectoryPath">Path to the directory where SMAQC.s3db should be created/updated</param>
        /// <param name="showQueryText">When true, show the text of every query at the console</param>
        public DBWrapper(string databaseDirectoryPath, bool showQueryText)
        {
            // Get path to db [needed for SQLite so we save in correct location]
            var databaseFilePath = Path.Combine(databaseDirectoryPath, "SMAQC.s3db");

            // Create db conn
            dbConn = new DBSQLite(databaseFilePath);

            // Verify that the required columns are present

            // Attach the event handler
            dbConn.ErrorEvent += DatabaseConnection_ErrorEvent;

            mShowQueryText = showQueryText;
        }

        /// <summary>
        /// Clear db temp tables for all data
        /// </summary>
        public void ClearTempTables()
        {
            dbConn.ClearTempTables(db_tables);
        }

        /// <summary>
        /// Clear db temp tables for all data
        /// </summary>
        /// <param name="random_id"></param>
        public void ClearTempTables(int random_id)
        {
            dbConn.ClearTempTables(db_tables, random_id);
        }

        /// <summary>
        /// Define the query to run
        /// </summary>
        /// <param name="query"></param>
        public void SetQuery(string query)
        {
            try
            {
                dbConn.SetQuery(query);

                if (mShowQueryText)
                {
                    Console.WriteLine();
                    PRISM.ConsoleMsgUtils.ShowDebug(query);
                }
            }
            catch (NullReferenceException ex)
            {
                Console.WriteLine("Error setting the query text: " + ex.Message);
            }
        }

        /// <summary>
        /// Bulk insert a set of data from sourceFile
        /// </summary>
        /// <param name="targetTable">Target table</param>
        /// <param name="sourceFile">Source file</param>
        /// <param name="excludedFieldNameSuffixes">Field prefixes to ignore</param>
        public void BulkInsert(string targetTable, string sourceFile, List<string> excludedFieldNameSuffixes)
        {
            dbConn.BulkInsert(targetTable, sourceFile, excludedFieldNameSuffixes);
        }

        /// <summary>
        /// Run a query that does not return results (insert/delete/update)
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Call SetQuery prior to calling this method</remarks>
        public bool ExecuteNonQuery()
        {
            try
            {
                var status = dbConn.ExecuteNonQuery();
                return status;
            }
            catch (NullReferenceException ex)
            {
                Console.WriteLine("Error in ExecuteNonQuery: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Initialize the reader
        /// </summary>
        /// <remarks>Call SetQuery prior to calling this method</remarks>
        public void InitReader()
        {
            dbConn.InitReader();
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
            var status = dbConn.ReadSingleLine(fields, out dctData);

            return status;
        }

        /// <summary>
        /// Read data for one database row
        /// </summary>
        /// <param name="fields"></param>
        /// <param name="dctData"></param>
        /// <returns>True if success, false if no further rows to read</returns>
        public bool ReadNextRow(string[] fields, out Dictionary<string, string> dctData)
        {
            dbConn.ReadNextRow(fields, out dctData);

            return true;
        }

        /// <summary>
        /// Get the database function for obtaining date/time as a string (within a SQL query)
        /// </summary>
        /// <returns>Function name, including format codes to convert date and time to a string</returns>
        public string GetDateTime()
        {
            return dbConn.GetDateTime();
        }

        /// <summary>
        /// Return the columns defined for the given table
        /// </summary>
        /// <param name="tableName"></param>
        public List<string> GetTableColumns(string tableName)
        {
            return dbConn.GetTableColumns(tableName);
        }

        /// <summary>
        /// Initialize the command for inserting PHRP data
        /// </summary>
        /// <param name="dbtrans"></param>
        public bool InitPHRPInsertCommand(out System.Data.Common.DbTransaction dbTrans)
        {
            return dbConn.InitPHRPInsertCommand(out dbTrans);
        }

        /// <summary>
        /// Add new PHRP data
        /// </summary>
        /// <param name="dctdata"></param>
        /// <param name="line_num"></param>
        public void ExecutePHRPInsertCommand(Dictionary<string, string> dctData, int lineNumber)
        {
            dbConn.ExecutePHRPInsert(dctData, lineNumber);
        }

        private void DatabaseConnection_ErrorEvent(string errorMessage)
        {
            ErrorEvent?.Invoke(errorMessage);
        }
    }
}
