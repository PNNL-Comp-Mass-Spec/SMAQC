using System.Collections.Generic;

namespace SMAQC
{
    internal interface IDBInterface
    {
        /// <summary>
        /// Clear database temp tables for all data
        /// </summary>
        /// <param name="tableNames"></param>
        void ClearTempTables(string[] tableNames);

        /// <summary>
        /// Clear database temp tables for given randomId value
        /// </summary>
        /// <param name="tableNames">Table names</param>
        /// <param name="randomId">Random ID for this analysis</param>
        void ClearTempTables(string[] tableNames, int randomId);

        /// <summary>
        /// Define the query to run
        /// </summary>
        /// <param name="query"></param>
        void SetQuery(string query);

        /// <summary>
        /// Run a query that does not return results (insert/delete/update)
        /// </summary>
        /// <remarks>Call SetQuery prior to calling this method</remarks>
        bool ExecuteNonQuery();

        /// <summary>
        /// Bulk insert a set of data from sourceFile
        /// </summary>
        /// <param name="targetTable">Target table</param>
        /// <param name="sourceFile">Source file</param>
        /// <param name="excludedColumnNameSuffixes">Column suffixes to ignore</param>
        void BulkInsert(string targetTable, string sourceFile, List<string> excludedColumnNameSuffixes);

        /// <summary>
        /// Run the query defined by SetQuery, thereby initializing a reader for retrieving the results
        /// </summary>
        /// <remarks>Call SetQuery prior to calling this method</remarks>
        void InitReader();

        /// <summary>
        /// Read a single database row
        /// </summary>
        /// <remarks>This method differs from ReadNextRow since here we close the reader after reading a single row of data</remarks>
        /// <param name="columnNames"></param>
        /// <param name="dctData"></param>
        /// <returns>True if success, false if no further rows to read</returns>
        bool ReadSingleLine(string[] columnNames, out Dictionary<string, string> dctData);

        /// <summary>
        /// Read data for one database row
        /// </summary>
        /// <param name="columnNames"></param>
        /// <param name="dctData"></param>
        /// <returns>True if success, false if no further rows to read</returns>
        bool ReadNextRow(string[] columnNames, out Dictionary<string, string> dctData);

        /// <summary>
        /// Get the database function for obtaining date/time as a string (within a SQL query)
        /// </summary>
        /// <returns>Function name, including format codes to convert date and time to a string</returns>
        string GetDateTime();

        /// <summary>
        /// Return the columns defined for the given table
        /// </summary>
        /// <param name="tableName"></param>
        List<string> GetTableColumns(string tableName);

        /// <summary>
        /// Initialize the command for inserting PHRP data
        /// </summary>
        /// <param name="dbTransaction"></param>
        bool InitPHRPInsertCommand(out System.Data.Common.DbTransaction dbTransaction);

        /// <summary>
        /// Add new PHRP data
        /// </summary>
        /// <param name="dctData"></param>
        /// <param name="lineNumber"></param>
        void ExecutePHRPInsert(Dictionary<string, string> dctData, int lineNumber);

        /// <summary>
        /// Error event
        /// </summary>
        event DBWrapper.DBErrorEventHandler ErrorEvent;
    }
}
