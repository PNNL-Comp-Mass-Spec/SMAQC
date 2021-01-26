using System.Collections.Generic;
using System.Data.SQLite;

namespace SMAQC
{
    interface DBInterface
    {
        /// <Summary>
        /// Clear database temp tables for all data
        /// </Summary>
        /// <Param name="tableNames"></param>
        void ClearTempTables(string[] tableNames);

        /// <Summary>
        /// Clear database temp tables for given randomId value
        /// </Summary>
        /// <Param name="tableNames">Table names</param>
        /// <Param name="randomId">Random ID for this analysis</param>
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

        void Open();

        /// <summary>
        /// Bulk insert a set of data from sourceFile
        /// </summary>
        /// <param name="targetTable">Target table</param>
        /// <param name="sourceFile">Source file</param>
        /// <param name="excludedFieldNameSuffixes">Field prefixes to ignore</param>
        void BulkInsert(string targetTable, string sourceFile, List<string> excludedFieldNameSuffixes);

        /// <summary>
        /// Run the query defined by SetQuery, thereby initializing a reader for retrieving the results
        /// </summary>
        /// <remarks>Call SetQuery prior to calling this method</remarks>
        void InitReader();

        /// <summary>
        /// Read a single database row
        /// </summary>
        /// <param name="fields"></param>
        /// <param name="dctData"></param>
        /// <returns>True if success, false if no further rows to read</returns>
        /// <remarks>This method differs from ReadNextRow since here we close the reader after reading a single row of data</remarks>
        bool ReadSingleLine(string[] fields, out Dictionary<string, string> dctData);

        /// <summary>
        /// Read data for one database row
        /// </summary>
        /// <param name="fields"></param>
        /// <param name="dctData"></param>
        /// <returns>True if success, false if no further rows to read</returns>
        bool ReadNextRow(string[] fields, out Dictionary<string, string> dctData);

        /// <summary>
        /// Get the database function for obtaining date/time as a string (within a SQL query)
        /// </summary>
        /// <returns>Function name, including format codes to convert date and time to a string</returns>
        string GetDateTime();

        /// <summary>
        /// Return the columns defined for the given table
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        List<string> GetTableColumns(string tableName);

        /// <Summary>
        /// Initialize the command for inserting PHRP data
        /// </Summary>
        /// <Param name="dbtrans"></param>
        /// <Returns></returns>
        bool InitPHRPInsertCommand(out System.Data.Common.DbTransaction dbTrans);

        /// <Summary>
        /// Add new PHRP data
        /// </Summary>
        /// <Param name="dctdata"></param>
        /// <Param name="line_num"></param>
        void ExecutePHRPInsert(Dictionary<string, string> dctData, int lineNumber);

        /// <summary>
        /// Error event
        /// </summary>
        event DBWrapper.DBErrorEventHandler ErrorEvent;

    }
}
