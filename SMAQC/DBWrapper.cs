using System;
using System.Collections.Generic;
using System.IO;

namespace SMAQC
{
    class DBWrapper
    {
        // Delegate function for error events
        public delegate void DBErrorEventHandler(string errorMessage);
        public event DBErrorEventHandler ErrorEvent;

        readonly DBInterface dbConn;
        private readonly string[] db_tables = {
            "temp_scanstats", "temp_scanstatsex", "temp_sicstats",
            "temp_xt", "temp_xt_resulttoseqmap", "temp_xt_seqtoproteinmap",
            "temp_PSMs", "temp_reporterions" };

        private readonly bool mShowQueryText;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dbFolderPath"></param>
        /// <param name="showQueryText"></param>
        public DBWrapper(string dbFolderPath, bool showQueryText)
        {
            // Get path to db [needed for sqlite so we save in correct location]
            var dbPath = Path.Combine(dbFolderPath, "SMAQC.s3db");

            // Create db conn
            dbConn = new DBSQLite(dbPath);

            // Verify that the required columns are present

            // Attach the event handler
            dbConn.ErrorEvent += dbConn_ErrorEvent;

            mShowQueryText = showQueryText;
        }

        /// <Summary>
        /// Clear db temp tables for all data
        /// </Summary>
        public void ClearTempTables()
        {
            dbConn.ClearTempTables(db_tables);
        }

        /// <Summary>
        /// Clear db temp tables for all data
        /// </Summary>
        /// <Param name="random_id"></param>
        public void ClearTempTables(int random_id)
        {
            dbConn.ClearTempTables(db_tables, random_id);
        }

        // Set query
        public void SetQuery(string myquery)
        {
            try
            {

                dbConn.SetQuery(myquery);

                if (mShowQueryText)
                {
                    Console.WriteLine();
                    Console.WriteLine(myquery);
                }
            }
            catch (NullReferenceException ex)
            {
                Console.WriteLine("Error setting the query text: " + ex.Message);
            }
        }

        // Bulk insert
        public void BulkInsert(string insert_into_table, string file_to_read_from)
        {
            dbConn.BulkInsert(insert_into_table, file_to_read_from);
        }

        // For queries such as insert/delete/update
        public bool ExecuteNonQuery()
        {

            var status = false;

            // Update status + run query
            try
            {
                status = dbConn.ExecuteNonQuery();
            }
            catch (NullReferenceException ex)
            {
                Console.WriteLine("Error in ExecuteNonQuery: " + ex.Message);
            }

            // Return true/false
            return status;
        }

        // Init reader [whenever we want to read a row]
        public void initReader()
        {
            dbConn.InitReader();
        }

        // Read single db row [different from readlines() as here we close reader afterward]
        // [Returns false if no further rows to read]
        public bool ReadSingleLine(string[] fields, ref Dictionary<string, string> dctData)
        {
            dctData.Clear();

            var status = dbConn.ReadSingleLine(fields, ref dctData);

            return status;
        }

        // Read db row(s) [returns false if no further rows to read]
        public bool ReadLines(string[] fields, ref Dictionary<string, string> dctData)
        {
            dctData.Clear();

            dbConn.ReadLines(fields, ref dctData);

            return true;
        }

        // Get the db getdate() for our db's as string
        public string GetDateTime()
        {
            return dbConn.GetDateTime();
        }

        /// <summary>
        /// Return the columns defined for the given table
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public List<string> GetTableColumns(string tableName)
        {
            return dbConn.GetTableColumns(tableName);
        }

        /// <Summary>
        /// Initialize the command for inserting phrp data
        /// </Summary>
        /// <Param name="dbtrans"></param>
        /// <Returns></returns>
        public bool InitPHRPInsertCommand(out System.Data.Common.DbTransaction dbTrans)
        {
            return dbConn.InitPHRPInsertCommand(out dbTrans);
        }

        /// <Summary>
        /// Add new phrp data
        /// </Summary>
        /// <Param name="dctdata"></param>
        /// <Param name="line_num"></param>
        public void ExecutePHRPInsertCommand(Dictionary<string, string> dctData, int line_num)
        {
            dbConn.ExecutePHRPInsert(dctData, line_num);
        }

        void dbConn_ErrorEvent(string errorMessage)
        {
            ErrorEvent?.Invoke(errorMessage);
        }

    }
}
