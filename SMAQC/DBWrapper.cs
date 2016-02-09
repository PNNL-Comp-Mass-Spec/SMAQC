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

        // Declare variables
        readonly DBInterface dbConn;
        public string[] db_tables = { "temp_scanstats", "temp_scanstatsex", "temp_sicstats", "temp_xt", "temp_xt_resulttoseqmap", "temp_xt_seqtoproteinmap", "temp_PSMs" };
        private bool mShowQueryText;

        #region "Properties"

        private string mCurrentQuery;

        // / <Summary>
        // / The most recent query sql
        // / </Summary>
        public string CurrentQuery
        {
            get
            {
                return mCurrentQuery;
            }
        }

        public bool ShowQueryText
        {
            get
            {
                return mShowQueryText;
            }
            set
            {
                mShowQueryText = value;
            }
        }

        #endregion

        // Constructor
        public DBWrapper(string dbFolderPath)
        {
            // Get path to db [needed for sqlite so we save in correct location]
            var dbPath = Path.Combine(dbFolderPath, "SMAQC.s3db");

            // Create db conn
            dbConn = new DBSQLite(dbPath);

            // Verify that the required columns are present

            // Attach the event handler
            dbConn.ErrorEvent += new DBErrorEventHandler(dbConn_ErrorEvent);

            mCurrentQuery = string.Empty;
        }

        // / <Summary>
        // / Clear db temp tables for all data
        // / </Summary>
        public void ClearTempTables()
        {
            dbConn.ClearTempTables(db_tables);
        }

        // / <Summary>
        // / Clear db temp tables for all data
        // / </Summary>
        // / <Param name="random_id"></param>
        public void ClearTempTables(int random_id)
        {
            dbConn.ClearTempTables(db_tables, random_id);
        }

        // Set query
        public void setQuery(string myquery)
        {
            try
            {
                mCurrentQuery = myquery;

                dbConn.setQuery(myquery);

                if (mShowQueryText)
                {
                    Console.WriteLine();
                    Console.WriteLine(myquery);
                }
            }
            catch (System.NullReferenceException ex)
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
        public bool QueryNonQuery()
        {

            var status = false;

            // Update status + run query
            try
            {
                status = dbConn.QueryNonQuery();
            }
            catch (System.NullReferenceException ex)
            {
                Console.WriteLine("Error in QueryNonQuery: " + ex.Message);
            }

            // Return true/false
            return status;
        }

        // Init reader [whenever we want to read a row]
        public void initReader()
        {
            dbConn.initReader();
        }

        // Read single db row [different from readlines() as here we close reader afterward]
        // [Returns false if no further rows to read]
        public bool readSingleLine(string[] fields, ref Dictionary<string, string> dctData)
        {
            var status = false;

            dctData.Clear();

            status = dbConn.readSingleLine(fields, ref dctData);

            return status;
        }

        // Read db row(s) [returns false if no further rows to read]
        public bool readLines(string[] fields, ref Dictionary<string, string> dctData)
        {
            var status = false;

            dctData.Clear();

            status = dbConn.readLines(fields, ref dctData);

            return true;
        }

        // Get the db getdate() for our db's as string
        public string getDateTime()
        {
            return dbConn.getDateTime();
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

        // / <Summary>
        // / Initialize the command for inserting phrp data
        // / </Summary>
        // / <Param name="dbtrans"></param>
        // / <Returns></returns>
        public bool InitPHRPInsertCommand(out System.Data.Common.DbTransaction dbTrans)
        {
            return dbConn.InitPHRPInsertCommand(out dbTrans);
        }

        // / <Summary>
        // / Add new phrp data
        // / </Summary>
        // / <Param name="dctdata"></param>
        // / <Param name="line_num"></param>
        public void ExecutePHRPInsertCommand(Dictionary<string, string> dctData, int line_num)
        {
            dbConn.ExecutePHRPInsert(dctData, line_num);
        }

        void dbConn_ErrorEvent(string errorMessage)
        {
            if (ErrorEvent != null)
            {
                ErrorEvent(errorMessage);
            }
        }

    }
}
