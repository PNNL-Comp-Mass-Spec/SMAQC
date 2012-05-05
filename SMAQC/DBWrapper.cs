using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
#if (!MySqlMissing)
using MySql.Data.MySqlClient;
#endif
using System.Collections;
using System.Data;
using System.IO;
using System.Reflection;

namespace SMAQC
{
    class DBWrapper
    {
		//DELEGATE FUNCTION FOR ERROR EVENTS
		public delegate void DBErrorEventHandler(string errorMessage);
		public event DBWrapper.DBErrorEventHandler ErrorEvent;

        //DECLARE VARIABLES
        DBInterface dbConn = null;
        public String[] db_tables = { "temp_scanstats", "temp_scanstatsex", "temp_sicstats", "temp_xt", "temp_xt_resulttoseqmap", "temp_xt_seqtoproteinmap" };
		private bool mShowQueryText;
	
#if (!MySqlMissing)
		//MySqlDataReader reader;                                 //READ POINTER
        IDataReader reader;
#endif

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

        //CONSTRUCTOR
        public DBWrapper(string host, string user, string pass, string db, string dbtype, string dbFolderPath)
        {
            //GET PATH TO DB [NEEDED FOR SQLite SO WE SAVE IN CORRECT LOCATION]
			string dbPath = System.IO.Path.Combine(dbFolderPath, "SMAQC.s3db");

            if (dbtype.Equals("MySQL", StringComparison.OrdinalIgnoreCase))
            {
                //CREATE DB CONN
                try
				{
#if (!MySqlMissing)
                    dbConn = new DBMySQL(host, user, pass, db);
#else
					Console.WriteLine("Error: MySQL support is not enabled in this version of SMAQC.");
					Environment.Exit(1);
#endif
                }
                catch
                {
                    Console.WriteLine("Error: You do not have MySQL NET Connector Installed on your Machine!");
                    Environment.Exit(1);
                }
            }
            else if(dbtype.Equals("SQLite", StringComparison.OrdinalIgnoreCase))
            {
                //CREATE DB CONN
                //try
                //{
                    dbConn = new DBSQLite(dbPath);
                //}
                //catch
                //{
                //    Console.WriteLine("Error: You do not have SQLite Library Installed on your Machine!");
                //    Environment.Exit(1);
                //}
            }
            else
            {
                Console.WriteLine("Invalid DBType! config.xml only supports 'MySQL' or 'SQLite'!");
                Environment.Exit(1);
            }

			// Attach the event handler
			dbConn.ErrorEvent +=new DBErrorEventHandler(dbConn_ErrorEvent);

        }

        //DESTRUCTOR
        ~DBWrapper()
        {
        }

        //CLEAR DB TABLES
        public void clearTempTables(int r_id)
        {
            dbConn.clearTempTables(r_id, db_tables);
        }

        //SET QUERY
        public void setQuery(string myquery)
        {
            try
            {
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

        //BULK MYSQL INSERT
        public void BulkInsert(String insert_into_table, String file_to_read_from)
        {
            dbConn.BulkInsert(insert_into_table, file_to_read_from);
        }

        //FOR QUERIES THAT RETURN ROWS
        //public Object QueryReader()
        //{
            //return dbConn.QueryReader();
        //}

        //FOR QUERIES SUCH AS INSERT/DELETE/UPDATE
        public Boolean QueryNonQuery()
        {
            //DECLARE VARIABLE
            Boolean status = false;

            //UPDATE STATUS + RUN QUERY
            try
            {
                status = dbConn.QueryNonQuery();
            }
            catch (System.NullReferenceException ex)
            {
				Console.WriteLine("Error in QueryNonQuery: " + ex.Message);
            }

            //RETURN TRUE/FALSE
            return status;
        }

        //INIT MYSQL READER [WHENEVER WE WANT TO READ A ROW]
        public void initReader()
        {
            dbConn.initReader();
        }

        //READ SINGLE DB ROW [DIFFERENT FROM readLines() as here we close reader afterwords]
        //[RETURNS FALSE IF NO FURTHER ROWS TO READ]
        public Boolean readSingleLine(String[] fields, ref Hashtable hash)
        {
            //DECLARE VARIABLE
            Boolean status = false;

            //BLANK HASH
            hash.Clear();

            //READ + UPDATE STATUS
            status = dbConn.readSingleLine(fields, ref hash);

            //RETURN TRUE SINCE READ == OK
            return status;
        }

        //READ DB ROW(s) [RETURNS FALSE IF NO FURTHER ROWS TO READ]
        public Boolean readLines(String[] fields, ref Hashtable hash)
        {
            //DECLARE VARIABLE
            Boolean status = false;

            //BLANK HASH
            hash.Clear();

            //READ + UPDATE STATUS
            status = dbConn.readLines(fields, ref hash);
 
            //RETURN TRUE SINCE READ == OK
            return true;
        }

        //GET THE DB GETDATE() FOR OUR DB'S AS STRING
        public String getDateTime()
        {
            return dbConn.getDateTime();
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
