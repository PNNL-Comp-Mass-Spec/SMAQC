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

		public enum eDBTypeConstants {
			Unknown = 0,
			SQLite = 1,
			MySql = 2
		}

        //DECLARE VARIABLES
        DBInterface dbConn = null;
        public string[] db_tables = { "temp_scanstats", "temp_scanstatsex", "temp_sicstats", "temp_xt", "temp_xt_resulttoseqmap", "temp_xt_seqtoproteinmap", "temp_PSMs"};
		private bool mShowQueryText;

		private eDBTypeConstants m_DBType = eDBTypeConstants.Unknown;

#if (!MySqlMissing)
		//MySqlDataReader reader;                                 //READ POINTER
        IDataReader reader;
#endif

		#region "Properties"

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

		public eDBTypeConstants DBType
		{
			get { 
				return m_DBType; 
			}
		}

		#endregion

		//CONSTRUCTOR
		public DBWrapper(string host, string user, string pass, string db, eDBTypeConstants dbtype, string dbFolderPath)
        {
            //GET PATH TO DB [NEEDED FOR SQLite SO WE SAVE IN CORRECT LOCATION]
			string dbPath = System.IO.Path.Combine(dbFolderPath, "SMAQC.s3db");

            if (dbtype == eDBTypeConstants.MySql)
            {
                //CREATE DB CONN
                try
				{
#if (!MySqlMissing)
                    dbConn = new DBMySQL(host, user, pass, db);
#else
					Console.WriteLine("Error: MySQL support is not enabled in this version of SMAQC.");
					System.Threading.Thread.Sleep(2000);
					Environment.Exit(1);
#endif
                }
                catch
                {
                    Console.WriteLine("Error: You do not have MySQL NET Connector Installed on your Machine!");
					System.Threading.Thread.Sleep(2000);
                    Environment.Exit(1);
                }

				m_DBType = eDBTypeConstants.MySql;

            }
            else if(dbtype == eDBTypeConstants.SQLite)
            {
                //CREATE DB CONN
                //try
                //{
                    dbConn = new DBSQLite(dbPath);
                //}
                //catch
                //{
                //    Console.WriteLine("Error: You do not have SQLite Library Installed on your Machine!");
				//    System.Threading.Thread.Sleep(2000);
                //    Environment.Exit(1);
                //}

					m_DBType = eDBTypeConstants.SQLite;
            }
            else
            {
                Console.WriteLine("Invalid DBType! config.xml only supports 'MySQL' or 'SQLite'!");
				System.Threading.Thread.Sleep(2000);
                Environment.Exit(1);
            }

			// Attach the event handler
			dbConn.ErrorEvent +=new DBErrorEventHandler(dbConn_ErrorEvent);

        }

        //DESTRUCTOR
        ~DBWrapper()
        {
        }

		/// <summary>
		/// Clear DB Temp Tables for all data
		/// </summary>
		/// <param name="random_id"></param>
		/// <param name="db_tables"></param>
		public void ClearTempTables()
		{
			dbConn.ClearTempTables(db_tables);
		}

		/// <summary>
		/// Clear DB Temp Tables for all data
		/// </summary>
		/// <param name="random_id"></param>
		/// <param name="db_tables"></param>
		public void ClearTempTables(int random_id)
        {
			dbConn.ClearTempTables(db_tables, random_id);
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
        public void BulkInsert(string insert_into_table, string file_to_read_from)
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

        //READ SINGLE DB ROW [DIFFERENT FROM readLines() as here we close reader afterward]
        //[RETURNS FALSE IF NO FURTHER ROWS TO READ]
		public Boolean readSingleLine(string[] fields, ref Dictionary<string, string> dctData)
        {
            Boolean status = false;

			dctData.Clear();

			status = dbConn.readSingleLine(fields, ref dctData);

            return status;
        }

        //READ DB ROW(s) [RETURNS FALSE IF NO FURTHER ROWS TO READ]
		public Boolean readLines(string[] fields, ref Dictionary<string, string> dctData)
        {
            Boolean status = false;

			dctData.Clear();

			status = dbConn.readLines(fields, ref dctData);
 
            return true;
        }

        //GET THE DB GETDATE() FOR OUR DB'S AS STRING
        public string getDateTime()
        {
            return dbConn.getDateTime();
        }

		/// <summary>
		/// Initialize the command for inserting PHRP data
		/// </summary>
		/// <param name="dctFieldsForInsert"></param>
		/// <returns></returns>
		public bool InitPHRPInsertCommand(out System.Data.Common.DbTransaction dbTrans)
		{
			return dbConn.InitPHRPInsertCommand(out dbTrans);
		}

		/// <summary>
		/// Add new PHRP data
		/// </summary>
		/// <param name="dctFieldsForInsert"></param>
		/// <param name="dctData"></param>
		/// <param name="line_num"></param>
		public void ExecutePHRPInsertCommand( Dictionary<string, string> dctData, int line_num)
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
