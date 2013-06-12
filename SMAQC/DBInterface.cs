using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
#if (!MySqlMissing == true)
using MySql.Data.MySqlClient;
#endif
using System.Data.SQLite;
using System.Collections;

namespace SMAQC
{
    interface DBInterface
    {
        void clearTempTables(int random_id, string[] db_tables);
        void setQuery(string temp);
        //MySqlDataReader QueryReader();
        //SQLiteDataReader QueryReader();
        Object QueryReader();
        Boolean QueryNonQuery();
        void QueryScalar();
        void Open();
        void BulkInsert(string insert_into_table, string file_to_read_from);
        void initReader();
        Boolean readSingleLine(string[] fields, ref Hashtable hash);
        Boolean readLines(string[] fields, ref Hashtable hash);
        string getDateTime();

		bool InitPHRPInsertCommand(out System.Data.Common.DbTransaction dbTrans);
		void ExecutePHRPInsert(Dictionary<string, string> dctData, int line_num);

		event DBWrapper.DBErrorEventHandler ErrorEvent;

    }
}
