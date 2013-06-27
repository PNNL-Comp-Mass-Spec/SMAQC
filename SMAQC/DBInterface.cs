using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SQLite;
using System.Collections;

namespace SMAQC
{
    interface DBInterface
    {
		void ClearTempTables(string[] db_tables);
		void ClearTempTables(string[] db_tables, int random_id);

        void setQuery(string temp);
        SQLiteDataReader QueryReader();
        Boolean QueryNonQuery();
        void QueryScalar();
        void Open();
        void BulkInsert(string insert_into_table, string file_to_read_from);
        void initReader();
		Boolean readSingleLine(string[] fields, ref Dictionary<string, string> dctData);
		Boolean readLines(string[] fields, ref Dictionary<string, string> hash);
        string getDateTime();

		bool InitPHRPInsertCommand(out System.Data.Common.DbTransaction dbTrans);
		void ExecutePHRPInsert(Dictionary<string, string> dctData, int line_num);

		event DBWrapper.DBErrorEventHandler ErrorEvent;

    }
}
