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
        void clearTempTables(int r_id, String[] db_tables);
        void setQuery(string temp);
        //MySqlDataReader QueryReader();
        //SQLiteDataReader QueryReader();
        Object QueryReader();
        Boolean QueryNonQuery();
        void QueryScalar();
        void Open();
        void BulkInsert(String insert_into_table, String file_to_read_from);
        void initReader();
        Boolean readSingleLine(String[] fields, ref Hashtable hash);
        Boolean readLines(String[] fields, ref Hashtable hash);
        String getDateTime();
    }
}
