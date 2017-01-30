using System.Collections.Generic;
using System.Data.SQLite;

namespace SMAQC
{
    interface DBInterface
    {
        void ClearTempTables(string[] db_tables);
        void ClearTempTables(string[] db_tables, int random_id);

        void setQuery(string temp);
        SQLiteDataReader QueryReader();
        bool QueryNonQuery();
        void QueryScalar();
        void Open();
        void BulkInsert(string insert_into_table, string file_to_read_from);
        void initReader();
        bool readSingleLine(string[] fields, ref Dictionary<string, string> dctData);
        bool readLines(string[] fields, ref Dictionary<string, string> hash);
        string getDateTime();
        
        List<string> GetTableColumns(string tableName);

        bool InitPHRPInsertCommand(out System.Data.Common.DbTransaction dbTrans);
        void ExecutePHRPInsert(Dictionary<string, string> dctData, int line_num);

        event DBWrapper.DBErrorEventHandler ErrorEvent;

    }
}
