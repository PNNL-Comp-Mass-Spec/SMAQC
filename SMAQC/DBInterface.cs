using System.Collections.Generic;
using System.Data.SQLite;

namespace SMAQC
{
    interface DBInterface
    {
        void ClearTempTables(string[] db_tables);
        void ClearTempTables(string[] db_tables, int random_id);

        void SetQuery(string temp);
        SQLiteDataReader QueryReader();
        bool ExecuteNonQuery();
        void QueryScalar();
        void Open();
        void BulkInsert(string targetTable, string sourceFile, List<string> excludedFieldNameSuffixes);
        void InitReader();
        bool ReadSingleLine(string[] fields, out Dictionary<string, string> dctData);
        bool ReadLines(string[] fields, out Dictionary<string, string> hash);
        string GetDateTime();
        
        List<string> GetTableColumns(string tableName);

        bool InitPHRPInsertCommand(out System.Data.Common.DbTransaction dbTrans);
        void ExecutePHRPInsert(Dictionary<string, string> dctData, int line_num);

        event DBWrapper.DBErrorEventHandler ErrorEvent;

    }
}
