using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SQLite;
using System.Collections;
using System.IO;
using System.Data.Common;

namespace SMAQC
{
    class DBSQLite : DBInterface
    {
        //DECLARE VARIABLES
        private SQLiteConnection conn;                                                  //SQLITE CONNECTION
        private String query;                                                           //QUERY TO RUN
        private SQLiteDataReader reader;                                                //SQLITE READER
        private DBSQLiteTools SQLiteTools = new DBSQLiteTools();                        //CREATE DBSQLITE TOOLS OBJECT

        //CONSTRUCTOR
        public DBSQLite(String datasource)
        {

            //IF OUR SQLiteDB DOES NOT EXIST!
            if (!File.Exists(datasource))
            {
                //CREATE IT + THE TABLES!
                SQLiteTools.create_tables(datasource);
            }

            conn = new SQLiteConnection("Data Source=" + datasource);
            
            //OPEN DB CONN
            this.Open();
        }

        //DESTRUCTOR
        ~DBSQLite()
        {
            try
            {
                conn.Close();
            }
            catch (System.NullReferenceException ex)
            {
				Console.WriteLine("Error closing the SQLite DB: " + ex.Message);
            }
        }

        //CLEAR DB TABLES
        public void clearTempTables(int r_id, String[] db_tables)
        {
            //LOOP THROUGH EACH TEMP TABLE
            for (int i = 0; i < db_tables.Length; i++)
            {
                //CREATE QUERY
                String temp_string = "DELETE FROM `" + db_tables[i] + "` WHERE random_id='" + r_id + "';";

                //SET QUERY
                this.setQuery(temp_string);

                //CALL QUERY FUNCTION
                this.QueryNonQuery();
            }
        }

        public void setQuery(string myquery)
        {
            //SET QUERY TO PARAM
            query = myquery;
        }

        //FOR QUERIES THAT RETURN ROWS
        //public SQLiteDataReader QueryReader()
        public Object QueryReader()
        {
            SQLiteDataReader reader = null;

            //ADD TRY {} BLOCKS HERE
            SQLiteCommand cmd = new SQLiteCommand(conn);
            cmd.CommandText = query;
            reader = cmd.ExecuteReader();

            return reader;
        }

        //FOR QUERIES SUCH AS INSERT/DELETE/UPDATE
        public Boolean QueryNonQuery()
        {
            SQLiteCommand cmd = null;

            //ADD TRY {} BLOCKS HERE
            cmd = new SQLiteCommand(conn);
            cmd.CommandText = query;
            cmd.ExecuteNonQuery();

            if (cmd == null)
                return false;
            else
                return true;
        }

        //FOR QUERIES THAT RETURN A SINGLE VALUE
        public void QueryScalar()
        {
            SQLiteCommand cmd = null;

            //ADD TRY {} BLOCKS HERE
            cmd = new SQLiteCommand(conn);
            cmd.CommandText = query;
            cmd.ExecuteScalar();

        }

        //THIS FUNCTION OPENS A CONNECTION TO THE DB
        public void Open()
        {
            //OPEN SQLite CONN
            conn.Open();
        }

        public void BulkInsert(String insert_into_table, String file_to_read_from)
        {
            //FETCH FIELDS
            String[] fields = SQLiteBulkInsert_Fields(file_to_read_from);

            //BUILD SQL LINE
            String sql = SQLiteBulkInsert_BuildSQL_Line(insert_into_table, fields);

            using (DbTransaction dbTrans = conn.BeginTransaction())
            {
                using (SQLiteCommand mycommand = conn.CreateCommand())
                {
                    mycommand.CommandText = sql;

                    StreamReader file = new StreamReader(file_to_read_from);
                    String line;
                    int line_num = 0;
                    while ((line = file.ReadLine()) != null)
                    {
                        //CHECK IF FIELD LISTING LINE #
                        if (line_num == 0)
                        {
                            line_num++;
                            continue;//SKIP THIS LINE AS WE DO NOT WANT THE FIELD LISTING
                        }


                        //Console.WriteLine("LINE [{0}]", line);
                        //FETCH VALUES
                        String[] values = SQLiteBulkInsert_TokenizeLine(line);

                        //LOOP THROUGH FIELD LISTING + SET PARAMETERS
                        for (int i = 0; i < fields.Length; i++)
                        {
                            //THIS PART NOT NEEDED DUE TO SWITCHING TO 0 ... n INSTEAD OF FIELD LIST
                            //fields[i] = SQLiteBulkInsert_CleanLine(fields[i]);

                            mycommand.Parameters.AddWithValue("@" + i, values[i]);
                        }

                        //NOW THAT ALL FIELDS + VALUES ARE IN OUR SYSTEM
                        mycommand.ExecuteNonQuery();
                    }
                    //CLOSE FILE
                    file.Close();
                }
                dbTrans.Commit();
            }
        }

        //INIT MYSQL READER [WHENEVER WE WANT TO READ A ROW]
        public void initReader()
        {
            //CALL QUERY READER
            reader = (SQLiteDataReader)QueryReader();
        }

        //READ SINGLE DB ROW [DIFFERENT FROM readLines() as here we close reader afterwords]
        //[RETURNS FALSE IF NO FURTHER ROWS TO READ]
        public Boolean readSingleLine(String[] fields, ref Hashtable hash)
        {
            //DECLARE VARIABLE
            Boolean status;

            //READ LINE
            status = reader.Read();

            //IF RETURNED FALSE ... NO ROWS TO RETURN
            if (!status)
            {
                //CLOSE READER
                reader.Close();

                //RETURN FALSE AS NO MORE TO READ
                return false;
            }

            //DECLARE VARIABLES
            for (int i = 0; i < fields.Length; i++)
            {
                try
                {
                    int value = reader.GetOrdinal(fields[i]);
                    hash.Add(fields[i], reader.GetValue(value).ToString() );
                }
                catch (System.Data.SqlTypes.SqlNullValueException)
                {

                }
            }

            //CLOSE READER
            reader.Close();

            //RETURN TRUE SINCE READ == OK
            return true;
        }

        //READ DB ROW(s) [RETURNS FALSE IF NO FURTHER ROWS TO READ]
        public Boolean readLines(String[] fields, ref Hashtable hash)
        {
            //DECLARE VARIABLE
            Boolean status;

            //READ LINE
            status = reader.Read();

            //IF RETURNED FALSE ... NO ROWS TO RETURN
            if (!status)
            {
                //CLOSE READER
                reader.Close();

                //RETURN FALSE AS NO MORE TO READ
                return false;
            }

            //DECLARE VARIABLES
            for (int i = 0; i < fields.Length; i++)
            {
                //READ + STORE FIELD [Value, Result]
                try
                {
                    int value = reader.GetOrdinal(fields[i]);
                    hash.Add(fields[i], reader.GetValue(value).ToString());
                }
                catch (System.Data.SqlTypes.SqlNullValueException)
                {

                }
            }

            //RETURN TRUE SINCE READ == OK
            return true;
        }

        String[] SQLiteBulkInsert_Fields(String filename)
        {
            StreamReader file = new StreamReader(filename);
            String line = file.ReadLine();
            file.Close();

            //SPLIT GIVEN DATA FILES BY TAB
            char[] delimiters = new char[] { ',' };

            //DO SPLIT OPERATION
            string[] parts = line.Split(delimiters, StringSplitOptions.None);

            for (int i = 0; i < parts.Length; i++)
            {
                parts[i] = SQLiteBulkInsert_CleanFields(parts[i]);
            }

            return parts;
        }

        String[] SQLiteBulkInsert_TokenizeLine(String line)
        {
            //SPLIT GIVEN DATA FILES BY TAB
            char[] delimiters = new char[] { ',' };

            //IF LINE CONTAINS ",," WHICH MEANS == FIELD IS ALLOWED NULL [SCANSTATSEX FIX]
            while (line.Contains(",,"))
            {
                line = line.Replace(",,", ", ,");
            }

            //DO SPLIT OPERATION
            string[] parts = line.Split(delimiters, StringSplitOptions.None);

            return parts;
        }

        String SQLiteBulkInsert_BuildSQL_Line(String table, String[] parts)
        {
            String sql = "";

            //BUILD BASE
            sql += "INSERT INTO " + table + " (";

            //BUILD COMMANDS
            for (int i = 0; i < parts.Length; i++)
            {
                sql += "`" + parts[i] + "`,";
            }
            //THERE IS NOW AN EXTRA , FIND INDEX OF IT + REMOVE
            sql = sql.Remove(sql.LastIndexOf(","));

            //ADD END OF COMMANDS
            sql += ") VALUES (";

            //BUILD VALUE LIST
            for (int i = 0; i < parts.Length; i++)
            {
                sql += "@" + i + ",";
            }
            //THERE IS NOW AN EXTRA , FIND INDEX OF IT + REMOVE
            sql = sql.Remove(sql.LastIndexOf(","));

            //ADD END OF VALUES
            sql += ");";

            return sql;
        }

        String SQLiteBulkInsert_CleanFields(String field)
        {
            String field_line = field;

            //IF == SPACE
            if (field_line.Contains(" "))
            {
                //REPLACE BLANKS WITH _
                field_line = field_line.Replace(" ", "_");
            }

            //REPLACE ALL INSTANCES OF _(
            while (field_line.IndexOf("_(") > 0)
            {
                field_line = field_line.Remove(field_line.IndexOf("_("));
            }

            //REPLACE ALL INSTANCES OF (
            while (field_line.IndexOf("(") > 0)
            {
                field_line = field_line.Remove(field_line.IndexOf("("));
            }

            return field_line;
        }

        public String getDateTime()
        {
            return "strftime('%Y-%m-%d %H:%M:%S','now', 'localtime')";
        }

    }
}
