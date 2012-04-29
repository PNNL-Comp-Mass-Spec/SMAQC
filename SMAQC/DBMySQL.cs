using System;
using MySql.Data.MySqlClient;
using System.Data;
using System.Data.SqlClient;
using System.Collections;
using System.IO;
using System.Data.Common;

namespace SMAQC
{
    class DBMySQL : DBInterface
    {
        public Boolean db_open = false;             //IS DB OPEN?
        public string dbinfo;                       //DB INFO
        public MySqlConnection conn = null;                //DB CONN VARIABLE
        MySqlDataReader reader;
        string query;                               //QUERY TO BE EXECUTED

        //CONSTRUCTOR
        public DBMySQL(string dbhost, string dbuser, string dbpass, string dbname)
        {
            //SET DB INFO
            dbinfo = "server=" + dbhost + ";database=" + dbname + ";uid=" + dbuser + ";password=" + dbpass;

            //OPEN DB CONN
            this.Open();

            //Console.WriteLine("MySQLDB() CONSTRUCTOR ({0})", dbinfo);
        }

        //DESTRUCTOR
        ~DBMySQL()
        {
            //Console.WriteLine("MySQLDB() DE-CONSTRUCTOR");
            try
            {
                conn.Close();
            }
            catch (System.NullReferenceException ex)
            {
            }
        }

        //CLEAR DB TABLES
        public void clearTempTables(int r_id, String[] db_tables)
        {
            //LOOP THROUGH EACH TEMP TABLE
            for (int i = 0; i < db_tables.Length; i++)
            {
                //CREATE QUERY
                String temp_string = "TRUNCATE `" + db_tables[i] + "`;";

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
        //public MySqlDataReader QueryReader()
        public Object QueryReader()
        {
            MySqlDataReader reader = null;
            try
            {
                MySqlCommand cmd = new MySqlCommand(query, conn);
                reader = cmd.ExecuteReader();
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                switch (ex.Number)
                {
                    case 1054:
                        Console.WriteLine("UNKNOWN COLUMN" + "[" + query + "]");
                        break;

                    case 1146:
                        Console.WriteLine("TABLE DOES NOT EXIST" + "[" + query + "]");
                        break;

                    default:
                        Console.WriteLine("QUERY FAILED! " + "[" + query + "]" + ex.Number);
                        break;
                }
            }
            return reader;
        }

        //FOR QUERIES SUCH AS INSERT/DELETE/UPDATE
        public Boolean QueryNonQuery()
        {
            MySqlCommand cmd = null;
            try
            {
                cmd = new MySqlCommand(query, conn);
                cmd.ExecuteNonQuery();
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                switch (ex.Number)
                {
                    case 1054:
                        Console.WriteLine("UNKNOWN COLUMN" + "[" + query + "]");
                        break;

                    case 1146:
                        Console.WriteLine("TABLE DOES NOT EXIST" + "[" + query + "]");
                        break;

                    default:
                        Console.WriteLine("QUERY FAILED! " + "[" + query + "]" + ex.Number);
                        break;
                }
            }

            if (cmd == null)
                return false;
            else
                return true;
        }

        //FOR QUERIES THAT RETURN A SINGLE VALUE
        public void QueryScalar()
        {
            try
            {
                MySqlCommand cmd = new MySqlCommand(query, conn);
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                switch (ex.Number)
                {
                    case 1054:
                        Console.WriteLine("UNKNOWN COLUMN" + "[" + query + "]");
                        break;

                    case 1146:
                        Console.WriteLine("TABLE DOES NOT EXIST" + "[" + query + "]");
                        break;

                    default:
                        Console.WriteLine("QUERY FAILED! " + "[" + query + "]" + ex.Number);
                        break;
                }
                //EXIT AS CANNOT CONTINUE
                Environment.Exit(1);
            }
        }

        //THIS FUNCTION OPENS A CONNECTION TO THE DB
        public void Open()
        {
            conn = new MySqlConnection(dbinfo);
            try
            {
                conn.Open(); // connection must be openned for command
            }
            catch (MySql.Data.MySqlClient.MySqlException ex)
            {
                Console.WriteLine("FAILED TO CONNECT TO DB ERROR #: " + ex.Number);
                switch (ex.Number)
                {
                    case 0:
                        Console.WriteLine("Server Connection Failed!");
                        break;
                    case 1041:
                        Console.WriteLine("Server Error!");
                        break;
                    case 1042:
                        Console.WriteLine("MySQL Net Connector Not Installed Possibly or Failure to Connect to DB Server");
                        break;
                    case 1044:
                        Console.WriteLine("Server DB Error!");
                        break;
                    case 1045:
                        Console.WriteLine("Invalid username/password!");
                        break;
                    case 1049:
                        Console.WriteLine("Server Invalid DB Name!");
                        break;
                }
                //EXIT AS CANNOT CONTINUE
                Environment.Exit(1);
            }
        }

        public void BulkInsert(String insert_into_table, String file_to_read_from)
        {
            //FETCH FIELDS
            String[] fields = SQLiteBulkInsert_Fields(file_to_read_from);

            //BUILD SQL LINE
            String sql = SQLiteBulkInsert_BuildSQL_Line(insert_into_table, fields);

            using (MySqlTransaction dbTrans = conn.BeginTransaction())
            {
                using (MySqlCommand mycommand = conn.CreateCommand())
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


                        //Console.WriteLine("START LINE [{0}]", line);
                        //Console.WriteLine("SQL=[{0}]", mycommand.CommandText);
                        //FETCH VALUES
                        String[] values = SQLiteBulkInsert_TokenizeLine(line);


                        

                        //LOOP THROUGH FIELD LISTING + SET PARAMETERS
                        for (int i = 0; i < fields.Length; i++)
                        {
                            //THIS PART NOT NEEDED DUE TO SWITCHING TO 0 ... n INSTEAD OF FIELD LIST
                            //fields[i] = SQLiteBulkInsert_CleanLine(fields[i]);
                            //Console.WriteLine("@{0} && values[i]={1}", i, values[i]);
                            mycommand.Parameters.AddWithValue("@" + i, values[i]);
                        }



                        //Console.WriteLine("END LINE");

                        //NOW THAT ALL FIELDS + VALUES ARE IN OUR SYSTEM
                        mycommand.ExecuteNonQuery();

                        mycommand.Parameters.Clear();
                    }
                    //CLOSE FILE
                    file.Close();
                }
                dbTrans.Commit();
            }



            /*
            var bl = new MySqlBulkLoader(conn);
            bl.TableName = insert_into_table;
            bl.FieldTerminator = ",";
            bl.LineTerminator = "\r\n";
            bl.FileName = file_to_read_from;
            bl.NumberOfLinesToSkip = 1; //1 AS WE NEED TO SKIP OVER FIELD LISTS
            var inserted = bl.Load();
            //Console.WriteLine(inserted + " rows inserted.");
            */
        }

        //INIT MYSQL READER [WHENEVER WE WANT TO READ A ROW]
        public void initReader()
        {
            //CALL QUERY READER
            reader = (MySqlDataReader)QueryReader();
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
                    hash.Add(fields[i], reader.GetString(fields[i]));
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
                    hash.Add(fields[i], reader.GetString(fields[i]));
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
            string[] parts = line.Split(delimiters, StringSplitOptions.RemoveEmptyEntries);

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
            string[] parts = line.Split(delimiters, StringSplitOptions.RemoveEmptyEntries);

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
            return "DATE_FORMAT(NOW(),\"%Y-%m-%d %k:%i:%s\")";
        }


    }
}