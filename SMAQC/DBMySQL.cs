using System;
using MySql.Data.MySqlClient;
using System.Data;
//using System.Data.SqlClient;
using System.Collections;
using System.IO;
using System.Data.Common;

namespace SMAQC
{
    class DBMySQL : IDBInterface
    {
        public Boolean db_open = false;             // Is db open?
        public string dbinfo;                       // Db info
        public MySqlConnection conn = null;                // Db conn variable
        MySqlDataReader reader;
        string query;                               // Query to be executed

        // Constructor
        public DBMySQL(string dbhost, string dbuser, string dbpass, string dbname)
        {
            // Set db info
            dbinfo = "server=" + dbhost + ";database=" + dbname + ";uid=" + dbuser + ";password=" + dbpass;

            // Open db conn
            this.Open();

            // Console.writeline("mysqldb() constructor ({0})", dbinfo);
        }

        // Destructor
        ~DBMySQL()
        {
            // Console.writeline("mysqldb() de-constructor");
            try
            {
                conn.Close();
            }
            catch (System.NullReferenceException ex)
            {
            }
        }

        // Clear db tables
        public void clearTempTables(int r_id, String[] db_tables)
        {
            // Loop through each temp table
            for (int i = 0; i < db_tables.Length; i++)
            {
                // Create query
                String temp_string = "TRUNCATE `" + db_tables[i] + "`;";

                // Set query
                this.setQuery(temp_string);

                // Call query function
                this.QueryNonQuery();
            }
        }

        public void setQuery(string myquery)
        {
            // Set query to param
            query = myquery;
        }

        // For queries that return rows
        // Public mysqldatareader queryreader()
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

        // For queries such as insert/delete/update
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

        // For queries that return a single value
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
                // Exit as cannot continue
                Environment.Exit(1);
            }
        }

        // This function opens a connection to the db
        public void Open()
        {
            conn = new MySqlConnection(dbinfo);
            try
            {
                conn.Open(); // Connection must be openned for command
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
                // Exit as cannot continue
                Environment.Exit(1);
            }
        }

        public void BulkInsert(String insert_into_table, String file_to_read_from)
        {
            // Fetch fields
            String[] fields = SQLiteBulkInsert_Fields(file_to_read_from);

            // Build sql line
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
                        // Check if field listing line #
                        if (line_num == 0)
                        {
                            line_num++;
                            continue;// Skip this line as we do not want the field listing
                        }

                        // Console.writeline("start line [{0}]", line);
                        // Console.writeline("sql=[{0}]", mycommand.commandtext);
                        // Fetch values
                        String[] values = SQLiteBulkInsert_TokenizeLine(line);

                        // Loop through field listing + set parameters
                        for (int i = 0; i < fields.Length; i++)
                        {
                            // This part not needed due to switching to 0 ... n instead of field list
                            // Fields[i] = sqlitebulkinsert_cleanline(fields[i]);
                            // Console.writeline("@{0} && values[i]={1}", i, values[i]);
                            mycommand.Parameters.AddWithValue("@" + i, values[i]);
                        }

                        // Console.writeline("end line");

                        // Now that all fields + values are in our system
                        mycommand.ExecuteNonQuery();

                        mycommand.Parameters.Clear();
                    }
                    // Close file
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
            bl.NumberOfLinesToSkip = 1; // 1 As we need to skip over field lists
            var inserted = bl.Load();
            // Console.writeline(inserted + " rows inserted.");
            */
        }

        // Init mysql reader [whenever we want to read a row]
        public void initReader()
        {
            // Call query reader
            reader = (MySqlDataReader)QueryReader();
        }

        // Read single db row [different from readlines() as here we close reader afterwords]
        // [Returns false if no further rows to read]
        public Boolean readSingleLine(String[] fields, ref Hashtable hash)
        {
            // Declare variable
            Boolean status;

            // Read line
            status = reader.Read();

            // If returned false ... no rows to return
            if (!status)
            {
                // Close reader
                reader.Close();

                // Return false as no more to read
                return false;
            }

            // Declare variables
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

            // Close reader
            reader.Close();

            // Return true since read == ok
            return true;
        }

        // Read db row(s) [returns false if no further rows to read]
        public Boolean readLines(String[] fields, ref Hashtable hash)
        {
            // Declare variable
            Boolean status;

            // Read line
            status = reader.Read();

            // If returned false ... no rows to return
            if (!status)
            {
                // Close reader
                reader.Close();

                // Return false as no more to read
                return false;
            }

            // Declare variables
            for (int i = 0; i < fields.Length; i++)
            {
                // Read + store field [value, result]
                try
                {
                    hash.Add(fields[i], reader.GetString(fields[i]));
                }
                catch (System.Data.SqlTypes.SqlNullValueException)
                {

                }
            }

            // Return true since read == ok
            return true;
        }

        String[] SQLiteBulkInsert_Fields(String filename)
        {
            StreamReader file = new StreamReader(filename);
            String line = file.ReadLine();
            file.Close();

            // Split given data files by tab
            char[] delimiters = new char[] { ',' };

            // Do split operation
            string[] parts = line.Split(delimiters, StringSplitOptions.RemoveEmptyEntries);

            for (int i = 0; i < parts.Length; i++)
            {
                parts[i] = SQLiteBulkInsert_CleanFields(parts[i]);
            }

            return parts;
        }

        String[] SQLiteBulkInsert_TokenizeLine(String line)
        {
            // Split given data files by tab
            char[] delimiters = new char[] { ',' };

            // If line contains ",," which means == field is allowed null [scanstatsex fix]
            while (line.Contains(",,"))
            {
                line = line.Replace(",,", ", ,");
            }

            // Do split operation
            string[] parts = line.Split(delimiters, StringSplitOptions.RemoveEmptyEntries);

            return parts;
        }

        String SQLiteBulkInsert_BuildSQL_Line(String table, String[] parts)
        {
            String sql = "";

            // Build base
            sql += "INSERT INTO " + table + " (";

            // Build commands
            for (int i = 0; i < parts.Length; i++)
            {
                sql += "`" + parts[i] + "`,";
            }
            // There is now an extra , find index of it + remove
            sql = sql.Remove(sql.LastIndexOf(","));

            // Add end of commands
            sql += ") VALUES (";

            // Build value list
            for (int i = 0; i < parts.Length; i++)
            {
                sql += "@" + i + ",";
            }
            // There is now an extra , find index of it + remove
            sql = sql.Remove(sql.LastIndexOf(","));

            // Add end of values
            sql += ");";
            return sql;
        }

        String SQLiteBulkInsert_CleanFields(String field)
        {
            String field_line = field;

            // If == space
            if (field_line.Contains(" "))
            {
                // Replace blanks with _
                field_line = field_line.Replace(" ", "_");
            }

            // Replace all instances of _(
            while (field_line.IndexOf("_(") > 0)
            {
                field_line = field_line.Remove(field_line.IndexOf("_("));
            }

            // Replace all instances of (
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