using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Text;
using System.Text.RegularExpressions;

namespace SMAQC
{
    class DBSQLiteTools
    {

        private SQLiteConnection conn;

        private readonly Regex mNonAlphanumericMatcher;

        /// <summary>
        /// Constructor
        /// </summary>
        public DBSQLiteTools()
        {
            mNonAlphanumericMatcher = new Regex("[^A-Z0-9]", RegexOptions.IgnoreCase | RegexOptions.Compiled);
        }

        /// <summary>
        /// Create the database tables
        /// </summary>
        /// <param name="datasource"></param>
        public void CreateTables(string datasource)
        {
            using (conn = new SQLiteConnection("Data Source=" + datasource, true))
            {

                using (var cmd = conn.CreateCommand())
                {

                    conn.Open();

                    // SMAQC results
                    cmd.CommandText = GetTableCreateSql("scan_results");
                    cmd.ExecuteNonQuery();

                    // MASIC ScanStats
                    cmd.CommandText = GetTableCreateSql("temp_scanstats");
                    cmd.ExecuteNonQuery();

                    // MASIC ScanStatsEx
                    cmd.CommandText = GetTableCreateSql("temp_scanstatsex");
                    cmd.ExecuteNonQuery();

                    // MASIC SICStats
                    cmd.CommandText = GetTableCreateSql("temp_sicstats");
                    cmd.ExecuteNonQuery();

                    // MASIC ReporterIons
                    cmd.CommandText = GetTableCreateSql("temp_reporterions");
                    cmd.ExecuteNonQuery();

                    // X!Tandem results
                    cmd.CommandText = GetTableCreateSql("temp_xt");
                    cmd.ExecuteNonQuery();

                    // ResultToSeqMap
                    cmd.CommandText = GetTableCreateSql("temp_xt_resulttoseqmap");
                    cmd.ExecuteNonQuery();

                    // SeqToProteinMap
                    cmd.CommandText = GetTableCreateSql("temp_xt_seqtoproteinmap");
                    cmd.ExecuteNonQuery();

                    // PSMs returned by PHRPReader
                    cmd.CommandText = GetTableCreateSql("temp_PSMs");
                    cmd.ExecuteNonQuery();

                    // CREATE INDICES ON THE TABLES
                    CreateIndices(cmd);

                    conn.Close();
                }
            }
        }

        protected void CreateIndices(SQLiteCommand cmd)
        {
            CreateIndicesMasic(cmd);
            CreateIndicesXTandem(cmd);
            CreateIndicesPHRP(cmd);
        }

        protected void CreateIndicesMasic(SQLiteCommand cmd)
        {           
            CreatePrimaryKey(cmd, "temp_scanstats", "random_id", "ScanNumber");
            CreatePrimaryKey(cmd, "temp_scanstatsex", "random_id", "ScanNumber");
            CreatePrimaryKey(cmd, "temp_sicstats", "random_id", "ParentIonIndex", "FragScanNumber");
            
            CreateIndex(cmd, "temp_scanstats", "ScanNumber");
            CreateIndex(cmd, "temp_scanstatsex", "ScanNumber");

            CreateIndex(cmd, "temp_sicstats", "ParentIonIndex");
            CreateIndex(cmd, "temp_sicstats", "FragScanNumber");
            CreateIndex(cmd, "temp_sicstats", "OptimalPeakApexScanNumber");

            CreateIndicesReporterIons(cmd);

        }

        protected void CreateIndicesReporterIons(SQLiteCommand cmd)
        {
            CreatePrimaryKey(cmd, "temp_reporterions", "random_id", "ScanNumber");
            CreateIndex(cmd, "temp_reporterions", "ScanNumber");
        }

        protected void CreateIndicesXTandem(SQLiteCommand cmd)
        {
            CreateIndex(cmd, "temp_xt", "Scan");
            CreateIndex(cmd, "temp_xt", "Peptide_Expectation_Value_Log");

            CreateIndex(cmd, "temp_xt_resulttoseqmap", "Result_ID");
            CreateIndex(cmd, "temp_xt_resulttoseqmap", "Unique_Seq_ID");

            CreateIndex(cmd, "temp_xt_seqtoproteinmap", "Unique_Seq_ID");
        }

        protected void CreateIndicesPHRP(SQLiteCommand cmd)
        {
            CreateIndex(cmd, "temp_PSMs", "Scan");
            CreateIndex(cmd, "temp_PSMs", "MSGFSpecProb");
        }

        private string AlphanumericOnly(string objectName)
        {
            var cleanName = mNonAlphanumericMatcher.Replace(objectName, "");
            return cleanName;
        }

        private void CreatePrimaryKey(SQLiteCommand cmd, string tableName, string colName1, string colName2 = "", string colName3 = "")
        {
            var tableNameClean = AlphanumericOnly(tableName);

            var colList = "[" + colName1 + "]";

            if (!string.IsNullOrEmpty(colName2))
                colList += ", [" + colName2 + "]";

            if (!string.IsNullOrEmpty(colName3))
                colList += ", [" + colName3 + "]";

            RunSql(cmd, "CREATE UNIQUE INDEX pk_" + tableNameClean + " on [" + tableName + "] (" + colList + ")");
        }

        private void CreateIndex(SQLiteCommand cmd, string tableName, string columnName)
        {
            var tableNameClean = AlphanumericOnly(tableName);
            var columnNameClean = AlphanumericOnly(columnName);

            RunSql(cmd, "CREATE INDEX IX_" + tableNameClean + "_" + columnNameClean + " on [" + tableName + "] ([" + columnName + "])");
        }

        public void create_missing_tables(SQLiteConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                if (!TableExists(connection, "temp_PSMs"))
                {
                    // PSMs returned by PHRPReader
                    cmd.CommandText = GetTableCreateSql("temp_PSMs");
                    cmd.ExecuteNonQuery();

                    CreateIndicesPHRP(cmd);
                }

                if (!TableHasColumn(connection, "temp_PSMs", "Keratinpeptide"))
                {
                    var columnsToAdd = new List<string>
                    {
                        "Keratinpeptide",
                        "MissedCleavages"
                    };

                    AddColumnsToTable(connection, "temp_PSMs", columnsToAdd);
                }

                if (!TableHasColumn(connection, "temp_PSMs", "Trypsinpeptide"))
                {
                    var columnsToAdd = new List<string>
                    {
                        "Trypsinpeptide"
                    };

                    AddColumnsToTable(connection, "temp_PSMs", columnsToAdd);
                }

                if (!TableExists(connection, "temp_reporterions"))
                {
                    // ReporterIons from MASIC
                    cmd.CommandText = GetTableCreateSql("temp_reporterions");
                    cmd.ExecuteNonQuery();

                    CreateIndicesReporterIons(cmd);
                }

                if (!TableHasColumn(connection, "scan_results", "Phos_2A"))
                {
                    var columnsToAdd = new List<string>
                    {
                        "Phos_2A",
                        "Phos_2C"
                    };

                    AddColumnsToTable(connection, "scan_results", columnsToAdd);
                }

                if (!TableHasColumn(connection, "scan_results", "Keratin_2A"))
                {
                    var columnsToAdd = new List<string>
                    {
                        "Keratin_2A",
                        "Keratin_2C",
                        "P_4A",
                        "P_4B"
                    };

                    AddColumnsToTable(connection, "scan_results", columnsToAdd);
                }

                if (!TableHasColumn(connection, "scan_results", "Trypsin_2A"))
                {
                    var columnsToAdd = new List<string>
                    {
                        "Trypsin_2A",
                        "Trypsin_2C"
                    };

                    AddColumnsToTable(connection, "scan_results", columnsToAdd);
                }

            } // End Using


        }

        private void AddColumnsToTable(SQLiteConnection dbConnection, string tableName, List<string> columnsToAdd, string columnType = "VARCHAR", bool isNullable = true)
        {
            foreach (var columnName in columnsToAdd)
            {
                var sqlCommand = "ALTER TABLE '" + tableName + "' ADD COLUMN '" + columnName + "' " + columnType;

                if (isNullable)
                    sqlCommand += " NULL;";
                else
                    sqlCommand += " NOT NULL;";

                using (
                    var cmd = new SQLiteCommand(dbConnection)
                    {
                        CommandText = sqlCommand
                    })
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        protected string GetTableCreateSql(string tableName)
        {
            var sql = string.Empty;

            switch (tableName)
            {
                case "scan_results":

                    // These are added to the table as VARCHAR NULL
                    var metricNames = new List<string>
                    {
                        "C_1A", "C_1B", "C_2A", "C_2B", "C_3A", "C_3B", "C_4A", "C_4B", "C_4C", 
                        "DS_1A", "DS_1B", "DS_2A", "DS_2B", "DS_3A", "DS_3B",
                        "IS_1A", "IS_1B", "IS_2", "IS_3A", "IS_3B", "IS_3C", 
                        "MS1_1", "MS1_2A", "MS1_2B", "MS1_3A", "MS1_3B", "MS1_5A", "MS1_5B", "MS1_5C", "MS1_5D", 
                        "MS2_1", "MS2_2", "MS2_3", "MS2_4A", "MS2_4B", "MS2_4C", "MS2_4D",
                        "P_1A", "P_1B", 
                        "P_2A", "P_2B", "P_2C", 
                        "P_3", 
                        "Phos_2A", "Phos_2C", 
                        "Keratin_2A", "Keratin_2C", 
                        "P_4A", "P_4B", 
                        "Trypsin_2A", "Trypsin_2C",
                    };

                    sql = "CREATE TABLE [scan_results] ("
                          + "[result_id] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                          + "[scan_id] INTEGER NOT NULL,"
                          + "[instrument_id] VARCHAR NOT NULL,"
                          + "[random_id] INTEGER  NOT NULL,"
                          + "[scan_date] VARCHAR NOT NULL,"
                          + VarcharColumnNamesToSql(metricNames)                     
                        + ")";
                    break;

                case "temp_scanstats":
                    sql = "CREATE TABLE [temp_scanstats] ("
                        + "[instrument_id] INTEGER NOT NULL,"
                        + "[random_id] INTEGER NOT NULL,"
                        + "[Dataset] INTEGER NOT NULL,"
                        + "[ScanNumber] INTEGER NOT NULL,"
                        + "[ScanTime] FLOAT NOT NULL,"
                        + "[ScanType] INTEGER NOT NULL,"
                        + "[TotalIonIntensity] FLOAT NOT NULL,"
                        + "[BasePeakIntensity] FLOAT NOT NULL,"
                        + "[BasePeakMZ] FLOAT NOT NULL,"
                        + "[BasePeakSignalToNoiseRatio] FLOAT NOT NULL,"
                        + "[IonCount] FLOAT NULL,"
                        + "[IonCountRaw] INTEGER NULL,"
                        + "[ScanTypeName] VARCHAR NULL"
                        + ")";
                    break;

                case "temp_scanstatsex":
                    sql = "CREATE TABLE [temp_scanstatsex] ("
                        + "[instrument_id] INTEGER NOT NULL,"
                        + "[random_id] INTEGER NOT NULL,"
                        + "[Dataset] INTEGER NOT NULL,"
                        + "[ScanNumber] INTEGER NOT NULL,"
                        + "[Ion_Injection_Time] FLOAT NULL,"
                        + "[Scan_Segment] VARCHAR NULL,"
                        + "[Scan_Event] INTEGER NULL,"
                        + "[Master_Index] INTEGER NULL,"
                        + "[Elapsed_Scan_Time] FLOAT NULL,"
                        + "[Charge_State] FLOAT NULL,"
                        + "[Monoisotopic_MZ] FLOAT NULL,"
                        + "[MS2_Isolation_Width] FLOAT NULL,"
                        + "[FT_Analyzer_Settings] VARCHAR NULL,"
                        + "[FT_Analyzer_Message] VARCHAR NULL,"
                        + "[FT_Resolution] FLOAT NULL,"
                        + "[Conversion_Parameter_B] FLOAT NULL,"
                        + "[Conversion_Parameter_C] FLOAT NULL,"
                        + "[Conversion_Parameter_D] FLOAT NULL,"
                        + "[Conversion_Parameter_E] FLOAT NULL,"
                        + "[Collision_Mode] VARCHAR DEFAULT NULL,"
                        + "[Scan_Filter_Text] VARCHAR NULL,"
                        + "[Source_Voltage] FLOAT NULL,"
                        + "[Source_Current] FLOAT NULL"
                        + ")";
                    break;

                case "temp_sicstats":
                    sql = "CREATE TABLE [temp_sicstats] ("
                        + "[instrument_id] INTEGER NOT NULL,"
                        + "[random_id] INTEGER NOT NULL,"
                        + "[Dataset] INTEGER NOT NULL,"
                        + "[ParentIonIndex] INTEGER NOT NULL,"
                        + "[MZ] FLOAT NOT NULL,"
                        + "[SurveyScanNumber] INTEGER NOT NULL,"
                        + "[FragScanNumber] INTEGER NOT NULL,"
                        + "[OptimalPeakApexScanNumber] INTEGER NOT NULL,"
                        + "[PeakApexOverrideParentIonIndex] INTEGER NOT NULL,"
                        + "[CustomSICPeak] INTEGER NOT NULL,"
                        + "[PeakScanStart] INTEGER NOT NULL,"
                        + "[PeakScanEnd] INTEGER NOT NULL,"
                        + "[PeakScanMaxIntensity] INTEGER NOT NULL,"
                        + "[PeakMaxIntensity] FLOAT NOT NULL,"
                        + "[PeakSignalToNoiseRatio] FLOAT NOT NULL,"
                        + "[FWHMInScans] INTEGER NOT NULL,"
                        + "[PeakArea] FLOAT NOT NULL,"
                        + "[ParentIonIntensity] FLOAT NOT NULL,"
                        + "[PeakBaselineNoiseLevel] INTEGER NOT NULL,"
                        + "[PeakBaselineNoiseStDev] INTEGER NOT NULL,"
                        + "[PeakBaselinePointsUsed] INTEGER NOT NULL,"
                        + "[StatMomentsArea] FLOAT NOT NULL,"
                        + "[CenterOfMassScan] INTEGER NOT NULL,"
                        + "[PeakStDev] FLOAT NOT NULL,"
                        + "[PeakSkew] FLOAT NOT NULL,"
                        + "[PeakKSStat] FLOAT NOT NULL,"
                        + "[StatMomentsDataCountUsed] INTEGER NOT NULL"
                        + ")";
                    break;

                case "temp_reporterions":
                    sql = "CREATE TABLE [temp_reporterions] ("
                        + "[instrument_id] INTEGER NOT NULL,"
                        + "[random_id] INTEGER NOT NULL,"
                        + "[Dataset] INTEGER NOT NULL,"
                        + "[ScanNumber] INTEGER NOT NULL,"
                        + "[CollisionMode] VARCHAR NULL,"
                        + "[ParentIonMZ] FLOAT NOT NULL,"
                        + "[BasePeakIntensity] FLOAT NOT NULL,"
                        + "[BasePeakMZ] FLOAT NOT NULL,"
                        + "[ReporterIonIntensityMax] FLOAT NOT NULL,"
                        + "[Ion_1] FLOAT NOT NULL,"
                        + "[Ion_2] FLOAT NOT NULL,"
                        + "[Ion_3] FLOAT NOT NULL,"
                        + "[Ion_4] FLOAT NOT NULL,"
                        + "[Ion_5] FLOAT NOT NULL,"
                        + "[Ion_7] FLOAT NOT NULL,"
                        + "[Ion_8] FLOAT NOT NULL,"
                        + "[Ion_9] FLOAT NOT NULL,"
                        + "[Ion_10] FLOAT NOT NULL,"
                        + "[Ion_11] FLOAT NOT NULL,"
                        + "[Ion_12] FLOAT NOT NULL,"
                        + "[Ion_13] FLOAT NOT NULL,"
                        + "[Ion_14] FLOAT NOT NULL,"
                        + "[Ion_15] FLOAT NOT NULL,"
                        + "[PctIntensityCorrection] FLOAT NOT NULL"
                        + ")";
                    break;

                case "temp_xt":
                    sql = "CREATE TABLE [temp_xt] ("
                        + "[instrument_id] INTEGER NOT NULL,"
                        + "[random_id] INTEGER NOT NULL,"
                        + "[Result_ID] INTEGER NOT NULL,"
                        + "[Group_ID] INTEGER NOT NULL,"
                        + "[Scan] INTEGER NOT NULL,"
                        + "[Charge] INTEGER NOT NULL,"
                        + "[Peptide_MH] FLOAT NOT NULL,"
                        + "[Peptide_Hyperscore] FLOAT NOT NULL,"
                        + "[Peptide_Expectation_Value_Log] FLOAT NOT NULL,"
                        + "[Multiple_Protein_Count] FLOAT NOT NULL,"
                        + "[Peptide_Sequence] VARCHAR NOT NULL,"
                        + "[DeltaCn2] FLOAT NOT NULL,"
                        + "[y_score] FLOAT NOT NULL,"
                        + "[y_ions] FLOAT NOT NULL,"
                        + "[b_score] FLOAT NOT NULL,"
                        + "[b_ions] FLOAT NOT NULL,"
                        + "[Delta_Mass] FLOAT NOT NULL,"
                        + "[Peptide_Intensity_Log] FLOAT NOT NULL,"
                        + "[DelM_PPM] FLOAT NULL"
                        + ")";
                    break;

                case "temp_xt_resulttoseqmap":
                    sql = "CREATE TABLE [temp_xt_resulttoseqmap] ("
                        + "[instrument_id] INTEGER NOT NULL,"
                        + "[random_id] INTEGER NOT NULL,"
                        + "[Result_ID] INTEGER NOT NULL,"
                        + "[Unique_Seq_ID] INTEGER NOT NULL"
                        + ")";
                    break;

                case "temp_xt_seqtoproteinmap":
                    sql = "CREATE TABLE [temp_xt_seqtoproteinmap] ("
                        + "[instrument_id] INTEGER NOT NULL,"
                        + "[random_id] INTEGER NOT NULL,"
                        + "[Unique_Seq_ID] INTEGER NOT NULL,"
                        + "[Cleavage_State] INTEGER NOT NULL,"
                        + "[Terminus_State] INTEGER NOT NULL,"
                        + "[Protein_Name] VARCHAR NOT NULL,"
                        + "[Protein_Expectation_Value_Log] FLOAT NOT NULL,"
                        + "[Protein_Intensity_Log] FLOAT NOT NULL"
                        + ")";
                    break;

                case "temp_PSMs":
                    sql = "CREATE TABLE [temp_PSMs] ("
                        + "[instrument_id] INTEGER NOT NULL,"
                        + "[random_id] INTEGER NOT NULL,"
                        + "[Result_ID] INTEGER NOT NULL,"
                        + "[Scan] INTEGER NOT NULL,"
                        + "[CollisionMode] VARCHAR NULL,"
                        + "[Charge] INTEGER NOT NULL,"
                        + "[Peptide_MH] FLOAT NOT NULL,"
                        + "[Peptide_Sequence] VARCHAR NOT NULL,"
                        + "[DelM_Da] FLOAT NULL,"
                        + "[DelM_PPM] FLOAT NULL,"
                        + "[MSGFSpecProb] FLOAT NULL, "
                        + "[Unique_Seq_ID] INTEGER NOT NULL,"
                        + "[Cleavage_State] INTEGER NOT NULL,"
                        + "[Phosphopeptide] INTEGER NOT NULL,"
                        + "[Keratinpeptide] INTEGER NOT NULL,"
                        + "[MissedCleavages] INTEGER NOT NULL,"
                        + "[Trypsinpeptide] INTEGER NOT NULL"
                        + ")";
                    break;

            }

            return sql;

        }

        /// <summary>
        /// Generate the SQL to create varchar columns on a table
        /// </summary>
        /// <param name="metricNames">List of column names</param>
        /// <returns>
        /// List of the form:
        /// [Metric1] VARCHAR NULL, [Metric2] VARCHAR NULL, [Metric3] VARCHAR NULL
        /// </returns>
        private string VarcharColumnNamesToSql(IEnumerable<string> metricNames)
        {
            var columnList = new StringBuilder();

            foreach (var metric in metricNames)
            {
                if (columnList.Length > 0)
                    columnList.Append(",");

                columnList.Append("[" + metric + "] VARCHAR NULL");
            }

            return columnList.ToString();
        }

        public static bool TableExists(SQLiteConnection conn, string tableName)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT COUNT(*) AS Tables FROM sqlite_master where type = 'table' and name = '" + tableName + "'";

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read() && reader.GetInt32(0) > 0)
                    {
                        return true;
                    }
                }

            }

            return false;
        }

        public static bool TableHasColumn(SQLiteConnection conn, string tableName, string columnName)
        {
            bool hasColumn;

            using (
                var cmd = new SQLiteCommand(conn)
                {
                    CommandText = "Select * From '" + tableName + "' Limit 1;"
                })
            {
                using (var reader = cmd.ExecuteReader())
                {
                    hasColumn = reader.GetOrdinal(columnName) >= 0;
                }
            }

            return hasColumn;
        }

        protected void RunSql(SQLiteCommand cmd, string sql)
        {
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
    }
}
