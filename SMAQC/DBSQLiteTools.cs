using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Text;
using System.Text.RegularExpressions;

namespace SMAQC
{
    internal class DBSQLiteTools
    {
        // Ignore Spelling: xt, frag, phos, plex, hyperscore, da, sqlite

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
        /// <param name="dbPath">Path to the SQLite database</param>
        public void CreateTables(string dbPath)
        {
            using var conn = new SQLiteConnection("Data Source=" + dbPath, true);

            using var cmd = conn.CreateCommand();
            conn.Open();

            // SMAQC results
            cmd.CommandText = GetTableCreateSql("scan_results");
            cmd.ExecuteNonQuery();

            // MASIC ScanStats
            cmd.CommandText = GetTableCreateSql("temp_ScanStats");
            cmd.ExecuteNonQuery();

            // MASIC ScanStatsEx
            cmd.CommandText = GetTableCreateSql("temp_ScanStatsEx");
            cmd.ExecuteNonQuery();

            // MASIC SICStats
            cmd.CommandText = GetTableCreateSql("temp_SICStats");
            cmd.ExecuteNonQuery();

            // MASIC ReporterIons
            cmd.CommandText = GetTableCreateSql("temp_ReporterIons");
            cmd.ExecuteNonQuery();

            // X!Tandem results
            cmd.CommandText = GetTableCreateSql("temp_xt");
            cmd.ExecuteNonQuery();

            // ResultToSeqMap
            cmd.CommandText = GetTableCreateSql("temp_xt_ResultToSeqMap");
            cmd.ExecuteNonQuery();

            // SeqToProteinMap
            cmd.CommandText = GetTableCreateSql("temp_xt_SeqToProteinMap");
            cmd.ExecuteNonQuery();

            // PSMs returned by PHRPReader
            cmd.CommandText = GetTableCreateSql("temp_PSMs");
            cmd.ExecuteNonQuery();

            // Create the indices on the tables
            CreateIndices(cmd);

            conn.Close();
        }

        private void CreateIndices(SQLiteCommand cmd)
        {
            CreateIndicesMasic(cmd);
            CreateIndicesXTandem(cmd);
            CreateIndicesPHRP(cmd);
        }

        private void CreateIndicesMasic(SQLiteCommand cmd)
        {
            CreatePrimaryKey(cmd, "temp_ScanStats", "random_id", "ScanNumber");
            CreatePrimaryKey(cmd, "temp_ScanStatsEx", "random_id", "ScanNumber");
            CreatePrimaryKey(cmd, "temp_SICStats", "random_id", "ParentIonIndex", "FragScanNumber");

            CreateIndex(cmd, "temp_ScanStats", "ScanNumber");
            CreateIndex(cmd, "temp_ScanStatsEx", "ScanNumber");

            CreateIndex(cmd, "temp_SICStats", "ParentIonIndex");
            CreateIndex(cmd, "temp_SICStats", "FragScanNumber");
            CreateIndex(cmd, "temp_SICStats", "OptimalPeakApexScanNumber");

            CreateIndicesReporterIons(cmd);
        }

        private void CreateIndicesReporterIons(SQLiteCommand cmd)
        {
            CreatePrimaryKey(cmd, "temp_ReporterIons", "random_id", "ScanNumber");
            CreateIndex(cmd, "temp_ReporterIons", "ScanNumber");
        }

        private void CreateIndicesXTandem(SQLiteCommand cmd)
        {
            CreateIndex(cmd, "temp_xt", "Scan");
            CreateIndex(cmd, "temp_xt", "Peptide_Expectation_Value_Log");

            CreateIndex(cmd, "temp_xt_ResultToSeqMap", "Result_ID");
            CreateIndex(cmd, "temp_xt_ResultToSeqMap", "Unique_Seq_ID");

            CreateIndex(cmd, "temp_xt_SeqToProteinMap", "Unique_Seq_ID");
        }

        private void CreateIndicesPHRP(SQLiteCommand cmd)
        {
            CreateIndex(cmd, "temp_PSMs", "Scan");
            CreateIndex(cmd, "temp_PSMs", "MSGFSpecProb");
        }

        private string AlphanumericOnly(string objectName)
        {
            var cleanName = mNonAlphanumericMatcher.Replace(objectName, string.Empty);
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

        public void CreateMissingTables(SQLiteConnection connection)
        {
            using var cmd = connection.CreateCommand();

            if (!TableExists(connection, "temp_PSMs"))
            {
                // PSMs returned by PHRPReader
                cmd.CommandText = GetTableCreateSql("temp_PSMs");
                cmd.ExecuteNonQuery();

                CreateIndicesPHRP(cmd);
            }

            if (!TableHasColumn(connection, "temp_PSMs", "KeratinPeptide"))
            {
                var columnsToAdd = new List<string>
                {
                    "KeratinPeptide",
                    "MissedCleavages"
                };

                AddColumnsToTable(connection, "temp_PSMs", columnsToAdd);
            }

            if (!TableHasColumn(connection, "temp_PSMs", "TrypsinPeptide"))
            {
                var columnsToAdd = new List<string>
                {
                    "TrypsinPeptide"
                };

                AddColumnsToTable(connection, "temp_PSMs", columnsToAdd);
            }

            if (!TableExists(connection, "temp_ReporterIons"))
            {
                // ReporterIons from MASIC
                cmd.CommandText = GetTableCreateSql("temp_ReporterIons");
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

            if (!TableHasColumn(connection, "scan_results", "MS2_RepIon_All"))
            {
                var columnsToAdd = new List<string>
                {
                    "MS2_RepIon_All",
                    "MS2_RepIon_1Missing",
                    "MS2_RepIon_2Missing",
                    "MS2_RepIon_3Missing",
                };

                AddColumnsToTable(connection, "scan_results", columnsToAdd);
            }
        }

        /// <summary>
        /// Assure that the specified columns exist in the table
        /// Add them if missing
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="tableName">Target table</param>
        /// <param name="columnNames">Required column names</param>
        /// <param name="columnType">Data type for the columns if they need to be added (varchar, float, int)</param>
        /// <param name="isNullable">True if the columns can contain nulls</param>
        public void AssureColumnsExist(
            SQLiteConnection conn,
            string tableName,
            IEnumerable<string> columnNames,
            string columnType = "VARCHAR",
            bool isNullable = true)
        {
            if (!TableExists(conn, tableName))
                return;

            var missingColumns = new List<string>();

            foreach (var columnName in columnNames)
            {
                if (!TableHasColumn(conn, tableName, columnName))
                {
                    missingColumns.Add(columnName);
                }
            }

            if (missingColumns.Count > 0)
            {
                AddColumnsToTable(conn, tableName, missingColumns, columnType, isNullable);
            }
        }

        private void AddColumnsToTable(
            SQLiteConnection dbConnection,
            string tableName,
            IEnumerable<string> columnsToAdd,
            string columnType = "VARCHAR",
            bool isNullable = true)
        {
            foreach (var columnName in columnsToAdd)
            {
                var sqlCommand = "ALTER TABLE '" + tableName + "' ADD COLUMN '" + columnName + "' " + columnType;

                if (isNullable)
                    sqlCommand += " NULL;";
                else
                    sqlCommand += " NOT NULL;";

                using var cmd = new SQLiteCommand(dbConnection)
                {
                    CommandText = sqlCommand
                };

                cmd.ExecuteNonQuery();
            }
        }

        private string GetTableCreateSql(string tableName)
        {
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
                        "MS2_RepIon_All", "MS2_RepIon_1Missing", "MS2_RepIon_2Missing", "MS2_RepIon_3Missing"
                    };

                    return "CREATE TABLE [scan_results] ("
                          + "[result_id] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                          + "[scan_id] INTEGER NOT NULL,"
                          + "[instrument_id] VARCHAR NOT NULL,"
                          + "[random_id] INTEGER  NOT NULL,"
                          + "[scan_date] VARCHAR NOT NULL,"
                          + VarcharColumnNamesToSql(metricNames)
                        + ")";

                case "temp_ScanStats":
                    return "CREATE TABLE [temp_ScanStats] ("
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

                case "temp_ScanStatsEx":
                    return "CREATE TABLE [temp_ScanStatsEx] ("
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

                case "temp_SICStats":
                    return "CREATE TABLE [temp_SICStats] ("
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

                case "temp_ReporterIons":
                    return "CREATE TABLE [temp_ReporterIons] ("
                        + "[instrument_id] INTEGER NOT NULL,"
                        + "[random_id] INTEGER NOT NULL,"
                        + "[Dataset] INTEGER NOT NULL,"
                        + "[ScanNumber] INTEGER NOT NULL,"
                        + "[Collision_Mode] VARCHAR NULL,"
                        + "[ParentIonMZ] FLOAT NOT NULL,"
                        + "[BasePeakIntensity] FLOAT NOT NULL,"
                        + "[BasePeakMZ] FLOAT NOT NULL,"
                        // + "[ParentScan] INTEGER NOT NULL,"
                        + "[ReporterIonIntensityMax] FLOAT NOT NULL,"
                        // + "[Ion_101] FLOAT NULL,"       // ETD iTRAQ Ion
                        // + "[Ion_102] FLOAT NULL,"       // ETD iTRAQ Ion
                        // + "[Ion_104] FLOAT NULL,"       // ETD iTRAQ Ion

                        // + "[Ion_113] FLOAT NULL,"       // 8-plex iTRAQ Ion
                        // + "[Ion_114] FLOAT NULL,"       // 4-plex and 8-plex iTRAQ Ion
                        // + "[Ion_115] FLOAT NULL,"       // 4-plex and 8-plex iTRAQ Ion
                        // + "[Ion_116] FLOAT NULL,"       // 4-plex and 8-plex iTRAQ Ion
                        // + "[Ion_117] FLOAT NULL,"       // 4-plex and 8-plex iTRAQ Ion
                        // + "[Ion_118] FLOAT NULL,"       // iTRAQ Ion
                        // + "[Ion_119] FLOAT NULL,"       // iTRAQ Ion
                        // + "[Ion_120] FLOAT NULL,"       // iTRAQ Ion
                        // + "[Ion_121] FLOAT NULL,"       // iTRAQ Ion

                        // + "[Ion_126] FLOAT NULL,"       // TMT Ion (TMT6)
                        // + "[Ion_127] FLOAT NULL,"       // TMT Ion (TMT6)
                        // + "[Ion_128] FLOAT NULL,"       // TMT Ion (TMT6)
                        // + "[Ion_129] FLOAT NULL,"       // TMT Ion (TMT6)
                        // + "[Ion_130] FLOAT NULL,"       // TMT Ion (TMT6)
                        // + "[Ion_131] FLOAT NULL,"       // TMT Ion (TMT6)

                        // + "[Ion_126.128] FLOAT NULL,"   // TMT Ion (TMT10)
                        // + "[Ion_127.125] FLOAT NULL,"   // TMT Ion (TMT10)
                        // + "[Ion_127.131] FLOAT NULL,"   // TMT Ion (TMT10)
                        // + "[Ion_128.128] FLOAT NULL,"   // TMT Ion (TMT10)
                        // + "[Ion_128.134] FLOAT NULL,"   // TMT Ion (TMT10)
                        // + "[Ion_129.131] FLOAT NULL,"   // TMT Ion (TMT10)
                        // + "[Ion_129.138] FLOAT NULL,"   // TMT Ion (TMT10)
                        // + "[Ion_130.135] FLOAT NULL,"   // TMT Ion (TMT10)
                        // + "[Ion_130.141] FLOAT NULL,"   // TMT Ion (TMT10)
                        // + "[Ion_131.138] FLOAT NULL,"   // TMT Ion (TMT10)

                        + "[Weighted_Avg_Pct_Intensity_Correction] FLOAT NOT NULL"
                        + ")";

                case "temp_xt":
                    return "CREATE TABLE [temp_xt] ("
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

                case "temp_xt_ResultToSeqMap":
                    return "CREATE TABLE [temp_xt_ResultToSeqMap] ("
                        + "[instrument_id] INTEGER NOT NULL,"
                        + "[random_id] INTEGER NOT NULL,"
                        + "[Result_ID] INTEGER NOT NULL,"
                        + "[Unique_Seq_ID] INTEGER NOT NULL"
                        + ")";

                case "temp_xt_SeqToProteinMap":
                    return "CREATE TABLE [temp_xt_SeqToProteinMap] ("
                        + "[instrument_id] INTEGER NOT NULL,"
                        + "[random_id] INTEGER NOT NULL,"
                        + "[Unique_Seq_ID] INTEGER NOT NULL,"
                        + "[Cleavage_State] INTEGER NOT NULL,"
                        + "[Terminus_State] INTEGER NOT NULL,"
                        + "[Protein_Name] VARCHAR NOT NULL,"
                        + "[Protein_Expectation_Value_Log] FLOAT NOT NULL,"
                        + "[Protein_Intensity_Log] FLOAT NOT NULL"
                        + ")";

                case "temp_PSMs":
                    return "CREATE TABLE [temp_PSMs] ("
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
                        + "[KeratinPeptide] INTEGER NOT NULL,"
                        + "[MissedCleavages] INTEGER NOT NULL,"
                        + "[TrypsinPeptide] INTEGER NOT NULL"
                        + ")";

                default:
                    throw new ArgumentException(nameof(tableName));
            }
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

                columnList.AppendFormat("[{0}] VARCHAR NULL", metric);
            }

            return columnList.ToString();
        }

        public static bool TableExists(SQLiteConnection conn, string tableName)
        {
            using var cmd = conn.CreateCommand();

            cmd.CommandText = "SELECT COUNT(*) AS Tables FROM sqlite_master where type = 'table' and name = '" + tableName + "'";

            using var reader = cmd.ExecuteReader();

            return reader.Read() && reader.GetInt32(0) > 0;
        }

        public static bool TableHasColumn(SQLiteConnection conn, string tableName, string columnName)
        {
            using var cmd = new SQLiteCommand(conn)
            {
                CommandText = "Select * From '" + tableName + "' Limit 1;"
            };

            using var reader = cmd.ExecuteReader();

            return reader.GetOrdinal(columnName) >= 0;
        }

        // ReSharper disable once SuggestBaseTypeForParameter
        private void RunSql(SQLiteCommand cmd, string sql)
        {
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
    }
}
