using System.Data.SQLite;

namespace SMAQC
{
    class DBSQLiteTools
    {
        //DECLARE VARIABLES
        private SQLiteConnection conn;                                                  //SQLITE CONNECTION

        //CREATE TABLES
        public void create_tables(string datasource)
        {
            using (conn = new SQLiteConnection("Data Source=" + datasource))
            {
                //RUN CREATE CMD
                using (var cmd = conn.CreateCommand())
                {
                    //OPEN DB
                    conn.Open();

					// SMAQC results
                    cmd.CommandText = GetTableCreateSql("scan_results");
                    cmd.ExecuteNonQuery();

					// MASIC ScanStats
					cmd.CommandText =GetTableCreateSql("temp_scanstats");
                    cmd.ExecuteNonQuery();

					// MASIC ScanStatsEx
                    cmd.CommandText =GetTableCreateSql("temp_scanstatsex");
                    cmd.ExecuteNonQuery();

					// MASIC SICStats
                    cmd.CommandText =GetTableCreateSql("temp_sicstats");
                    cmd.ExecuteNonQuery();

                    // X!Tandem results
                    cmd.CommandText =GetTableCreateSql("temp_xt");
                    cmd.ExecuteNonQuery();

                    // ResultToSeqMap
                    cmd.CommandText =GetTableCreateSql("temp_xt_resulttoseqmap");
                    cmd.ExecuteNonQuery();

                    // SeqToProteinMap
					cmd.CommandText = GetTableCreateSql("temp_xt_seqtoproteinmap");
                    cmd.ExecuteNonQuery();

					// PSMs returned by PHRPReader
					cmd.CommandText = GetTableCreateSql("temp_PSMs");
					cmd.ExecuteNonQuery();				

					// CREATE INDICES ON THE TABLES
					CreateIndices(cmd);

                    //CLOSE DB
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
			RunSql(cmd, "CREATE UNIQUE INDEX pk_temp_ScanStats on temp_scanstats(random_id, ScanNumber)");
			RunSql(cmd, "CREATE UNIQUE INDEX pk_temp_ScanStatsEx on temp_scanstatsex(random_id, ScanNumber)");
			RunSql(cmd, "CREATE UNIQUE INDEX pk_temp_SicStats on temp_sicstats(random_id, ParentIonIndex, FragScanNumber)");

			RunSql(cmd, "CREATE INDEX IX_temp_ScanStats on temp_scanstats(ScanNumber)");

			RunSql(cmd, "CREATE INDEX IX_temp_ScanStatsEx on temp_scanstatsex(ScanNumber)");

			RunSql(cmd, "CREATE INDEX IX_temp_SicStats on temp_sicstats(ParentIonIndex)");
			RunSql(cmd, "CREATE INDEX IX_temp_SicStats_FragScan on temp_sicstats(FragScanNumber)");
			RunSql(cmd, "CREATE INDEX IX_temp_SicStats_PeakApexScan on temp_sicstats(OptimalPeakApexScanNumber)");
		}

		protected void CreateIndicesXTandem(SQLiteCommand cmd)
		{
			RunSql(cmd, "CREATE INDEX ix_temp_xt_Scan on temp_xt(Scan)");
			RunSql(cmd, "CREATE INDEX ix_temp_xt_LogEValue on temp_xt(Peptide_Expectation_Value_Log)");

			RunSql(cmd, "CREATE INDEX ix_temp_xt_resulttoseqmap_ResultID on temp_xt_resulttoseqmap(Result_ID)");
			RunSql(cmd, "CREATE INDEX ix_temp_xt_resulttoseqmap_SeqID on temp_xt_resulttoseqmap(Unique_Seq_ID)");

			RunSql(cmd, "CREATE INDEX ix_temp_xt_seqtoproteinmap_SeqID on temp_xt_seqtoproteinmap(Unique_Seq_ID)");
		}

		protected void CreateIndicesPHRP(SQLiteCommand cmd)
		{
			RunSql(cmd, "CREATE INDEX ix_temp_PSMs_Scan on temp_PSMs(Scan)");
			RunSql(cmd, "CREATE INDEX ix_temp_PSMs_LogEValue on temp_PSMs(MSGFSpecProb)");

		}

		public void create_missing_tables(SQLiteConnection connection)
		{
			using (SQLiteCommand cmd = connection.CreateCommand())
			{
				if (!TableExists(connection, "temp_PSMs"))
				{
					// PSMs returned by PHRPReader
					cmd.CommandText = GetTableCreateSql("temp_PSMs");
					cmd.ExecuteNonQuery();

					CreateIndicesPHRP(cmd);
				}
			}

		
		}
	
		protected string GetTableCreateSql(string tableName)
		{
			string sql = string.Empty;

			switch (tableName)
			{
				case "scan_results":
					sql = "CREATE TABLE [scan_results] ("
                        + "[result_id] INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                        + "[scan_id] INTEGER NOT NULL,"
                        + "[instrument_id] VARCHAR(255) NOT NULL,"
                        + "[random_id] INTEGER  NOT NULL,"
                        + "[scan_date] VARCHAR(255) NOT NULL,"
                        + "[C_1A] VARCHAR(255)  NULL,"
                        + "[C_1B] VARCHAR(255)  NULL,"
                        + "[C_2A] VARCHAR(255)  NULL,"
                        + "[C_2B] VARCHAR(255)  NULL,"
                        + "[C_3A] VARCHAR(255)  NULL,"
                        + "[C_3B] VARCHAR(255)  NULL,"
                        + "[C_4A] VARCHAR(255)  NULL,"
                        + "[C_4B] VARCHAR(255)  NULL,"
                        + "[C_4C] VARCHAR(255)  NULL,"
                        + "[DS_1A] VARCHAR(255)  NULL,"
                        + "[DS_1B] VARCHAR(255)  NULL,"
                        + "[DS_2A] VARCHAR(255)  NULL,"
                        + "[DS_2B] VARCHAR(255)  NULL,"
                        + "[DS_3A] VARCHAR(255)  NULL,"
                        + "[DS_3B] VARCHAR(255)  NULL,"
                        + "[IS_1A] VARCHAR(255)  NULL,"
                        + "[IS_1B] VARCHAR(255)  NULL,"
                        + "[IS_2] VARCHAR(255)  NULL,"
                        + "[IS_3A] VARCHAR(255)  NULL,"
                        + "[IS_3B] VARCHAR(255)  NULL,"
                        + "[IS_3C] VARCHAR(255)  NULL,"
                        + "[MS1_1] VARCHAR(255)  NULL,"
                        + "[MS1_2A] VARCHAR(255)  NULL,"
                        + "[MS1_2B] VARCHAR(255)  NULL,"
                        + "[MS1_3A] VARCHAR(255)  NULL,"
                        + "[MS1_3B] VARCHAR(255)  NULL,"
                        + "[MS1_4A] VARCHAR(255)  NULL,"
                        + "[MS1_5A] VARCHAR(255)  NULL,"
                        + "[MS1_5B] VARCHAR(255)  NULL,"
                        + "[MS1_5C] VARCHAR(255)  NULL,"
                        + "[MS1_5D] VARCHAR(255)  NULL,"
                        + "[MS2_1] VARCHAR(255)  NULL,"
                        + "[MS2_2] VARCHAR(255)  NULL,"
                        + "[MS2_3] VARCHAR(255)  NULL,"
                        + "[MS2_4A] VARCHAR(255)  NULL,"
                        + "[MS2_4B] VARCHAR(255)  NULL,"
                        + "[MS2_4C] VARCHAR(255)  NULL,"
                        + "[MS2_4D] VARCHAR(255)  NULL,"
                        + "[P_1A] VARCHAR(25)  NULL,"
                        + "[P_1B] VARCHAR(25)  NULL,"
                        + "[P_2A] VARCHAR(25)  NULL,"
                        + "[P_2B] VARCHAR(25)  NULL,"
                        + "[P_2C] VARCHAR(25)  NULL,"
                        + "[P_3] VARCHAR(25)  NULL,"
						+ "[Phos_2A] VARCHAR(25)  NULL,"
						+ "[Phos_2C] VARCHAR(25)  NULL"
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
                        + "[PeakSignalToNoiseRatio] FLOAT NOT NULL," //CHANGED FROM FLOAT TO VARCHAR() IN MYSQL
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
                        + "[PeakKSStat] FLOAT NOT NULL," //CHANGED FROM FLOAT TO VARCHAR() IN MYSQL
                        + "[StatMomentsDataCountUsed] INTEGER NOT NULL"
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
                        + "[Peptide_Sequence] varchar(124) NOT NULL,"
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
                        + "[Protein_Name] varchar(45) NOT NULL,"
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
						+ "[CollisionMode] varchar(64) NULL,"
						+ "[Charge] INTEGER NOT NULL,"
						+ "[Peptide_MH] FLOAT NOT NULL,"
						+ "[Peptide_Sequence] varchar(124) NOT NULL,"
						+ "[DelM_Da] FLOAT NULL,"
						+ "[DelM_PPM] FLOAT NULL,"
						+ "[MSGFSpecProb] FLOAT NULL, "
						+ "[Unique_Seq_ID] INTEGER NOT NULL,"
						+ "[Cleavage_State] INTEGER NOT NULL,"
						+ "[Phosphopeptide] INTEGER NOT NULL"
						+ ")";
					break;

			}

			return sql;

		}

		public static bool TableExists(SQLiteConnection conn, string tableName)
		{
			using (SQLiteCommand cmd = conn.CreateCommand())
			{
				cmd.CommandText = "SELECT COUNT(*) AS Tables FROM sqlite_master where type = 'table' and name = '" + tableName + "'";
				
				using (SQLiteDataReader reader = cmd.ExecuteReader())
				{
					if (reader.Read() && reader.GetInt32(0) > 0)
					{
						return true;
					}
				}
		
			}

			return false;
			
		}

		protected void RunSql(SQLiteCommand cmd, string sql)
		{
			cmd.CommandText = sql;
			cmd.ExecuteNonQuery();
		}
    }
}
