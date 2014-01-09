using System;
using System.Collections.Generic;
using System.IO;

namespace SMAQC
{
    class Aggregate
    {
		private const string SCAN_STATS_FILENAME_SUFFIX = "_ScanStats.txt";

        //DECLARE VARIABLES
        private readonly string m_DataFolder;									// DIRECTORY WE NEED TO SEARCH

	    readonly Dictionary<string, bool> MasicImportFiles;						// LIST OF THE FILES TO IMPORT
	    readonly Dictionary<string, bool> XTandemImportFiles;                   // LIST OF THE FILES TO IMPORT

	    readonly List<string> ValidDataSets = new List<string>();				// LIST OF VALID DATASETS
        private string m_CurrentDataset = "";									//CURRENT RUNNING DATASET


        //CONSTRUCTOR
        public Aggregate(string dirtosearch)
        {
            //SET FILE DIR
            m_DataFolder = dirtosearch;
        
            // SET VALID IMPORT FILES
            MasicImportFiles = new Dictionary<string, bool>(StringComparer.CurrentCultureIgnoreCase);
			XTandemImportFiles = new Dictionary<string, bool>(StringComparer.CurrentCultureIgnoreCase);

			// Masic files (ScanStats and SicStats are required)
            MasicImportFiles.Add("ScanStats", true);
            MasicImportFiles.Add("ScanStatsEx", false);
            MasicImportFiles.Add("SICstats", true);

			// X!Tandem files (only use this if not using PHRP Reader)
            XTandemImportFiles.Add("xt", true);
			XTandemImportFiles.Add("xt_ResultToSeqMap", true);
			XTandemImportFiles.Add("xt_SeqToProteinMap", true);

            //ENSURE temp.txt does not exist ... IF PROGRAM CLOSED FILE NOT REMOVED AND ON RESTART CRASHES
            checkTempFileNotExist();

        }

        //DESTRUCTOR
        ~Aggregate()
        {
            MasicImportFiles.Clear();
			XTandemImportFiles.Clear();
            ValidDataSets.Clear();
        }

        //THIS ENSURES THAT OUR TEMP FILE HAS BEEN DELETED IN SOME CASES IF IT IS NOT DUE TO PROGRAM CRASHING WE CAN HAVE PROBLEMS
        public void checkTempFileNotExist()
        {
	        const string file = "temp.txt";
	        if (File.Exists(file))
            {
                File.Delete(file);

            }
        }

	    //THIS FUNCTION DETECTS THE NUMBER OF DATASETS THAT WE MUST CHECK [USEFUL IF FOLDER WE ARE SEARCHING HAS 2+ DIFFERENT DATA SETS IN THEM]
		// Performs check by looking for files ending in _ScanStats.txt
        public List<string> DetectDatasets()
        {
            //DECLARE VARIABLES
            FileInfo[] filePaths = null;                      //SET TO NULL AS IN TRY BLOCK OR WILL NOT COMPILE

			ValidDataSets.Clear();

            try
            {
                //GET LIST OF FILES IN SPECIFIED DIRECTORY MATCHING FILE_EXT
				var fidir = new DirectoryInfo(m_DataFolder);
				filePaths = fidir.GetFiles("*" + SCAN_STATS_FILENAME_SUFFIX);
            }
            catch (DirectoryNotFoundException)
            {
                Console.WriteLine("GFIL():: Could not find directory {0}!", m_DataFolder);
				System.Threading.Thread.Sleep(2000);
                Environment.Exit(1);
            }

            //LOOP THROUGH ALL FILES IN SPECIFIED DIRECTORY
            foreach (FileInfo fileName in filePaths)
            {
				string dataSetName = fileName.Name.Substring(0, fileName.Name.Length - SCAN_STATS_FILENAME_SUFFIX.Length);
	            ValidDataSets.Add(dataSetName);
            }

            return ValidDataSets;
        }

        //THIS FUNCTION RETURNS THE # OF DATA SETS WE MUST SEARCH THROUGH
        public int numberOfDataSets()
        {
            return ValidDataSets.Count;
        }

		public List<string> getMasicFileImportList(string dataset, string file_ext)
		{
			return getFileImportList(dataset, file_ext, MasicImportFiles);
		}

		public List<string> getXTandemFileImportList(string dataset, string file_ext)
		{
			return getFileImportList(dataset, file_ext, XTandemImportFiles);
		}

        //GET A LIST OF ALL FILES MATCHING FILE EXT IN OUR FILE DIRECTORY
        protected List<string> getFileImportList(string dataset, string file_ext, Dictionary<string, bool> importFiles)
        {
            //DECLARE VARIABLES
            var FileArray = new List<string>();
            string[] filePaths = null;                      //SET TO NULL AS IN TRY BLOCK OR WILL NOT COMPILE

            try
            {
                //GET LIST OF FILES IN SPECIFIED DIRECTORY MATCHING FILE_EXT
                filePaths = Directory.GetFiles(m_DataFolder, file_ext);
            }
            catch (DirectoryNotFoundException)
            {
				Console.WriteLine("getFileImportList():: Could not find directory {0}!", m_DataFolder);
				System.Threading.Thread.Sleep(2000);
                Environment.Exit(1);				
            }

            //LOOP THROUGH ALL FILES IN SPECIFIED DIRECTORY
            for (int i = 0; i < filePaths.Length; i++)
            {
                //IF THE FILE IS IN OUR LIST
				if (is_valid_import_file(filePaths[i], importFiles))
                {
                    //ENSURE FILE IS A VALID DATASET FILE
                    if (is_valid_dataset_file(dataset, filePaths[i]))
                    {
                        //SAVE TO FileArray
                        FileArray.Add(filePaths[i]);
                        //Console.WriteLine("File {0}:: {1} -- ({2})", i, filePaths[i], index);
                    }
                }
            }
            return FileArray;
        }

        //THIS FUNCTION VERIFIES IF A FILENAME IS IN OUR DATASET LIST
        public Boolean is_valid_dataset_file(string dataset, string filename)
        {

			//GET FILENAME WITHOUT EXTENSION
			filename = Path.GetFileNameWithoutExtension(filename);

            //NOW CHECK FOR DATASET NAME IN FILENAME
            //IF FOUND A VALID FILE IN A CERTAIN DATASET
			if (filename.ToLower().StartsWith(dataset.ToLower()))
            {
                return true;
            }
            return false;
        }

        //THIS FUNCTION VERIFIES IF A FILENAME IS IN OUR IMPORT LIST
        //KNOWN_DATASET IS FOR IF WE HAVE ALREADY SET A RUNNING DATASET
		public Boolean is_valid_import_file(string filename, Dictionary<string, bool> importFiles)
        {
			string fileType;

			//GET FILENAME WITHOUT EXTENSION
			filename = Path.GetFileNameWithoutExtension(filename);

			if (filename.ToLower().StartsWith(m_CurrentDataset.ToLower()))
			{
				//SET TO FILENAME type[xt, scanstats, etc]
				fileType = filename.Substring(m_CurrentDataset.Length + 1);
			}
			else
			{
				return false;
			}

			if (importFiles.ContainsKey(fileType))
				return true;

			return false;
        }

        //SET RUNNING DATASET
        public void setDataset(string mydataset)
        {
            m_CurrentDataset = mydataset;
        }

    }
}
