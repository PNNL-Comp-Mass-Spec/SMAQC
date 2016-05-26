using System;
using System.Collections.Generic;
using System.IO;

namespace SMAQC
{
    class Aggregate
    {
		private const string SCAN_STATS_FILENAME_SUFFIX = "_ScanStats.txt";

        // Declare variables
        private readonly string m_DataFolder;									// Directory we need to search

	    readonly Dictionary<string, bool> MasicImportFiles;						// List of the files to import
	    readonly Dictionary<string, bool> XTandemImportFiles;                   // List of the files to import

	    readonly List<string> ValidDataSets = new List<string>();				// List of valid datasets
        private string m_CurrentDataset = "";									// Current running dataset


        // Constructor
        public Aggregate(string dirtosearch)
        {
            // Set file dir
            m_DataFolder = dirtosearch;
        
            // Set valid import files
            MasicImportFiles = new Dictionary<string, bool>(StringComparer.CurrentCultureIgnoreCase);
			XTandemImportFiles = new Dictionary<string, bool>(StringComparer.CurrentCultureIgnoreCase);

            // Masic files (scanstats and sicstats are required, ScanStatsEx and ReporterIons are optional)
            MasicImportFiles.Add("ScanStats", true);
            MasicImportFiles.Add("ScanStatsEx", false);
            MasicImportFiles.Add("SICstats", true);
            MasicImportFiles.Add("ReporterIons", false);

			// X!tandem files (only use this if not using PHRP Reader)
            XTandemImportFiles.Add("xt", true);
			XTandemImportFiles.Add("xt_ResultToSeqMap", true);
			XTandemImportFiles.Add("xt_SeqToProteinMap", true);

            // Ensure temp.txt does not exist ... if program closed file not removed and on restart crashes
            checkTempFileNotExist();

        }

        // Destructor
        ~Aggregate()
        {
            MasicImportFiles.Clear();
			XTandemImportFiles.Clear();
            ValidDataSets.Clear();
        }

        // This ensures that our temp file has been deleted in some cases if it is not due to program crashing we can have problems
        public void checkTempFileNotExist()
        {
	        const string file = "temp.txt";
	        if (File.Exists(file))
            {
                File.Delete(file);

            }
        }

	    // This function detects the number of datasets that we must check [useful if folder we are searching has 2+ different data sets in them]
		// Performs check by looking for files ending in _scanstats.txt
        public List<string> DetectDatasets()
        {
            // Declare variables
            FileInfo[] filePaths = null;                      // Set to null as in try block or will not compile

			ValidDataSets.Clear();

            try
            {
                // Get list of files in specified directory matching file_ext
				var fidir = new DirectoryInfo(m_DataFolder);
				filePaths = fidir.GetFiles("*" + SCAN_STATS_FILENAME_SUFFIX);
            }
            catch (DirectoryNotFoundException)
            {
                Console.WriteLine("GFIL():: Could not find directory {0}!", m_DataFolder);
				System.Threading.Thread.Sleep(2000);
                Environment.Exit(1);
            }

            // Loop through all files in specified directory
            foreach (var fileName in filePaths)
            {
				var dataSetName = fileName.Name.Substring(0, fileName.Name.Length - SCAN_STATS_FILENAME_SUFFIX.Length);
	            ValidDataSets.Add(dataSetName);
            }

            return ValidDataSets;
        }

        // This function returns the # of data sets we must search through
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

        // Get a list of all files matching file ext in our file directory
        protected List<string> getFileImportList(string dataset, string file_ext, Dictionary<string, bool> importFiles)
        {
            // Declare variables
            var FileArray = new List<string>();
            string[] filePaths = null;                      // Set to null as in try block or will not compile

            try
            {
                // Get list of files in specified directory matching file_ext
                filePaths = Directory.GetFiles(m_DataFolder, file_ext);
            }
            catch (DirectoryNotFoundException)
            {
				Console.WriteLine("getFileImportList():: Could not find directory {0}!", m_DataFolder);
				System.Threading.Thread.Sleep(2000);
                Environment.Exit(1);				
            }

            // Loop through all files in specified directory
            for (var i = 0; i < filePaths.Length; i++)
            {
                if (!is_valid_import_file(filePaths[i], importFiles))
                {
                    continue;
                }

                // Ensure file is a valid dataset file
                if (is_valid_dataset_file(dataset, filePaths[i]))
                {
                    // Save to filearray
                    FileArray.Add(filePaths[i]);
                    // Console.writeline("file {0}:: {1} -- ({2})", i, filepaths[i], index);
                }
            }
            return FileArray;
        }

        // This function verifies if a filename is in our dataset list
        public bool is_valid_dataset_file(string dataset, string filename)
        {

			// Get filename without extension
			filename = Path.GetFileNameWithoutExtension(filename);

            // Now check for dataset name in filename
            // If found a valid file in a certain dataset
			if (filename != null && filename.ToLower().StartsWith(dataset.ToLower()))
            {
                return true;
            }
            return false;
        }

        // This function verifies if a filename is in our import list
        // Known_dataset is for if we have already set a running dataset
		public bool is_valid_import_file(string filename, Dictionary<string, bool> importFiles)
        {
			string fileType;

			// Get filename without extension
			filename = Path.GetFileNameWithoutExtension(filename);

			if (filename != null && filename.ToLower().StartsWith(m_CurrentDataset.ToLower()))
			{
				// Set to filename type[xt, scanstats, etc]
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

        // Set running dataset
        public void setDataset(string mydataset)
        {
            m_CurrentDataset = mydataset;
        }

    }
}
