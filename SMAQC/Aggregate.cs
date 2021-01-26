using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace SMAQC
{
    internal class Aggregate
    {
        private const string SCAN_STATS_FILENAME_SUFFIX = "_ScanStats.txt";

        /// <summary>
        /// Directory we need to search
        /// </summary>
        private readonly string mInputDirectoryPath;

        /// <summary>
        /// List of MASIC files to import
        /// Keys are file names, values are true if required or false if optional
        /// </summary>
        private readonly Dictionary<string, bool> MasicImportFiles;

        /// <summary>
        /// List of valid datasets
        /// </summary>
        private readonly List<string> ValidDataSets = new List<string>();

        /// <summary>
        /// Current running dataset
        /// </summary>
        private string m_CurrentDataset = string.Empty;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="directoryToSearch"></param>
        public Aggregate(string directoryToSearch)
        {
            mInputDirectoryPath = directoryToSearch;

            // Set valid import files
            // Masic files (ScanStats and SICStats are required, ScanStatsEx and ReporterIons are optional)

            MasicImportFiles = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase)
            {
                { "ScanStats", true },
                { "ScanStatsEx", false },
                { "SICStats", true },
                { "ReporterIons", false }
            };

            // Ensure temp.txt does not exist ... if program closed file not removed and on restart crashes
            CheckTempFileNotExist();
        }

        // Destructor
        ~Aggregate()
        {
            MasicImportFiles.Clear();
            ValidDataSets.Clear();
        }

        // This ensures that our temp file has been deleted in some cases if it is not due to program crashing we can have problems
        private void CheckTempFileNotExist()
        {
            const string file = "temp.txt";
            if (File.Exists(file))
            {
                File.Delete(file);
            }
        }

        // This function detects the number of datasets that we must check [useful if directory we are searching has multiple datasets]
        // Performs check by looking for files ending in _scanstats.txt
        public List<string> DetectDatasets()
        {
            FileInfo[] filePaths = null;                      // Set to null as in try block or will not compile

            ValidDataSets.Clear();

            try
            {
                // Get list of files in specified directory matching file_ext
                var inputDirectory = new DirectoryInfo(mInputDirectoryPath);
                filePaths = inputDirectory.GetFiles("*" + SCAN_STATS_FILENAME_SUFFIX);
            }
            catch (DirectoryNotFoundException)
            {
                Console.WriteLine("Input directory not found: {0}!", mInputDirectoryPath);
                Thread.Sleep(1500);
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

        /// <summary>
        /// Find the MASIC related files for dataset m_CurrentDataset in mInputDirectoryPath
        /// </summary>
        /// <param name="fileExtension">File extension to match, typically *.txt</param>
        /// <returns>Dictionary where Keys are file paths and Values are lists of header column suffixes to ignore</returns>
        public Dictionary<string, List<string>> GetMasicFileImportList(string fileExtension)
        {
            return GetFileImportList(fileExtension, MasicImportFiles);
        }

        /// <summary>
        /// Get a list of all files matching fileExtension for dataset m_CurrentDataset in mInputDirectoryPath
        /// </summary>
        /// <param name="fileExtension">File extension to match, typically *.txt</param>
        /// <param name="importFiles">Files to find; keys are filename suffixes, values are True if required or false if optional</param>
        /// <returns>Dictionary where Keys are file paths and Values are lists of header column suffixes to ignore</returns>
        private Dictionary<string, List<string>> GetFileImportList(string fileExtension, IReadOnlyDictionary<string, bool> importFiles)
        {
            // Keys are file paths; values are lists of header column suffixes to ignore
            var fileImportList = new Dictionary<string, List<string>>();
            string[] filePaths = null;                      // Set to null as in try block or will not compile

            try
            {
                // Get list of files in specified directory matching file_ext
                filePaths = Directory.GetFiles(mInputDirectoryPath, fileExtension);
            }
            catch (DirectoryNotFoundException)
            {
                Console.WriteLine("getFileImportList():: Could not find directory {0}!", mInputDirectoryPath);
                Thread.Sleep(1500);
                Environment.Exit(1);
            }

            // Loop through all files in specified directory
            foreach (var filePath in filePaths)
            {
                if (!IsKnownImportFile(filePath, importFiles))
                {
                    continue;
                }

                // Ensure file is a valid dataset file
                if (!IsAssociatedWithCurrentDataset(filePath))
                    continue;

                // Valid file
                var excludedColumnNameSuffixes = new List<string>();
                if (filePath.EndsWith("_ReporterIons.txt", StringComparison.OrdinalIgnoreCase))
                {
                    excludedColumnNameSuffixes.Add("_SignalToNoise");
                    excludedColumnNameSuffixes.Add("_Resolution");
                    excludedColumnNameSuffixes.Add("_OriginalIntensity");
                    excludedColumnNameSuffixes.Add("_ObsMZ");
                    excludedColumnNameSuffixes.Add("_LabelDataMZ");
                }

                fileImportList.Add(filePath, excludedColumnNameSuffixes);
                // Console.WriteLine("file {0}:: {1} -- ({2})", i, filePaths[i], index);
            }
            return fileImportList;
        }

        // This function verifies that the filename is associated with m_CurrentDataset
        private bool IsAssociatedWithCurrentDataset(string filename)
        {
            // Get filename without extension
            filename = Path.GetFileNameWithoutExtension(filename);

            // Now check for dataset name in filename
            // If found a valid file in a certain dataset
            return filename?.StartsWith(m_CurrentDataset, StringComparison.OrdinalIgnoreCase) == true;
        }

        // This function verifies if a filename is in our import list
        // Known_dataset is for if we have already set a running dataset
        private bool IsKnownImportFile(string filename, IReadOnlyDictionary<string, bool> importFiles)
        {
            // Get filename without extension
            filename = Path.GetFileNameWithoutExtension(filename);

            if (filename?.StartsWith(m_CurrentDataset, StringComparison.OrdinalIgnoreCase) != true)
                return false;

            if (filename.Length < m_CurrentDataset.Length + 1)
                return false;

            // Filename starts with the dataset name
            // Get the text after the dataset name and an underscore
            var fileType = filename.Substring(m_CurrentDataset.Length + 1);

            // fileType is now something like "_msgfplus_syn" or "ScanStats"
            return importFiles.ContainsKey(fileType);
        }

        // Set running dataset
        public void SetDatasetName(string datasetName)
        {
            m_CurrentDataset = datasetName;
        }
    }
}
