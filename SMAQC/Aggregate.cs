﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace SMAQC
{
    class Aggregate
    {
        private const string SCAN_STATS_FILENAME_SUFFIX = "_ScanStats.txt";

        /// <summary>
        /// Directory we need to search
        /// </summary>
        private readonly string m_DataFolder;

        /// <summary>
        /// List of MASIC files to import
        /// Keys are file names, values are true if required or false if optional
        /// </summary>
        readonly Dictionary<string, bool> MasicImportFiles;

        /// <summary>
        /// List of X!Tandem files to import
        /// Keys are file names, values are true if required or false if optional
        /// </summary>
        /// <remarks>This is only uses if not using PHRP Reader</remarks>
        readonly Dictionary<string, bool> XTandemImportFiles;

        /// <summary>
        /// List of valid datasets
        /// </summary>
        readonly List<string> ValidDataSets = new List<string>();

        /// <summary>
        /// Current running dataset
        /// </summary>
        private string m_CurrentDataset = "";


        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="folderToSearch"></param>
        public Aggregate(string folderToSearch)
        {
            // Set file dir
            m_DataFolder = folderToSearch;

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
        private void checkTempFileNotExist()
        {
            const string file = "temp.txt";
            if (File.Exists(file))
            {
                File.Delete(file);

            }
        }

        // This function detects the number of datasets that we must check [useful if folder we are searching has multiple datasets]
        // Performs check by looking for files ending in _scanstats.txt
        public List<string> DetectDatasets()
        {
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
                Console.WriteLine("Input folder not found: {0}!", m_DataFolder);
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
        /// Find the MASIC related files for dataset m_CurrentDataset in m_DataFolder
        /// </summary>
        /// <param name="file_ext">File extension to match, typically *.txt</param>
        /// <returns>Dictionary where Keys are file paths and Values are lists of header column suffixes to ignore</returns>
        public Dictionary<string, List<string>> GetMasicFileImportList(string file_ext)
        {
            return GetFileImportList(file_ext, MasicImportFiles);
        }

        /// <summary>
        /// Find the X!Tandem related files for dataset m_CurrentDataset in m_DataFolder
        /// </summary>
        /// <param name="file_ext">File extension to match, typically *.txt</param>
        /// <returns>Dictionary where Keys are file paths and Values are lists of header column suffixes to ignore</returns>
        [Obsolete("Unused")]
        private Dictionary<string, List<string>> GetXTandemFileImportList(string file_ext)
        {
            return GetFileImportList(file_ext, XTandemImportFiles);
        }

        /// <summary>
        /// Get a list of all files matching file_ext for dataset m_CurrentDataset in m_DataFolder
        /// </summary>
        /// <param name="file_ext">File extension to match, typically *.txt</param>
        /// <param name="importFiles">Files to find; keys are filename suffixes, values are True if required or false if optional</param>
        /// <returns>Dictionary where Keys are file paths and Values are lists of header column suffixes to ignore</returns>
        private Dictionary<string, List<string>> GetFileImportList(string file_ext, Dictionary<string, bool> importFiles)
        {
            // Keys are file paths; values are lists of header column suffixes to ignore
            var fileImportList = new Dictionary<string, List<string>>();
            string[] filePaths = null;                      // Set to null as in try block or will not compile

            try
            {
                // Get list of files in specified directory matching file_ext
                filePaths = Directory.GetFiles(m_DataFolder, file_ext);
            }
            catch (DirectoryNotFoundException)
            {
                Console.WriteLine("getFileImportList():: Could not find directory {0}!", m_DataFolder);
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
                var excludedFieldNameSuffixes = new List<string>();
                if (filePath.ToLower().EndsWith("_reporterions.txt"))
                {
                    excludedFieldNameSuffixes.Add("_SignalToNoise");
                    excludedFieldNameSuffixes.Add("_Resolution");
                    excludedFieldNameSuffixes.Add("_OriginalIntensity");
                    excludedFieldNameSuffixes.Add("_ObsMZ");
                    excludedFieldNameSuffixes.Add("_LabelDataMZ");
                }

                fileImportList.Add(filePath, excludedFieldNameSuffixes);
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
            if (filename != null && filename.ToLower().StartsWith(m_CurrentDataset.ToLower()))
            {
                return true;
            }
            return false;
        }

        // This function verifies if a filename is in our import list
        // Known_dataset is for if we have already set a running dataset
        private bool IsKnownImportFile(string filename, Dictionary<string, bool> importFiles)
        {
            // Get filename without extension
            filename = Path.GetFileNameWithoutExtension(filename);

            if (filename == null || !filename.ToLower().StartsWith(m_CurrentDataset.ToLower()))
                return false;

            if (filename.Length < m_CurrentDataset.Length + 1)
                return false;

            // Filename starts with the dataset name
            // Get the text after the dataset name and an underscore
            var fileType = filename.Substring(m_CurrentDataset.Length + 1);

            // fileType is now something like "_msgfplus_syn" or "scanstats"
            return importFiles.ContainsKey(fileType);
        }

        // Set running dataset
        public void SetDatasetName(string datasetName)
        {
            m_CurrentDataset = datasetName;
        }

    }
}
