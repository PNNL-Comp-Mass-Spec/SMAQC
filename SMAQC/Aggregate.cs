using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace SMAQC
{
    class Aggregate
    {
        //DECLARE VARIABLES
        private string filedir;                                             //DIRECTORY WE NEED TO SEARCH
        List<String> ValidImportFiles;                                      //LIST OF THE ONLY VALID FILES WE SHOULD BE READING
        List<String> ValidDataSets = new List<String>();                    //LIST OF VALID DATASETS
        private string dataset = "";                                        //CURRENT RUNNING DATASET


        //CONSTRUCTOR
        public Aggregate(string dirtosearch)
        {
            //SET FILE DIR
            filedir = dirtosearch;
        
            //SET VALID IMPORT FILES
            ValidImportFiles = new List<string>();
            ValidImportFiles.Add("ScanStats");
            ValidImportFiles.Add("ScanStatsEx");
            ValidImportFiles.Add("SICstats");
            ValidImportFiles.Add("xt");
            ValidImportFiles.Add("xt_ResultToSeqMap");
            ValidImportFiles.Add("xt_SeqToProteinMap");

            //ENSURE temp.txt does not exist ... IF PROGRAM CLOSED FILE NOT REMOVED AND ON RESTART CRASHES
            checkTempFileNotExist();

        }

        //DESTRUCTOR
        ~Aggregate()
        {
            ValidImportFiles.Clear();
            ValidDataSets.Clear();
        }

        //THIS ENSURES THAT OUR TEMP FILE HAS BEEN DELETED IN SOME CASES IF IT IS NOT DUE TO PROGRAM CRASHING WE CAN HAVE PROBLEMS
        public void checkTempFileNotExist()
        {
            String file = "temp.txt";
            if (File.Exists(file))
            {
                File.Delete(file);

            }
        }

        //THIS FUNCTION DETECTS THE NUMBER OF DATASETS THAT WE MUST CHECK [USEFUL IF FOLDER WE ARE SEARCHING HAS 2+ DIFFERENT DATA SETS IN THEM]
        public List<String> DetectDatasets(string file_ext)
        {
            //DECLARE VARIABLES
            string[] filePaths = null;                      //SET TO NULL AS IN TRY BLOCK OR WILL NOT COMPILE

            try
            {
                //GET LIST OF FILES IN SPECIFIED DIRECTORY MATCHING FILE_EXT
                filePaths = Directory.GetFiles(filedir, file_ext);
            }
            catch (DirectoryNotFoundException ex)
            {
                Console.WriteLine("GFIL():: Could not find directory {0}!", filedir);
                Environment.Exit(1);
            }

            //LOOP THROUGH ALL FILES IN SPECIFIED DIRECTORY
            for (int i = 0; i < filePaths.Length; i++)
            {
                //IF THE FILE IS IN OUR LIST
                if (is_valid_import_file(filePaths[i]))
                {
                    //STRIP FULL PATHNAME FROM FILENAME AND STORE AS filename
                    int pathprefix = filePaths[i].LastIndexOf(@"\");
                    string filename = filePaths[i].Substring(pathprefix + 1);

                    //FIND THE DATASET BY CHECKING FOR _ BEFORE ScanStats.txt [ScanStats.txt will be used to detect each dataset]
                    int dataprefix = filename.LastIndexOf("_ScanStats.txt");

                    //IF FOUND A UNIQUE DATA SET [USING ScanStats.txt TO FIND EACH ONE SINCE IT IS SIMPLE]
                    if (dataprefix > 0)
                    {
                        //GET DATA NAME
                        string dataname = filename.Substring(0, dataprefix);

                        ValidDataSets.Add(dataname);
                        //Console.WriteLine("File {0}:: {1}", i, dataname);
                    }

                }
            }

            return ValidDataSets;
        }

        //THIS FUNCTION RETURNS THE # OF DATA SETS WE MUST SEARCH THROUGH
        public int numberOfDataSets()
        {
            return ValidDataSets.Count;
        }

        //GET A LIST OF ALL FILES MATCHING FILE EXT IN OUR FILE DIRECTORY
        public List<String> getFileImportList(string dataset, string file_ext)
        {
            //DECLARE VARIABLES
            List<String> FileArray = new List<String>();
            int index = 0;
            string[] filePaths = null;                      //SET TO NULL AS IN TRY BLOCK OR WILL NOT COMPILE

            try
            {
                //GET LIST OF FILES IN SPECIFIED DIRECTORY MATCHING FILE_EXT
                filePaths = Directory.GetFiles(filedir, file_ext);
            }
            catch (DirectoryNotFoundException ex)
            {
                Console.WriteLine("GFIL():: Could not find directory {0}!", filedir);
                Environment.Exit(1);
            }

            //LOOP THROUGH ALL FILES IN SPECIFIED DIRECTORY
            for (int i = 0; i < filePaths.Length; i++)
            {
                //IF THE FILE IS IN OUR LIST
                if (is_valid_import_file(filePaths[i], 1))
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
            //DECLALRE VARIABLES
            int index = 0;                              //SET DEFAULT VALUE

            //Console.WriteLine("DS={0}", dataset);

            //STRIP FULL PATHNAME FROM FILENAME AND STORE AS filename
            index = filename.LastIndexOf(@"\");
            filename = filename.Substring(index + 1);

            //REMOVE ENDING .TXT AS IT MATCHES XT
            index = filename.LastIndexOf(".txt");       //FIND LAST INDEX OF .TXT
            filename = filename.Substring(0, index);    //REMOVE .TXT AND REPLACE FILENAME WITH NEW NAME

            //NOW CHECK FOR DATASET NAME FROM FILENAME [IF < 0 ... NOT EXIST IN FILENAME ... WRONG DATA SET]
            index = filename.LastIndexOf(dataset);                              //FIND INDEX OF DATASET [>=0 == TRUE ... <0 == FALSE]

            //IF FOUND A VALID FILE IN A CERTAIN DATASET
            if (index >= 0)
            {
                //REPLACE THE FILENAME [FOR PRINTING REASONS]
                //filename = filename.Substring(dataset.Length + 1);                  //RETURNS ScanStats, ScanStatsEx, ...
                //Console.WriteLine("FN={0}", filename);

                return true;
            }
            return false;
        }

        //THIS FUNCTION VERIFIES IF A FILENAME IS IN OUR IMPORT LIST
        //KNOWN_DATASET IS FOR IF WE HAVE ALREADY SET A RUNNING DATASET
        public Boolean is_valid_import_file(string filename, int known_dataset=0)
        {
            //DECLALRE VARIABLES
            int index = 0;                              //SET DEFAULT VALUE

            //STRIP FULL PATHNAME FROM FILENAME AND STORE AS filename
            index = filename.LastIndexOf(@"\");
            filename = filename.Substring(index + 1);

            //REMOVE ENDING .TXT AS IT MATCHES XT
            index = filename.LastIndexOf(".txt");       //FIND LAST INDEX OF .TXT
            filename = filename.Substring(0, index);    //REMOVE .TXT AND REPLACE FILENAME WITH NEW NAME

            //IF WE ARE PROCESSING A DATASET
            if (known_dataset == 1)
            {
                index = filename.LastIndexOf(dataset);

                //IF INVALID!
                if (index < 0)
                {
                    return false;
                }

                //SET TO FILENAME [xt, scanstats, etc]
                filename = filename.Substring(dataset.Length + 1);
            }

            //LOOP THROUGH VALID IMPORT LIST SEARCHING FOR INDEX ... RETURN TRUE IF INDEX > 0
            for (int i = 0; i < ValidImportFiles.Count; i++)
            {
                //FOR CHECKING FOR VALID MATCH
                index = filename.IndexOf(ValidImportFiles[i]);

                //IF WE ARE PROCESSING A DATASET
                if (known_dataset == 1)
                {
                    //CHECK AGAINST NAME ... NOT INDEX VALUE
                    if (ValidImportFiles[i].Equals(filename, StringComparison.OrdinalIgnoreCase))
                    {
                        return true;
                    }
                }
                else if (index != -1)
                {
                    return true;
                }
            }
            return false;
        }

        //SET RUNNING DATASET
        public void setDataset(string mydataset)
        {
            dataset = mydataset;
        }

    }
}
