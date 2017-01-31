using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;

namespace SMAQC
{
    class SystemLogManager
    {
        
        private readonly string applicationlog_filename;
        private readonly List<string> applicationlog_records = new List<string>();

        StreamWriter mApplicationLogFile;

        /// <summary>
        /// Constructor
        /// </summary>
        public SystemLogManager()
        {
            applicationlog_filename = "SMAQC-log_" + DateTime.Now.ToString("yyyy-MM-dd") + ".txt";
        }

        // Create our application log filename
        public void CreateApplicationLog()
        {
            
            try
            {
                // Create file
                mApplicationLogFile =
                    new StreamWriter(new FileStream(applicationlog_filename, FileMode.Append, FileAccess.Write, FileShare.Read))
                    {
                        AutoFlush = true
                    };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error creating log file: " + ex.Message);
            }

            // Add default text to record log
            AddApplicationLog("[Version Info]");
            AddApplicationLog("Loading Assemblies");

            // Fetch assemblies + log to file
            var MyDomain = AppDomain.CurrentDomain;
            var AssembliesLoaded = MyDomain.GetAssemblies();
            foreach (var MyAssembly in AssembliesLoaded)
            {
                AddApplicationLog(MyAssembly.FullName);
            }

            // Add system information
            AddApplicationLog("[System Information]");
            AddApplicationLog("OS Version: " + Environment.OSVersion);
            AddApplicationLog("Processor Count: " + Environment.ProcessorCount);

            if (Environment.Is64BitOperatingSystem)
                AddApplicationLog("Operating System Type: 64-Bit OS");
            else
                AddApplicationLog("Operating System Type: 32-Bit OS");

            AddApplicationLog("Page Size: " + Environment.SystemPageSize);

            // Start with main system log
            AddApplicationLog("[LogStart]");
            AddApplicationLog("-----------------------------------------------------");
        }

        // Add application logs to our list<>
        public void AddApplicationLog(string message)
        {
            // Append to the log file
            // Add to record log
            applicationlog_records.Add(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt") + " - " + message);

            mApplicationLogFile?.WriteLine(applicationlog_records.Last());


            Console.WriteLine("{0:yyyy-MM-dd hh:mm:ss tt} - {1}", DateTime.Now, message);
        }

        // Close the log file
        public void CloseLogFile()
        {
            mApplicationLogFile?.Close();
        }
    }
}
