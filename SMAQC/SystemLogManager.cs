using System;
using System.IO;
using PRISM;

namespace SMAQC
{
    internal class SystemLogManager
    {
        // Ignore Spelling: yyyy-MM-dd, hh:mm:ss tt

        private readonly string mApplicationLogFileName;

        private StreamWriter mApplicationLogFile;

        /// <summary>
        /// Constructor
        /// </summary>
        public SystemLogManager()
        {
            mApplicationLogFileName = "SMAQC-log_" + DateTime.Now.ToString("yyyy-MM-dd") + ".txt";
        }

        // Create our application log filename
        public void CreateApplicationLog()
        {
            try
            {
                // Create file
                mApplicationLogFile =
                    new StreamWriter(new FileStream(mApplicationLogFileName, FileMode.Append, FileAccess.Write, FileShare.Read))
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

            Console.WriteLine();

            // Start with main system log
            AddApplicationLog("[LogStart]");
            AddApplicationLog("-----------------------------------------------------");
        }

        /// <summary>
        /// Add entry to the log records list and to the log file (if defined)
        /// </summary>
        /// <param name="message"></param>
        /// <param name="showMessage"></param>
        public void AddApplicationLog(string message, bool showMessage = true)
        {
            var messageWithTime = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt") + " - " + message;

            // Append to the log file
            mApplicationLogFile?.WriteLine(messageWithTime);

            if (showMessage)
                Console.WriteLine(message);
        }

        public void AddApplicationLogError(string message)
        {
            AddApplicationLog(message, false);
            ConsoleMsgUtils.ShowError(message);
        }

        public void AddApplicationLogWarning(string message)
        {
            AddApplicationLog(message, false);
            ConsoleMsgUtils.ShowWarning(message);
        }

        // Close the log file
        public void CloseLogFile()
        {
            mApplicationLogFile?.Close();
        }
    }
}
