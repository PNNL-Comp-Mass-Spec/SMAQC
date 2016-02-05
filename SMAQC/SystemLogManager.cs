using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;

namespace SMAQC
{
	class SystemLogManager
	{
	    readonly ConcreteSubject s = new ConcreteSubject();
		string applicationlog_filename = "";
	    readonly List<string> applicationlog_records = new List<string>();

		StreamWriter mApplicationLogFile;

		// Constructor
		public SystemLogManager()
		{
			// Attach observer
			s.Attach(new ConcreteObserver(s));
		}

		// Create our application log filename
		public void createApplicationLog()
		{
			applicationlog_filename = "SMAQC-log_" + DateTime.Now.ToString("yyyy-MM-dd") + ".txt";

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
			addApplicationLog("[Version Info]");
			addApplicationLog("Loading Assemblies");

			// Fetch assemblies + log to file
			var MyDomain = AppDomain.CurrentDomain;
			var AssembliesLoaded = MyDomain.GetAssemblies();
			foreach (var MyAssembly in AssembliesLoaded)
			{
				addApplicationLog(MyAssembly.FullName);
			}

			// Add system information
			addApplicationLog("[System Information]");
			addApplicationLog("OS Version: " + Environment.OSVersion);
			addApplicationLog("Processor Count: " + Environment.ProcessorCount);

			if (Environment.Is64BitOperatingSystem)
				addApplicationLog("Operating System Type: 64-Bit OS");
			else
				addApplicationLog("Operating System Type: 32-Bit OS");

			addApplicationLog("Page Size: " + Environment.SystemPageSize);

			// Start with main system log
			addApplicationLog("[LogStart]");
			addApplicationLog("-----------------------------------------------------");
		}

		// Add application logs to our list<>
		public void addApplicationLog(string message)
		{
			// Append to the log file
			// Add to record log
			applicationlog_records.Add(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt") + " - " + message);
			
			if (mApplicationLogFile != null)
				mApplicationLogFile.WriteLine(applicationlog_records.Last());

			// Set observer subject to our message
			s.SubjectState = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt") + " - " + message;

			// Notify observer of change
			s.Notify();
		}

		// Close the log file
		public void CloseLogFile()
		{
			if (mApplicationLogFile != null)
				mApplicationLogFile.Close();

		}
	}
}
