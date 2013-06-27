using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.IO;

namespace SMAQC
{
	class SystemLogManager
	{
		ConcreteSubject s = new ConcreteSubject();
		string applicationlog_filename = "";
		List<string> applicationlog_records = new List<string>();

		StreamWriter mApplicationLogFile;

		//CONSTRUCTOR
		public SystemLogManager()
		{
			//ATTACH OBSERVER
			s.Attach(new ConcreteObserver(s, "OBSERVER 1"));
		}

		//DESTRUCTOR
		~SystemLogManager()
		{

		}

		//CREATE OUR APPLICATION LOG FILENAME
		public void createApplicationLog()
		{
			applicationlog_filename = "SMAQC-log_" + DateTime.Now.ToString("yyyy-MM-dd") + ".txt";

			try
			{
				//CREATE FILE
				mApplicationLogFile = new StreamWriter(new System.IO.FileStream(applicationlog_filename, FileMode.Append, FileAccess.Write, FileShare.Read));
				mApplicationLogFile.AutoFlush = true;
			}
			catch (Exception ex)
			{
				Console.WriteLine("Error creating log file: " + ex.Message);
			}

			//ADD DEFAULT TEXT TO RECORD LOG
			addApplicationLog("[Version Info]");
			addApplicationLog("Loading Assemblies");

			//FETCH ASSEMBLIES + LOG TO FILE
			AppDomain MyDomain = AppDomain.CurrentDomain;
			Assembly[] AssembliesLoaded = MyDomain.GetAssemblies();
			foreach (Assembly MyAssembly in AssembliesLoaded)
			{
				addApplicationLog(MyAssembly.FullName);
			}

			//ADD SYSTEM INFORMATION
			addApplicationLog("[System Information]");
			addApplicationLog("OS Version: " + Environment.OSVersion);
			addApplicationLog("Processor Count: " + Environment.ProcessorCount);

			if (Environment.Is64BitOperatingSystem)
				addApplicationLog("Operating System Type: 64-Bit OS");
			else
				addApplicationLog("Operating System Type: 32-Bit OS");

			addApplicationLog("Page Size: " + Environment.SystemPageSize);

			//START WITH MAIN SYSTEM LOG
			addApplicationLog("[LogStart]");
			addApplicationLog("-----------------------------------------------------");
		}

		//ADD APPLICATION LOGS TO OUR LIST<>
		public void addApplicationLog(string message)
		{
			//APPEND TO THE LOG FILE
			//ADD TO RECORD LOG
			applicationlog_records.Add(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt") + " - " + message);
			
			if (mApplicationLogFile != null)
				mApplicationLogFile.WriteLine(applicationlog_records.Last());

			//SET OBSERVER SUBJECT TO OUR MESSAGE
			s.SubjectState = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt") + " - " + message;

			//NOTIFY OBSERVER OF CHANGE
			s.Notify();
		}

		//CLOSE THE LOG FILE
		public void CloseLogFile()
		{
			if (mApplicationLogFile != null)
				mApplicationLogFile.Close();

		}
	}
}
