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
        String applicationlog_filename = "";
        List<String> applicationlog_records = new List<String>();

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
            applicationlog_filename = "SMAQC-log_" + DateTime.Now.ToString("M-dd-yyyy-H-m-ss") + ".txt";

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
        public void addApplicationLog(String message)
        {
            //ADD TO RECORD LOG
            applicationlog_records.Add(DateTime.Now.ToString("M/dd/yyyy hh:mm:ss tt") + " - " + message);

            //SET OBSERVER SUBJECT TO OUR MESSAGE
            s.SubjectState = DateTime.Now.ToString("M/dd/yyyy hh:mm:ss tt") + " - " + message;

            //NOTIFY OBSERVER OF CHANGE
            s.Notify();
        }

        //SAVE LOG FILE
        public void saveApplicationLogFile()
        {
            //CREATE FILE
            StreamWriter file = new StreamWriter(applicationlog_filename);

            //ENABLE AUTOFLUSH [PNNL REQUIREMENT]
            file.AutoFlush = true;

            for (int i = 0; i < applicationlog_records.Count; i++)
            {
                file.Write(applicationlog_records[i] + "\r\n");
            }

            //CLOSE FILE
            file.Close();
        }
    }
}
