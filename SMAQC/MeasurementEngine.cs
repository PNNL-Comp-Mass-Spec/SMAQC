using System;
using System.Collections.Generic;

namespace SMAQC
{
    class MeasurementEngine
    {
        //DECLARE VARIABLES
	    readonly List<string> m_MeasurementsToRun;
	    readonly MeasurementFactory factory;
	    readonly SystemLogManager m_SystemLogManager;

		// Properties
		public bool UsingPHRP { get; set; }

        //CONSTRUCTOR
		public MeasurementEngine(List<string> lstMeasurementsToRun, ref Measurement measurement, ref SystemLogManager systemLogManager)
        {
            //SET VARIABLES
			factory = new MeasurementFactory(ref measurement);
			m_MeasurementsToRun = lstMeasurementsToRun;            
			m_SystemLogManager = systemLogManager;

			UsingPHRP = false;
        }

        //RUN MEASUREMENTS
		public Dictionary<string, string> run()
        {
			var dctResults = new Dictionary<string, string>();                                         //MEASUREMENT RESULTS TABLE
			int iMeasurementsStarted = 0;

			factory.m_Measurement.UsingPHRP = UsingPHRP;

            foreach (string measurementName in m_MeasurementsToRun)
            {
                //Console.WriteLine("MeasurementEngine ELEMENT={0}", element);
				DateTime dtStartTime = DateTime.UtcNow;
				iMeasurementsStarted += 1;
				double percentComplete = iMeasurementsStarted / Convert.ToDouble(m_MeasurementsToRun.Count) * 100.0;

				try
                {
					string sResult = factory.buildMeasurement(measurementName);
					if (String.IsNullOrEmpty(sResult))
						sResult = "Null";

                    dctResults.Add(measurementName, sResult);
					m_SystemLogManager.addApplicationLog((measurementName + ":").PadRight(7) + " complete in " + DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds.ToString("0.00") + " seconds; " + percentComplete.ToString("0") + "% complete");
                }
                catch (Exception ex)
                {
                    //THIS HAPPENS WHEN A MEASUREMENT FAILS ... STORE AS NULL!
                    dctResults.Add(measurementName, "Null");
					Console.WriteLine();
					m_SystemLogManager.addApplicationLog(measurementName + " failed: " + ex.Message);
                }
            }

            //CLEAR MEASUREMENTS
            factory.cleanMeasurements();

            return dctResults;
        }

    }
}
