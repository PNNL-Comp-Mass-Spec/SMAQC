using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;

namespace SMAQC
{
    class MeasurementEngine
    {
        //DECLARE VARIABLES
        List<string> m_MeasurementsToRun;
        MeasurementFactory factory;
		SystemLogManager m_SystemLogManager;

		// Properties
		public bool UsingPHRP { get; set; }

        //CONSTRUCTOR
		public MeasurementEngine(List<string> lstMeasurementsToRun, ref Measurement measurement, ref SystemLogManager systemLogManager)
        {
            //SET VARIABLES
			factory = new MeasurementFactory(ref measurement);
			this.m_MeasurementsToRun = lstMeasurementsToRun;            
			this.m_SystemLogManager = systemLogManager;

			this.UsingPHRP = false;
        }

        //DESTRUCTOR
        ~MeasurementEngine()
        {
        }

        //RUN MEASUREMENTS
		public Dictionary<string, string> run()
        {
			Dictionary<string, string> dctResults = new Dictionary<string, string>();                                         //MEASUREMENT RESULTS TABLE
			System.DateTime dtStartTime;
			double percentComplete;
			int iMeasurementsStarted = 0;
			string sResult;

			factory.m_Measurement.UsingPHRP = this.UsingPHRP;

            foreach (string measurementName in m_MeasurementsToRun)
            {
                //Console.WriteLine("MeasurementEngine ELEMENT={0}", element);
				dtStartTime = System.DateTime.UtcNow;
				iMeasurementsStarted += 1;
				percentComplete = iMeasurementsStarted / Convert.ToDouble(m_MeasurementsToRun.Count) * 100.0;

				try
                {
					sResult = factory.buildMeasurement(measurementName);
					if (String.IsNullOrEmpty(sResult))
						sResult = "Null";

                    dctResults.Add(measurementName, sResult);
					m_SystemLogManager.addApplicationLog((measurementName + ":").PadRight(7) + " complete in " + System.DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds.ToString("0.00") + " seconds; " + percentComplete.ToString("0") + "% complete");
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
