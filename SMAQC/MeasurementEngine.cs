using System;
using System.Collections.Generic;

namespace SMAQC
{
    class MeasurementEngine
    {
        
        readonly List<string> m_MeasurementsToRun;
        readonly MeasurementFactory factory;
        readonly SystemLogManager m_SystemLogManager;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="lstMeasurementsToRun"></param>
        /// <param name="measurement"></param>
        /// <param name="systemLogManager"></param>
        public MeasurementEngine(List<string> lstMeasurementsToRun, ref Measurement measurement, ref SystemLogManager systemLogManager)
        {
            factory = new MeasurementFactory(ref measurement);
            m_MeasurementsToRun = lstMeasurementsToRun;            
            m_SystemLogManager = systemLogManager;

        }

        /// <summary>
        /// Compute the stats
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, string> RunMeasurements()
        {
            // Results dictionary
            var dctResults = new Dictionary<string, string>();
            var iMeasurementsStarted = 0;

            foreach (var measurementName in m_MeasurementsToRun)
            {
                // Console.WriteLine("MeasurementEngine ELEMENT={0}", element);

                var dtStartTime = DateTime.UtcNow;
                iMeasurementsStarted += 1;
                var percentComplete = iMeasurementsStarted / Convert.ToDouble(m_MeasurementsToRun.Count) * 100.0;

                try
                {
                    var sResult = factory.BuildMeasurement(measurementName);
                    if (string.IsNullOrEmpty(sResult))
                        sResult = "Null";

                    dctResults.Add(measurementName, sResult);
                    m_SystemLogManager.AddApplicationLog((measurementName + ":").PadRight(7) + " complete in " + DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds.ToString("0.00") + " seconds; " + percentComplete.ToString("0") + "% complete");
                }
                catch (Exception ex)
                {
                    // Measurement failed; store Null
                    dctResults.Add(measurementName, "Null");
                    Console.WriteLine();
                    m_SystemLogManager.AddApplicationLog(measurementName + " failed: " + ex.Message);
                }
            }

            // Clear cached measurements
            factory.CleanMeasurements();

            return dctResults;
        }

    }
}
