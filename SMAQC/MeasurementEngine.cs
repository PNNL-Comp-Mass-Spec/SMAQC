using System;
using System.Collections.Generic;

namespace SMAQC
{
    class MeasurementEngine
    {

        readonly List<string> mMeasurementsToRun;
        readonly MeasurementFactory factory;
        readonly SystemLogManager mSystemLogManager;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="lstMeasurementsToRun"></param>
        /// <param name="measurement"></param>
        /// <param name="systemLogManager"></param>
        public MeasurementEngine(List<string> lstMeasurementsToRun, Measurement measurement, SystemLogManager systemLogManager)
        {
            factory = new MeasurementFactory(measurement);
            mMeasurementsToRun = lstMeasurementsToRun;
            mSystemLogManager = systemLogManager;

        }

        /// <summary>
        /// Compute the stats
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, string> RunMeasurements()
        {

            factory.ResetMeasurements();

            // Results dictionary
            var dctResults = new Dictionary<string, string>();
            var iMeasurementsStarted = 0;

            foreach (var measurementName in mMeasurementsToRun)
            {
                var dtStartTime = DateTime.UtcNow;
                iMeasurementsStarted += 1;
                var percentComplete = iMeasurementsStarted / (double)mMeasurementsToRun.Count * 100;

                try
                {
                    var sResult = factory.BuildMeasurement(measurementName);
                    if (string.IsNullOrEmpty(sResult))
                        sResult = "Null";

                    dctResults.Add(measurementName, sResult);
                    mSystemLogManager.AddApplicationLog((measurementName + ":").PadRight(15) + " complete in " + DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds.ToString("0.00") + " seconds; " + percentComplete.ToString("0") + "% complete");
                }
                catch (Exception ex)
                {
                    // Measurement failed; store Null
                    dctResults.Add(measurementName, "Null");
                    Console.WriteLine();
                    mSystemLogManager.AddApplicationLog(measurementName + " failed: " + ex.Message);
                }
            }

            return dctResults;
        }

    }
}
