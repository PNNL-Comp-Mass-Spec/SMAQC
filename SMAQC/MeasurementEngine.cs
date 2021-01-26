using System;
using System.Collections.Generic;

namespace SMAQC
{
    class MeasurementEngine
    {

        readonly List<string> mMeasurementsToRun;
        readonly MeasurementFactory mFactory;
        readonly SystemLogManager mSystemLogManager;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="measurementsToRun"></param>
        /// <param name="measurement"></param>
        /// <param name="systemLogManager"></param>
        public MeasurementEngine(List<string> measurementsToRun, Measurement measurement, SystemLogManager systemLogManager)
        {
            mFactory = new MeasurementFactory(measurement);
            mMeasurementsToRun = measurementsToRun;
            mSystemLogManager = systemLogManager;

        }

        /// <summary>
        /// Compute the stats
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, string> RunMeasurements()
        {

            mFactory.ResetMeasurements();

            // Results dictionary
            var dctResults = new Dictionary<string, string>();
            var measurementsStarted = 0;

            foreach (var measurementName in mMeasurementsToRun)
            {
                var startTime = DateTime.UtcNow;
                measurementsStarted += 1;
                var percentComplete = measurementsStarted / (double)mMeasurementsToRun.Count * 100;

                try
                {
                    var result = mFactory.BuildMeasurement(measurementName);
                    if (string.IsNullOrEmpty(result))
                        result = "Null";

                    dctResults.Add(measurementName, result);

                    mSystemLogManager.AddApplicationLog(string.Format(
                        "{0,-22} complete in {1:F2} seconds; {2:F0}% complete",
                        measurementName + ":",
                        DateTime.UtcNow.Subtract(startTime).TotalSeconds,
                        percentComplete));
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
