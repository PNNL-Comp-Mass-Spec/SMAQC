
using System;

namespace SMAQC
{
    internal class MeasurementFactory
    {
        public Measurement mMeasurement;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="measurement"></param>
        public MeasurementFactory(Measurement measurement)
        {
            this.mMeasurement = measurement;
        }

        public string BuildMeasurement(string measurementName)
        {
            // Convert measurement name to function using reflection
            var methodName = measurementName;
            var info = mMeasurement.GetType().GetMethod(methodName);
            if (info != null)
            {
                var result = (string)info.Invoke(mMeasurement, null);
                return result;
            }

            throw new ArgumentOutOfRangeException(nameof(measurementName), "Measurement name not recognized: " + measurementName);
        }

        /// <summary>
        /// Clear cached data
        /// </summary>
        public void ResetMeasurements()
        {
            mMeasurement.Reset();
        }
    }
}
