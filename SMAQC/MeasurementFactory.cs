
namespace SMAQC
{
    class MeasurementFactory
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
            var result = (string)info.Invoke(mMeasurement, null);

            return result;
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
