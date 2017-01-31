
namespace SMAQC
{
    class MeasurementFactory
    {
        
        public Measurement m_Measurement;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="measurement"></param>
        public MeasurementFactory(Measurement measurement)
        {
            this.m_Measurement = m_Measurement;
        }

        public string BuildMeasurement(string measurement)
        {
            // Convert measurement name to function using reflection
            var methodName = measurement;
            var info = m_Measurement.GetType().GetMethod(methodName);
            var result = (string)info.Invoke(m_Measurement, null);

            return result;
        }

        /// <summary>
        /// Clear cached data
        /// </summary>
        public void ResetMeasurements()
        {
            m_Measurement.Reset();
        }
    }
}
