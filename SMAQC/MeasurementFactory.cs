
namespace SMAQC
{
    class MeasurementFactory
    {
        
        public Measurement m_Measurement;

        // Constructor
        public MeasurementFactory(ref Measurement m_Measurement)
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

        // Clear saved data
        public void CleanMeasurements()
        {
            m_Measurement.ClearStorage();
        }
    }
}
