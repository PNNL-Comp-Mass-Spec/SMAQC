using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Remoting;

namespace SMAQC
{
    class MeasurementFactory
    {
        //DECLARE VARIABLES
        public Measurement m_Measurement;                                                        //CREATE MEASUREMENT OBJECT

        //CONSTRUCTOR
        public MeasurementFactory(ref Measurement m_Measurement)
        {
            this.m_Measurement = m_Measurement;
        }

        //DESTRUCTOR
        ~MeasurementFactory()
        {
        }

        public string buildMeasurement(string measurement)
        {
            //DECLARE VARIABLE
            string result;

            //CONVERT STRING TO FUNCTION USING REFLECTION
            string methodName = measurement;
            System.Reflection.MethodInfo info = m_Measurement.GetType().GetMethod(methodName);
            result = (string)info.Invoke(m_Measurement, null);

            return result;
        }

        //SOME MEASUREMENTS REQUIRE DATA TO BE SAVED. THIS FUNCTION IS CALLED AFTER THE END OF EACH DATASET RUN TO CLEAR THAT SAVED DATA
        public void cleanMeasurements()
        {
            //CALL CLEAR STORAGE TO CLEAR HASH TABLES
            m_Measurement.clearStorage();
        }
    }
}
