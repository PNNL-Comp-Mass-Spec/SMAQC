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
        private List<Measurement> m_Measurements;
        List<string> m_Measurements_list;
        MeasurementFactory factory;
		SystemLogManager m_SystemLogManager;
        public Measurement m_Measurement;                                                        //CREATE MEASUREMENT OBJECT

        //CONSTRUCTOR
		public MeasurementEngine(List<string> measurements_list, ref Measurement measurement, ref SystemLogManager systemLogManager)
        {
            //SET VARIABLES
			factory = new MeasurementFactory(ref measurement);
            this.m_Measurements = new List<Measurement>();
            this.m_Measurements_list = measurements_list;
            this.m_Measurement = measurement;
			this.m_SystemLogManager = systemLogManager;
        }

        //DESTRUCTOR
        ~MeasurementEngine()
        {
        }

        //RUN MEASUREMENTS
        public Hashtable run()
        {
            Hashtable resultstable = new Hashtable();                                         //MEASUREMENT RESULTS HASH TABLE
			System.DateTime dtStartTime;
			double percentComplete;
			int iMeasurementsStarted = 0;

            foreach (string element in m_Measurements_list)
            {
                //Console.WriteLine("MeasurementEngine ELEMENT={0}", element);
				dtStartTime = System.DateTime.UtcNow;
				iMeasurementsStarted += 1;
				percentComplete = iMeasurementsStarted / Convert.ToDouble(m_Measurements_list.Count) * 100.0;

				try
                {
                    resultstable.Add(element, factory.buildMeasurement(element));
					m_SystemLogManager.addApplicationLog((element + ":").PadRight(7) + " complete in " + System.DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds.ToString("0.00") + " seconds; " + percentComplete.ToString("0") + "% complete");
                }
                catch (Exception ex)
                {
                    //THIS HAPPENS WHEN A MEASUREMENT FAILS ... STORE AS NULL!
                    resultstable.Add(element, "Null");
					m_SystemLogManager.addApplicationLog(element + " failed: " + ex.Message);
                }
            }

            //CLEAR MEASUREMENTS
            factory.cleanMeasurements();

            return resultstable;
        }

    }
}
