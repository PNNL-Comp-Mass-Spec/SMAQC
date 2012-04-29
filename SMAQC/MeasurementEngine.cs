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
        public Measurement m_Measurement;                                                        //CREATE MEASUREMENT OBJECT

        //CONSTRUCTOR
        public MeasurementEngine(List<string> measurements_list, ref Measurement m_Measurement)
        {
            //SET VARIABLES
            factory = new MeasurementFactory(ref m_Measurement);
            this.m_Measurements = new List<Measurement>();
            this.m_Measurements_list = measurements_list;
            this.m_Measurement = m_Measurement;
        }

        //DESTRUCTOR
        ~MeasurementEngine()
        {
        }

        //RUN MEASUREMENTS
        public Hashtable run()
        {
            Hashtable resultstable = new Hashtable();                                         //MEASUREMENT RESULTS HASH TABLE

            foreach (string element in m_Measurements_list)
            {
                //Console.WriteLine("MeasurementEngine ELEMENT={0}", element);
                try
                {
                    resultstable.Add(element, factory.buildMeasurement(element));
                }
                catch
                {
                    //THIS HAPPENS WHEN A MEASUREMENT FAILS ... STORE AS NULL!
                    resultstable.Add(element, "Null");
                }
            }

            //CLEAR MEASUREMENTS
            factory.cleanMeasurements();

            return resultstable;
        }

    }
}
