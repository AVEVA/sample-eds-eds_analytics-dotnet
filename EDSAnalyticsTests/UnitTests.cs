using System;
using System.Collections.Generic;
using System.Linq;
using EDSAnalytics;
using Xunit;

namespace EDSAnalyticsTest
{
    public class UnitTests
    {
        public static bool RunTestAsync()
        {
            try
            {
                // ====================== Data Filtering portion ======================
                Console.WriteLine();
                Console.WriteLine("================= Data Filtering =================");

                // Step 1 - Create SineWave type
                List<SdsTypeProperty> sineWaveProperties = new ()
                {
                        Program.CreateSdsTypePropertyOfTypeDateTime(nameof(SineData.Timestamp), true),
                        Program.CreateSdsTypePropertyOfTypeDouble(nameof(SineData.Value), false),
                };

                // Step 3 - Create a list of events of SineData objects. The value property of the SineData object is intitialized to a value between -1.0 and 1.0
                Console.WriteLine("Initializing SineData Events");
                List<SineData> waveList = new ();
                DateTime firstTimestamp = DateTime.UtcNow;

                // numberOfEvents must be an integer > 1
                int numberOfEvents = 100;
                for (int i = 0; i < numberOfEvents; i++)
                {
                    waveList.Add(new SineData(i)
                    {
                        Timestamp = firstTimestamp.AddSeconds(i),
                    });
                }

                // Step 4 - Ingress the sine wave data from the SineWave stream
                List<SineData> returnData = Enumerable.Range(0, 100).Select(i => new SineData(i)).ToList();

                // Step 6 - Populate FilteredSineWaveStream with filtered data
                List<SineData> filteredWave = new ();
                int numberOfValidValues = 0;
                Console.WriteLine("Filtering Data");
                for (int i = 0; i < numberOfEvents; i++)
                {
                    // filters the data to only include values outside the range -0.9 to 0.9 
                    // change this conditional to apply the type of filter you desire
                    if (returnData[i].Value > .9 || returnData[i].Value < -.9)
                    {
                        filteredWave.Add(returnData[i]);
                        numberOfValidValues++;
                    }
                }

                // ====================== Data Aggregation portion ======================
                Console.WriteLine();
                Console.WriteLine("================ Data Aggregation ================");

                // Step 7 - Create aggregatedDataType type   
                List<SdsTypeProperty> aggregatedData = new ()
                {
                        Program.CreateSdsTypePropertyOfTypeDateTime(Constants.AggregatedDataTimestampProperty, true),
                        Program.CreateSdsTypePropertyOfTypeDouble(Constants.AggregatedDataMeanProperty, false),
                        Program.CreateSdsTypePropertyOfTypeDouble(Constants.AggregatedDataMinimumProperty, false),
                        Program.CreateSdsTypePropertyOfTypeDouble(Constants.AggregatedDataMaximumProperty, false),
                        Program.CreateSdsTypePropertyOfTypeDouble(Constants.AggregatedDataRangeProperty, false),
                };

                // Step 9 - Calculate mean, min, max, and range using c# libraries and send to the CalculatedAggregatedData Stream
                Console.WriteLine("Calculating mean, min, max, and range");
                List<double> sineDataValues = new ();
                for (int i = 0; i < numberOfEvents; i++)
                {
                    sineDataValues.Add(returnData[i].Value);
                    numberOfValidValues++;
                }

                AggregateData calculatedData = new ()
                {
                    Timestamp = firstTimestamp,
                    Mean = returnData.Average(rd => rd.Value),
                    Minimum = sineDataValues.Min(),
                    Maximum = sineDataValues.Max(),
                    Range = sineDataValues.Max() - sineDataValues.Min(),
                };

                Console.WriteLine("    Mean = " + calculatedData.Mean);
                Console.WriteLine("    Minimum = " + calculatedData.Minimum);
                Console.WriteLine("    Maximum = " + calculatedData.Maximum);
                Console.WriteLine("    Range = " + calculatedData.Range);

                return true;
            }
            catch
            {
                return false;
            }
        }

        [Fact]
        public void EDSAnalyticsTest()
        {
            Assert.True(RunTestAsync());
        }
    }
}
