﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;

namespace EDSAnalytics
{
    public static class Program
    {
        private static readonly HttpClient _httpClient = new ();
        private static readonly HttpClient _httpClientGzip = new ();

        private static int _port;
        private static string _tenantId;
        private static string _namespaceId;

        public static async Task Main()
        {
            await MainAsync().ConfigureAwait(false);
            Console.WriteLine();
            Console.WriteLine("Demo Application Ran Successfully!");
        }

        public static async Task<bool> MainAsync()
        {
            // ==== Client constants ====
            _port = Constants.DefaultPortNumber;    // defaults to 5590
            _tenantId = Constants.TenantId;
            _namespaceId = Constants.NamespaceId;

            _httpClientGzip.DefaultRequestHeaders.Add("Accept-Encoding", "gzip");

            try
            {
                // ====================== Data Filtering portion ======================
                Console.WriteLine();
                Console.WriteLine("================= Data Filtering =================");

                // Step 1 - Create SineWave type
                List<SdsTypeProperty> sineWaveProperties = new ()
                {
                        CreateSdsTypePropertyOfTypeDateTime(nameof(SineData.Timestamp), true),
                        CreateSdsTypePropertyOfTypeDouble(nameof(SineData.Value), false),
                };

                SdsType sineWaveType = await AsyncCreateTypeAsync(Constants.SineWaveStream, Constants.SineWaveStream, sineWaveProperties).ConfigureAwait(false);

                // Step 2 - Create SineWave stream        
                SdsStream sineWaveStream = await AsyncCreateStreamAsync(sineWaveType, Constants.SineWaveStream, Constants.SineWaveStream).ConfigureAwait(false);

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

                await AsyncWriteDataToStreamAsync(waveList, sineWaveStream).ConfigureAwait(false);

                // Step 4 - Ingress the sine wave data from the SineWave stream
                List<SineData> returnData = await AsyncQuerySineDataAsync(sineWaveStream, waveList[0].Timestamp, numberOfEvents).ConfigureAwait(false);

                // Step 5 - Create FilteredSineWaveStream
                SdsStream filteredSineWaveStream = await AsyncCreateStreamAsync(sineWaveType, Constants.FilteredSineWaveStream, Constants.FilteredSineWaveStream).ConfigureAwait(false);

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

                await AsyncWriteDataToStreamAsync(filteredWave, filteredSineWaveStream).ConfigureAwait(false);

                // ====================== Data Aggregation portion ======================
                Console.WriteLine();
                Console.WriteLine("================ Data Aggregation ================");

                // Step 7 - Create aggregatedDataType type   
                List<SdsTypeProperty> aggregatedData = new ()
                {
                        CreateSdsTypePropertyOfTypeDateTime(Constants.AggregatedDataTimestampProperty, true),
                        CreateSdsTypePropertyOfTypeDouble(Constants.AggregatedDataMeanProperty, false),
                        CreateSdsTypePropertyOfTypeDouble(Constants.AggregatedDataMinimumProperty, false),
                        CreateSdsTypePropertyOfTypeDouble(Constants.AggregatedDataMaximumProperty, false),
                        CreateSdsTypePropertyOfTypeDouble(Constants.AggregatedDataRangeProperty, false),
                };

                SdsType aggregatedDataType = await AsyncCreateTypeAsync(Constants.AggregatedDataStream, Constants.AggregatedDataStream, aggregatedData).ConfigureAwait(false);

                // Step 8 - Create CalculatedAggregatedData stream
                SdsStream calculatedAggregatedDataStream = await AsyncCreateStreamAsync(aggregatedDataType, 
                    Constants.CalculatedAggregatedDataStream, Constants.CalculatedAggregatedDataStream).ConfigureAwait(false);

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
                await AsyncWriteDataToStreamAsync(calculatedData, calculatedAggregatedDataStream).ConfigureAwait(false);

                // Step 10 - Create EdsApiAggregatedData stream
                SdsStream edsApiAggregatedDataStream = await AsyncCreateStreamAsync(aggregatedDataType, 
                    Constants.EdsApiAggregatedDataStream, Constants.EdsApiAggregatedDataStream).ConfigureAwait(false);

                // Step 11 - Use EDS’s standard data aggregate API calls to ingress aggregated data calculated by EDS and send to EdsApiAggregatedData stream
                string summaryData = await AsyncQuerySummaryDataAsync(sineWaveStream, calculatedData.Timestamp, firstTimestamp.AddSeconds(numberOfEvents)).ConfigureAwait(false);
                AggregateData edsApi = new ()
                {
                    Timestamp = firstTimestamp,
                    Mean = GetValue(summaryData, Constants.AggregatedDataMeanProperty),
                    Minimum = GetValue(summaryData, Constants.AggregatedDataMinimumProperty),
                    Maximum = GetValue(summaryData, Constants.AggregatedDataMaximumProperty),
                    Range = GetValue(summaryData, Constants.AggregatedDataRangeProperty),
                };

                await AsyncWriteDataToStreamAsync(edsApi, edsApiAggregatedDataStream).ConfigureAwait(false);

                Console.WriteLine();
                Console.WriteLine("==================== Clean-Up =====================");

                // Step 12 - Delete Streams and Types
                await AsyncDeleteStreamAsync(sineWaveStream).ConfigureAwait(false);
                await AsyncDeleteStreamAsync(filteredSineWaveStream).ConfigureAwait(false);
                await AsyncDeleteStreamAsync(calculatedAggregatedDataStream).ConfigureAwait(false);
                await AsyncDeleteStreamAsync(edsApiAggregatedDataStream).ConfigureAwait(false);
                await AsyncDeleteTypeAsync(sineWaveType).ConfigureAwait(false);
                await AsyncDeleteTypeAsync(aggregatedDataType).ConfigureAwait(false);
            }
            catch
            {
                _httpClient?.Dispose();
                _httpClientGzip?.Dispose();
                throw;
            }
            finally
            {
                _httpClient?.Dispose();
                _httpClientGzip?.Dispose();
            }

            return true;
        }

        private static void CheckIfResponseWasSuccessful(HttpResponseMessage response)
        {
            if (!response.IsSuccessStatusCode)
            {
                throw new HttpRequestException(response.ToString());
            }
        }

        private static async Task AsyncDeleteStreamAsync(SdsStream stream)
        {
            Console.WriteLine("Deleting " + stream.Id + " Stream");

            Uri requestUri = new ($"http://localhost:{_port}/api/v1/Tenants/{_tenantId}/Namespaces/{_namespaceId}/Streams/{stream.Id}");
            HttpResponseMessage responseDeleteStream = await _httpClient.DeleteAsync(requestUri).ConfigureAwait(false);
            CheckIfResponseWasSuccessful(responseDeleteStream);
        }

        private static async Task AsyncDeleteTypeAsync(SdsType type)
        {
            Console.WriteLine("Deleting " + type.Id + " Type");

            Uri requestUri = new ($"http://localhost:{_port}/api/v1/Tenants/{_tenantId}/Namespaces/{_namespaceId}/Types/{type.Id}");
            HttpResponseMessage responseDeleteType = await _httpClient.DeleteAsync(requestUri).ConfigureAwait(false);
            CheckIfResponseWasSuccessful(responseDeleteType);
        }

        private static async Task<SdsStream> AsyncCreateStreamAsync(SdsType type, string id, string name)
        {
            SdsStream stream = new ()
            {
                TypeId = type.Id,
                Id = id,
                Name = name,
            };

            Console.WriteLine("Creating " + stream.Id + " Stream");
            
            using (StringContent stringStream = new (JsonSerializer.Serialize(stream)))
            {
                Uri requestUri = new ($"http://localhost:{_port}/api/v1/Tenants/{_tenantId}/Namespaces/{_namespaceId}/Streams/{stream.Id}");
                HttpResponseMessage responseCreateStream = await _httpClient.PostAsync(requestUri, stringStream).ConfigureAwait(false);

                CheckIfResponseWasSuccessful(responseCreateStream);
            }

            return stream;
        }

        private static async Task<SdsType> AsyncCreateTypeAsync(string id, string name, List<SdsTypeProperty> properties)
        {
            SdsType type = new (id, name, 1, properties);

            Console.WriteLine("Creating " + type.Id + " Type");
            
            using (StringContent stringType = new (JsonSerializer.Serialize(type)))
            {
                Uri requestUri = new ($"http://localhost:{_port}/api/v1/Tenants/{_tenantId}/Namespaces/{_namespaceId}/Types/{type.Id}");
                HttpResponseMessage responseType = await _httpClient.PostAsync(requestUri, stringType).ConfigureAwait(false);

                CheckIfResponseWasSuccessful(responseType);
            }

            return type;
        }

        private static async Task<List<SineData>> AsyncQuerySineDataAsync(SdsStream stream, DateTime timestamp, int numberOfEvents)
        {
            Console.WriteLine("Ingressing data from " + stream.Id + " stream");

            Uri requestUri = new ($"http://localhost:{_port}/api/v1/Tenants/{_tenantId}/Namespaces/{_namespaceId}/Streams/" +
                $"{stream.Id}/Data?startIndex={timestamp:o}&count={numberOfEvents}");
            using HttpResponseMessage responseIngress = await _httpClientGzip.GetAsync(requestUri).ConfigureAwait(false);
            CheckIfResponseWasSuccessful(responseIngress);
            MemoryStream ms = await AsyncDecompressGzipAsync(responseIngress).ConfigureAwait(false);
            using StreamReader sr = new (ms);
            return await JsonSerializer.DeserializeAsync<List<SineData>>(ms).ConfigureAwait(false);
        }

        private static async Task<string> AsyncQuerySummaryDataAsync(SdsStream stream, DateTime startTimestamp, DateTime endTimestamp)
        {
            Console.WriteLine("Ingressing Data from " + stream.Id + " Stream Summary");

            Uri requestUri = new ($"http://localhost:{_port}/api/v1/Tenants/{_tenantId}/Namespaces/{_namespaceId}/Streams/" +
                $"{stream.Id}/Data/Summaries?startIndex={startTimestamp:o}&endIndex={endTimestamp:o}&count=1");
            using HttpResponseMessage responseIngress = await _httpClientGzip.GetAsync(requestUri).ConfigureAwait(false);
            CheckIfResponseWasSuccessful(responseIngress);
            MemoryStream ms = await AsyncDecompressGzipAsync(responseIngress).ConfigureAwait(false);
            using StreamReader sr = new (ms);
            object objectSummaryData = await JsonSerializer.DeserializeAsync<object>(ms).ConfigureAwait(false);
            return objectSummaryData.ToString().TrimStart(new char[] { '[' }).TrimEnd(new char[] { ']' });
        }

        private static async Task<MemoryStream> AsyncDecompressGzipAsync(HttpResponseMessage httpMessage)
        {
            using Stream response = await httpMessage.Content.ReadAsStreamAsync().ConfigureAwait(false);
            MemoryStream destination = new ();
            using (Stream decompressor = (Stream)new GZipStream(response, CompressionMode.Decompress, true))
            {
                await decompressor.CopyToAsync(destination).ConfigureAwait(false);
            }

            destination.Seek(0, SeekOrigin.Begin);
            return destination;
        }

        private static async Task AsyncWriteDataToStreamAsync(List<SineData> list, SdsStream stream)
        {
            Console.WriteLine("Writing Data to " + stream.Id + " stream");

            using StringContent serializedData = new (JsonSerializer.Serialize(list));
            Uri requestUri = new ($"http://localhost:{_port}/api/v1/Tenants/{_tenantId}/Namespaces/{_namespaceId}/Streams/{stream.Id}/Data");
            HttpResponseMessage responseWriteDataToStream = await _httpClient.PostAsync(requestUri, serializedData).ConfigureAwait(false);
            CheckIfResponseWasSuccessful(responseWriteDataToStream);
        }

        private static async Task AsyncWriteDataToStreamAsync(AggregateData data, SdsStream stream)
        {
            List<AggregateData> dataList = new ()
            {
                data,
            };
            Console.WriteLine("Writing Data to " + stream.Id + " stream");

            using StringContent serializedData = new (JsonSerializer.Serialize(dataList));
            Uri requestUri = new ($"http://localhost:{_port}/api/v1/Tenants/{_tenantId}/Namespaces/{_namespaceId}/Streams/{stream.Id}/Data");
            HttpResponseMessage responseWriteDataToStream = await _httpClient.PostAsync(requestUri, serializedData).ConfigureAwait(false);
            CheckIfResponseWasSuccessful(responseWriteDataToStream);
        }

        private static SdsTypeProperty CreateSdsTypePropertyOfTypeDouble(string idAndName, bool isKey)
        {
            SdsTypeProperty property = new ()
            {
                Id = idAndName,
                Name = idAndName,
                IsKey = isKey,
                SdsType = new SdsType
                {
                    Name = Constants.DoubleTypeName,
                    SdsTypeCode = 14, // 14 is the SdsTypeCode for a Double type. Go to the SdsTypeCode section in EDS documentation for more information.
                },
            };

            return property;
        }

        private static SdsTypeProperty CreateSdsTypePropertyOfTypeDateTime(string idAndName, bool isKey)
        {
            SdsTypeProperty property = new ()
            {
                Id = idAndName,
                Name = idAndName,
                IsKey = isKey,
                SdsType = new SdsType
                {
                    Name = Constants.DateTimeTypeName,
                    SdsTypeCode = 16, // 16 is the SdsTypeCode for a DateTime type. Go to the SdsTypeCode section in EDS documentation for more information.
                },
            };

            return property;
        }

        private static double GetValue(string json, string property)
        {
            using JsonDocument document = JsonDocument.Parse(json);
            JsonElement root = document.RootElement;
            JsonElement summaryElement = root.GetProperty(Constants.SummariesProperty);
            if (summaryElement.TryGetProperty(property, out JsonElement propertyElement))
            {
                if (propertyElement.TryGetProperty(Constants.ValueProperty, out JsonElement valueElement))
                {
                    Console.WriteLine("    " + property + " = " + valueElement.ToString());
                    return Convert.ToDouble(valueElement.ToString(), CultureInfo.InvariantCulture);
                }
            }

            return 0;
        }
    }
}
