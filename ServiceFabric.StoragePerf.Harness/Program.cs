using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Http;
using Newtonsoft.Json;

namespace ServiceFabric.StoragePerf.Harness
{
    public class PerfStats
    {
        public string Info { get; set; }
    }


    class Program
    {
        static HttpClient client = new HttpClient();

        static void Main(string[] args)
        {
            //storageEngine



            //getBatch(size,StorageEngine)
            //Init(size)
            //Drop()
            //Add(storageEngine)
            //
            //

            //action
            String action = args[0];

            //batch size
            int batchSize = Convert.ToInt32(args[1]);

            //count
            int count = Convert.ToInt32(args[2]);

            //storage type
            try
            {
                PerfStats perfStats = GetPerfStatsAsync(action, batchSize).Result;

                //Console.WriteLine(GetBatch(count, batchSize, "reliable"));
            } catch
            {
                Console.WriteLine("something went wrong");
            }

        }

        //static String GetBatch(int count, int batchSize, String storageEngine)
        //{
        //    client.BaseAddress = new Uri("http://endpointurl");
        //    client.DefaultRequestHeaders.Accept.Clear();
        //    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        //    for (int x = 0; x>batchSize; x++)
        //    {
        //        var task = client.GetAsync("");
        //    }

        //    var results = await Task.WhenAll(tasks);
        //    return "";
        //}

        static async Task<PerfStats> GetPerfStatsAsync(String action, int batchSize)
        {
            client.BaseAddress = new Uri("https://httpbin.org/get"+action+"?batchSize="+batchSize);
            client.DefaultRequestHeaders.Accept.Clear();

            PerfStats perfStats = null;
            try
            {
                HttpResponseMessage response = await client.GetAsync(client.BaseAddress);

                if (response.IsSuccessStatusCode)
                {
                    String responseString = await response.Content.ReadAsStringAsync();
                    perfStats = JsonConvert.DeserializeObject<PerfStats>(responseString);
                }
            } catch {
                Console.WriteLine("couldn't connect to host");
            }


            return perfStats;
        }
    }
}
