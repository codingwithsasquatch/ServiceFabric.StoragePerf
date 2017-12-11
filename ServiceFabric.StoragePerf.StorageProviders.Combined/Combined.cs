using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using ServiceFabric.StoragePerf.Shared;
using System.Diagnostics;
using Microsoft.ServiceFabric.Data;

namespace ServiceFabric.StoragePerf.StorageProviders.Combined
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    public sealed class Combined : StatefulService
    {
        private static int DatasetSize = 10000;
        private static int ReadToWriteMultiple = 5; // 5 times more reads than writes
        private static int RampIntervalCount = 5; //at each interval, the delay between batches decreases
        private static TimeSpan StartingDelay = TimeSpan.FromSeconds(1);  // the delay between batches
        private static TimeSpan RampInterval = TimeSpan.FromMinutes(1); // duration spent at each interval
        public Combined(StatefulServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new ServiceReplicaListener[0];
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // get provider to use from config
            //ReliableCollectionProvider rcp = new ReliableCollectionProvider(this);
            var storageProvider = new RedisProvider();

            // first, initialize
            //storageProvider.InitializeTestData(DatasetSize).Wait();

            // ramp up to full load by decreasing the delay between batches

            int currentInterval = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // TODO: batch size should be dynamically calculated to take a certain 
                // amount of time, say 2-5 seconds.  The batch size would be different
                // between storage providers due to the latency inherent in each.
                int batchSize = 5000;
                TimeSpan delayThisInterval = new TimeSpan( (StartingDelay.Ticks / RampIntervalCount) * currentInterval);


                // handle gets and updates outside of storageproviders using their get and
                // update interface members.  Keeps it consistant and less code
                // TODO: probably just put in a base class for storageProviders

                // get list of emails for this batch
                var emailGenerator = new EmailBatchGenerator();
                var emails = emailGenerator.GetBatch(batchSize, DatasetSize);

                Random r = new Random();
                foreach(var email in emails)
                {
                    Customer c = storageProvider.Get(email).Result;
                    
                    // randomly do an update as frequently as ReadWriteMultiple dictates
                    if( r.Next(ReadToWriteMultiple) == ReadToWriteMultiple - 1)
                    {
                        // do an update on this one
                        c.FirstName = GetRandomString(10);
                        storageProvider.Update(c).GetAwaiter().GetResult();
                    }
                   
                }

                var metric = storageProvider.GetBatch(batchSize).Result;
                metric.NodeName = this.Context.NodeContext.NodeName;
                metric.TimeCollected = DateTime.Now;
                

                ServiceEventSource.Current.Message($"Batch Size of {batchSize} took {metric.ElapsedTime.Milliseconds} ms.  ");
                await Task.Delay(delayThisInterval, cancellationToken);
                if (currentInterval < 5)
                    currentInterval++;
            }
        }

        static Random rand = new Random();
        public const string Alphabet = "abcdefghijklmnopqrstuvwyxzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";


        static string GetRandomString(int size)
        {
            char[] chars = new char[size];
            for (int i = 0; i<size; i++)
            {
                chars[i] = Alphabet[rand.Next(Alphabet.Length)];
            }
            return new string (chars);
        }

    }
}
