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
    public sealed class Combined : StatefulService, IStorageProvider
    {
        private static string DictionaryName = "Customer";
        private static int DatasetSize = 10000;

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
            // first, initialize
            InitializeDataAsync().Wait();

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int batchSize = 5000;
                var metric = GetBatch(batchSize).Result;

                ServiceEventSource.Current.Message($"Batch Size of {batchSize} took {metric.ElapsedTime.Milliseconds} ms.  ");
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
 #region hideme

        public Task Add(Customer customer)
        {
            throw new NotImplementedException();
        }

        public Task Clear()
        {
            throw new NotImplementedException();
        }

        public Task Delete(Customer customer)
        {
            throw new NotImplementedException();
        }

        public Task<Customer> Get(string email)
        {
            throw new NotImplementedException();
        }

        public async Task<StoragePerfMetrics> GetBatch(int batchSize)
        {
            EmailBatchGenerator generator = new EmailBatchGenerator();

            Stopwatch stopwatch = Stopwatch.StartNew();

            var customers = await this.StateManager.TryGetAsync<IReliableDictionary<string, Customer>>(DictionaryName);

            foreach (var email in generator.GetBatch(batchSize, DatasetSize))
            {
                using (var tx = this.StateManager.CreateTransaction())
                {
                    var customer = (await customers.Value.TryGetValueAsync(tx, email)).Value;
                }
            }

            stopwatch.Stop();

            StoragePerfMetrics metrics = new StoragePerfMetrics()
            {
                BatchSize = batchSize,
                ElapsedTime = stopwatch.Elapsed
            };

            return metrics;
        }

        public Task Update(Customer customer)
        {
            throw new NotImplementedException();
        }

#endregion

        public async Task InitializeDataAsync()
        {

            var customersDic = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, Customer>>(DictionaryName);

            // clear the dictionary first
           // customersDic.Value.ClearAsync().Wait();
            
            // fill the collection with a bunch of customers
            for (int x = 0; x < 1000; x++)
            {
                // create new random customer
                var newCust = new Customer
                {
                    FirstName = "John",
                    LastName = "Doe",
                    Email = $"{x}@constos.com",
                    State = "ca",
                    Street = "123 Main St.",
                    ZipCode = "92648"
                };

                try
                {
                    // Create a new Transaction object for this partition
                    using (ITransaction tx = base.StateManager.CreateTransaction())
                    {
                        // AddAsync takes key's write lock; if >4 secs, TimeoutException
                        // Key & value put in temp dictionary (read your own writes),
                        // serialized, redo/undo record is logged & sent to
                        // secondary replicas
                        await customersDic.AddAsync(tx, newCust.Email, newCust);

                        // CommitAsync sends Commit record to log & secondary replicas
                        // After quorum responds, all locks released
                        await tx.CommitAsync();
                    }
                    // If CommitAsync not called, Dispose sends Abort
                    // record to log & all locks released
                }
                catch (TimeoutException)
                {
                    //await Task.Delay(100, cancellationToken); goto retry;
                }

            }
        }

      
    }
}
