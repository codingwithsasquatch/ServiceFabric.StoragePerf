using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using ServiceFabric.StoragePerf.Shared;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceFabric.StoragePerf.StorageProviders.CustomerProviderService
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class CustomerProviderService : StatefulService, IStorageProvider
    {
        private static string DictionaryName = "Customer";
        private static int DatasetSize = 10000;

        public CustomerProviderService(StatefulServiceContext context)
            : base(context)
        { }

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

        public async Task<Customer> Get(string email)
        {
            using (var tx = this.StateManager.CreateTransaction())
            {
                var customers = await this.StateManager.TryGetAsync<IReliableDictionary<string, Customer>>(DictionaryName);
                var customer = await customers.Value.TryGetValueAsync(tx, email);
                return customer.Value;
            }
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

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return this.CreateServiceReplicaListeners();
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>(DictionaryName);

            await base.RunAsync(cancellationToken);
        }
    }
}
