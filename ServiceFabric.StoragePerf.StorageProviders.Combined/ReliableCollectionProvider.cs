using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Runtime;
using ServiceFabric.StoragePerf.Shared;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServiceFabric.StoragePerf.StorageProviders.Combined
{
    public class ReliableCollectionProvider
    {

        private static string DictionaryName = "Customer";
        private static StatefulService statefulService;
        private static int DataSetSize;

        public ReliableCollectionProvider(StatefulService StatefulService)
        {
            statefulService = StatefulService;
        }
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

            var customers = await statefulService.StateManager.TryGetAsync<IReliableDictionary<string, Customer>>(DictionaryName);

            foreach (var email in generator.GetBatch(batchSize, DataSetSize))
            {
                using (var tx = statefulService.StateManager.CreateTransaction())
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


        public async Task InitializeTestData(int ItemCount)
        {
            DataSetSize = ItemCount;
            var customersDic = await statefulService.StateManager.GetOrAddAsync<IReliableDictionary<string, Customer>>(DictionaryName);

            // clear the dictionary first
            // customersDic.Value.ClearAsync().Wait();

            // fill the collection with a bunch of customers
            for (int x = 0; x < ItemCount; x++)
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
                    using (ITransaction tx = statefulService.StateManager.CreateTransaction())
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
