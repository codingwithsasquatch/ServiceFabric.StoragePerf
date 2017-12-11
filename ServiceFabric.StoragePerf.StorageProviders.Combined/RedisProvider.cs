using Microsoft.ServiceFabric.Services.Runtime;
using Newtonsoft.Json;
using ServiceFabric.StoragePerf.Shared;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServiceFabric.StoragePerf.StorageProviders.Combined
{
    public class RedisProvider :  IStorageProvider
    {
        //private const string REDIS_SERVER = "yourmom.redis.cache.windows.net:6380";
        //private const string REDIS_CONNECTION_CONFIG = "yourmom.redis.cache.windows.net:6380,password=DkUlGI+8Zdw8uo3ULMAV+pjPlOlPMBkzZSjaf+BkIi8=,ssl=True,abortConnect=False";
        private const string REDIS_SERVER = "hTFuw3G+b50gP7ZMwfDjXYM+u4IWodPsq7RaRwtw5+4=";
        private const string REDIS_CONNECTION_CONFIG = "perftesting.redis.cache.windows.net:6380,password=hTFuw3G+b50gP7ZMwfDjXYM+u4IWodPsq7RaRwtw5+4=,ssl=True,abortConnect=False";

        private static Lazy<ConnectionMultiplexer> _lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            return ConnectionMultiplexer.Connect(REDIS_CONNECTION_CONFIG);
        });

        private static ConnectionMultiplexer Connection
        {
            get
            {
                return _lazyConnection.Value;
            }
        }

        private int DatasetSize
        {
            get { return 10000; }
        }

        public RedisProvider()
        {
        }

        public Task<StoragePerfMetrics> GetBatch(int batchSize)
        {
            EmailBatchGenerator generator = new EmailBatchGenerator();

            Stopwatch stopwatch = Stopwatch.StartNew();

            IEnumerable<string> emails = generator.GetBatch(batchSize, DatasetSize);

            // Do the work by enumeration
            GetByEnumeration(emails);

            // Do the work as a range
            //GetByRange(emails);

            stopwatch.Stop();

            StoragePerfMetrics metrics = new StoragePerfMetrics()
            {
                TimeCollected = DateTime.Now,

                BatchSize = batchSize,
                ElapsedTime = stopwatch.Elapsed
            };

            return Task.FromResult(metrics);
        }

        private void GetByEnumeration(IEnumerable<string> emails)
        {
            IDatabase cache = Connection.GetDatabase();
            foreach (var email in emails)
            {
                cache.StringGet(email);
            }
        }

        private void GetByEnumerationBatch(IEnumerable<string> emails)
        {
            IDatabase cache = Connection.GetDatabase();
            IBatch batch = cache.CreateBatch();
            var tasks = new List<Task>();
            foreach (var email in emails)
            {
                tasks.Add(batch.StringGetAsync(email));
            }

            batch.Execute();
            Task.WhenAll(tasks);
        }

        private void GetByRange(IEnumerable<string> emails)
        {
            IDatabase cache = Connection.GetDatabase();
            cache.StringGet(emails.Select(key => (RedisKey)key).ToArray());
        }

        private void GetByRangeBatch(IEnumerable<string> emails)
        {
            IDatabase cache = Connection.GetDatabase();
            IBatch batch = cache.CreateBatch();
            var task = batch.StringGetAsync(emails.Select(key => (RedisKey)key).ToArray());
            batch.Execute();
            task.Wait();
        }

        public Task<Customer> Get(string email)
        {
            IDatabase cache = Connection.GetDatabase();
            var retrieved = JsonConvert.DeserializeObject<Customer>(cache.StringGet(email));
            return Task.FromResult(retrieved);
        }

        public Task Add(Customer customer)
        {
            IDatabase cache = Connection.GetDatabase();
            return cache.StringSetAsync(customer.Email, JsonConvert.SerializeObject(customer));
        }

        public Task Update(Customer customer)
        {
            throw new NotImplementedException();
        }

        public Task Delete(Customer customer)
        {
            throw new NotImplementedException();
        }

        public Task Clear()
        {
            var server = Connection.GetServer(REDIS_SERVER);
            server.FlushAllDatabases();
            return Task.CompletedTask;
        }

        public async Task InitializeTestData(int ItemCount)
        {
            var iStorageType = ((IStorageProvider)this);
            // need to clear the redis cache first
            await iStorageType.Clear();
            
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

                await iStorageType.Add(newCust);
            }            
        }
    }
}
