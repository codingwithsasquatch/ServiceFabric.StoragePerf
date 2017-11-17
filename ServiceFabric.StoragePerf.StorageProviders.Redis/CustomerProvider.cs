using Newtonsoft.Json;
using ServiceFabric.StoragePerf.Shared;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServiceFabric.StoragePerf.StorageProviders.Redis
{
    public class CustomerProvider : IStorageProvider
    {
        private const string REDIS_SERVER = "yourmom.redis.cache.windows.net:6380";
        private const string REDIS_CONNECTION_CONFIG = "yourmom.redis.cache.windows.net:6380,password=DkUlGI+8Zdw8uo3ULMAV+pjPlOlPMBkzZSjaf+BkIi8=,ssl=True,abortConnect=False";

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

        public CustomerProvider()
        {
        }

        Task<StoragePerfMetrics> IStorageProvider.GetBatch(int batchSize)
        {
            EmailBatchGenerator generator = new EmailBatchGenerator();

            Stopwatch stopwatch = Stopwatch.StartNew();

            IEnumerable<string> emails = generator.GetBatch(batchSize, DatasetSize);

            // Do the work by enumeration
            GetByEnumeration(emails);

            // Do the work as a range
            GetByRange(emails);

            stopwatch.Stop();

            StoragePerfMetrics metrics = new StoragePerfMetrics()
            {
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

        Task<Customer> IStorageProvider.Get(string email)
        {
            IDatabase cache = Connection.GetDatabase();
            var retrieved = JsonConvert.DeserializeObject<Customer>(cache.StringGet(email));
            return Task.FromResult(retrieved);
        }

        Task IStorageProvider.Add(Customer customer)
        {
            IDatabase cache = Connection.GetDatabase();
            return cache.StringSetAsync(customer.Email, JsonConvert.SerializeObject(customer));
        }

        Task IStorageProvider.Update(Customer customer)
        {
            throw new NotImplementedException();
        }

        Task IStorageProvider.Delete(Customer customer)
        {
            throw new NotImplementedException();
        }

        Task IStorageProvider.Clear()
        {
            var server = Connection.GetServer(REDIS_SERVER);
            server.FlushAllDatabases();
            return Task.CompletedTask;
        }
    }
}
