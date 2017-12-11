using System.Threading.Tasks;

namespace ServiceFabric.StoragePerf.Shared
{
    public interface IStorageProvider
    {
        Task<StoragePerfMetrics> GetBatch(int batchSize);
        Task<Customer> Get(string email);
        Task Add(Customer customer);
        Task Update(Customer customer);
        Task Delete(Customer customer);
        Task Clear();
        Task InitializeTestData(int ItemCount);
    }
}
