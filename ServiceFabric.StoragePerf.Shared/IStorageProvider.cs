namespace ServiceFabric.StoragePerf.Shared
{
    public interface IStorageProvider
    {
        StoragePerfMetrics GetBatch(long batchSize);
        Customer Get(string email);
        void Add(Customer customer);
        void Update(Customer customer);
        void Delete(Customer customer);
        void Clear();
    }
}
