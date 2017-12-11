using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using ServiceFabric.StoragePerf.Shared;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServiceFabric.StoragePerf.StorageProviders.Combined
{
    /// <summary>
    /// Used to log telemetry to Azure Table Storage
    /// </summary>
    public static class TableStorage
    {
        static CloudStorageAccount _StorageAccount;
        static CloudTableClient _TableClient;
        static CloudTable _Table;

        static TableStorage()
        {
            // setup storage credentials
            // Retrieve storage account information from connection string.
            string storageConnectionString = ""; // this has to come from settings
            _StorageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create a table client for interacting with the table service
            _TableClient= _StorageAccount.CreateCloudTableClient();
           
            // Create a table client for interacting with the table service 
            _Table = _TableClient.GetTableReference("perfMetrics");
        }

        /// <summary>
        /// Persists storage provider performance metrics in Azure Table
        /// storage in batches for efficiency. Batch size should limit calls
        /// to this method to no more than once every few seconds.
        /// </summary>
        /// <param name="metrics"></param>
        static void PersistPerfMetrics(List<StoragePerfMetrics> metrics)
        {
            // upload a record into table storage for each item in the list           
            var batchOperations = new TableBatchOperation();
            foreach(var p in metrics)
            {
                batchOperations.Add(TableOperation.Insert(p));
            }
            var result = _Table.ExecuteBatch(batchOperations);
        }
    }
}
