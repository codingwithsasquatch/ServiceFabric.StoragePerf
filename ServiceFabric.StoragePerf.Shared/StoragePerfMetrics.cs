using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace ServiceFabric.StoragePerf.Shared
{
    public class StoragePerfMetrics : TableEntity
    {
        private DateTime timeCollected;

        public StoragePerfMetrics() { }

   
        /// <summary>
        /// The number of items in the batch
        /// </summary>
        public int BatchSize { get; set; }

        /// <summary>
        /// How long the batch took to execute
        /// </summary>
        public TimeSpan ElapsedTime { get; set; }

        /// <summary>
        /// The name of the Service Fabric Node that the metrics were 
        /// colleced from
        /// </summary>
        public string NodeName {
            get { return PartitionKey; }
            set { PartitionKey = value; }
        }

        /// <summary>
        ///  The date and time the performance metrics were collected
        /// </summary>
        public DateTime TimeCollected {
            get { return timeCollected; }
            set
            {
                timeCollected = value;

                // using invertedTicks trick to order records in partition
                RowKey = string.Format("{0:D19}", DateTime.MaxValue.Ticks - value.ToUniversalTime().Ticks);
            }
        }
    }
}
