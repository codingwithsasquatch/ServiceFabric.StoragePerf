using System;
using System.Collections.Generic;

namespace ServiceFabric.StoragePerf.Shared
{
    public class EmailBatchGenerator
    {
        public IEnumerable<string> GetBatch(long batchSize, int datasetSize)
        {
            Random rand = new Random();

            for(int x = 0; x < batchSize; x++)
            {
                yield return $"{rand.Next(datasetSize)}@contoso.com";
            }
        }
    }
}
