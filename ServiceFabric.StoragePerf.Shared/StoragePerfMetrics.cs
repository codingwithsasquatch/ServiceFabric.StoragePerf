using System;

namespace ServiceFabric.StoragePerf.Shared
{
    public class StoragePerfMetrics
    {
        public int BatchSize { get; set; }
        public TimeSpan ElapsedTime { get; set; }
    }
}
