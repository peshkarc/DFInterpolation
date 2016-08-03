using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DFInterpolationNS
{
    public class GasMeterReadingEntity : TableEntity
    {
        public GasMeterReadingEntity(string partitionkey, string rowkey)
        {
            this.PartitionKey = partitionkey;
            this.RowKey = rowkey;
        }

        public GasMeterReadingEntity() { }

        public string Timestamp { get; set; }
        public DateTime CreatedDate { get; set; }
        public float LDNHighPosition { get; set; }
        public float LDNLowPosition { get; set; }
        public DateTime LastModifiedDate { get; set; }
        public DateTime ReadingDateTime { get; set; }
        public string ReadingMethod { get; set; }
        public float dLDNHighPosition { get; set; }
        public float dLDNLowPosition { get; set; }
        public float LDNGasPosition { get; set; }
        public float dLDNGasUsage { get; set; }
        public string Profile { get; set; }
        public int AnnualStandardUsageOffPeak { get; set; }
        public int AnnualStandardUsagePeak { get; set; }
        public int DeltaLDNGasUsage { get; set; }
        public int AnnualStandardUsageSingle { get; set; }
    }
}
