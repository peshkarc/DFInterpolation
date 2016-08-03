using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DFInterpolationNS
{
    public class InterpolationOutputEntity : TableEntity
    {
        public InterpolationOutputEntity(string partitionkey, string rowkey)
        {
            this.PartitionKey = partitionkey;
            this.RowKey = rowkey;
        }

        public InterpolationOutputEntity() { }

        public string Timestamp { get; set; }
        //public DateTime CreatedDate { get; set; }
        public float LDNHighPosition { get; set; }
        public float LDNLowPosition { get; set; }
        //public DateTime LastModifiedDate { get; set; }
        public DateTime ReadingDateTime { get; set; }
        //public string ReadingMethod { get; set; }
        //public float dLDNHighPosition { get; set; }
        //public float dLDNLowPosition { get; set; }
        public string Profile { get; set; }
        //public int AnnualStandardUsageOffPeak { get; set; }
        //public int AnnualStandardUsagePeak { get; set; }
        //public int AnnualStandardUsageSingle { get; set; }
        public float pfraction { get; set; }
        public int Ispeak { get; set; }
        public string RecordType { get; set; }
        public string MarketSegment { get; set; }
    }
}
