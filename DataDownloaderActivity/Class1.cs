namespace DFInterpolationNS
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.IO.Compression;
    using System.Net;
    using System.Reflection;
    using System.Threading;
    using Microsoft.Azure.Management.DataFactories.Models;
    using Microsoft.Azure.Management.DataFactories.Runtime;

    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage;
    using System.Linq;
    using System.Text;
    using D = System.Data;           // System.Data.dll  
    using C = System.Data.SqlClient; // System.Data.dll 
    using Microsoft.WindowsAzure.Storage.Table;

    public class DFInterpolation : IDotNetActivity
    {
        public const string accountName = "vandebronanalytics";
        public const string accountKey = "3w4JZIQ2diCbKbTAg0jiVxsiEAUx8lj5mFBazfPbziOSgvzlKuU+p7xALtSmH+JNpl53e5DPJAZU0FONukQDCg==";

        public IDictionary<string, string> Execute(
           IEnumerable<LinkedService> linkedServices,
           IEnumerable<Dataset> datasets,
           Activity activity,
           IActivityLogger logger)
        {
            using (var connection = new C.SqlConnection("Server=tcp:vdbanalytics.database.windows.net,1433;Initial Catalog=vdbanalytics-dev;Persist Security Info=False;User ID=vdbadmin;Password=admin123$$;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"))
            {
                var connectionupd = new C.SqlConnection("Server=tcp:vdbanalytics.database.windows.net,1433;Initial Catalog=vdbanalytics-dev;Persist Security Info=False;User ID=vdbadmin;Password=admin123$$;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;");
                connection.Open();
                connectionupd.Open();
                C.SqlCommand command = new C.SqlCommand();
                C.SqlCommand updcmd = new C.SqlCommand();
                updcmd.Connection = connectionupd;
                updcmd.CommandTimeout = 0;

                command.Connection = connection;
                command.CommandTimeout = 0;
                command.CommandText = @"select * from dbo.ReportQueue where Status != 'Processed'";
                C.SqlDataReader reader = command.ExecuteReader();

                while (reader.Read())
                {
                    Console.WriteLine(reader[3].ToString() + " " + reader[1].ToString() + " " + reader[2].ToString());
                    ProcessRecords(reader[3].ToString(), reader[1].ToString(), reader[2].ToString(), reader[4].ToString());
                    updcmd.CommandText = @"UPDATE dbo.ReportQueue SET Status = 'Processed' WHERE ConnectionID='" + reader[3].ToString() + "'";
                    updcmd.ExecuteNonQuery();
                }
                connection.Close();
                connectionupd.Close();
            }



            return new Dictionary<string, string>();
        }
        static public void ProcessRecords(string connid, string From, string To, string MktSeg)
        {
            var listofreadings = new List<dynamic>();
            var SortedList = new List<dynamic>();
            int gap = 0;
            double MinutesGap;
            try
            {
                StorageCredentials creds = new StorageCredentials(accountName, accountKey);
                CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);

                CloudTableClient client = account.CreateCloudTableClient();
                string tablename = "";
                if (MktSeg.Trim() == "Electricity")
                {
                    tablename = "ElectricMeterReading";
                }
                else if (MktSeg.Trim() == "Gas")
                {
                    tablename = "GasMeterReading";
                }
                CloudTable table = client.GetTableReference(tablename);

                string pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, connid.ToUpper());
                string rkLowerFilter = TableQuery.GenerateFilterConditionForDate("ReadingDateTime", QueryComparisons.GreaterThanOrEqual, DateTime.Parse(From));
                string rkUpperFilter = TableQuery.GenerateFilterConditionForDate("ReadingDateTime", QueryComparisons.LessThanOrEqual, DateTime.Parse(To));

                string combinedFilter = string.Format("({0}) {1} ({2}) {3} ({4})", pkFilter, TableOperators.And, rkLowerFilter, TableOperators.And, rkUpperFilter);
                if (MktSeg.Trim() == "Electricity")
                {
                    TableQuery<ElectricMeterReadingEntity> query = new TableQuery<ElectricMeterReadingEntity>().Where(combinedFilter);
                    foreach (ElectricMeterReadingEntity entity in table.ExecuteQuery(query))
                    {
                        listofreadings.Add(entity);
                    }
                }
                else if (MktSeg.Trim() == "Gas")
                {
                    TableQuery<GasMeterReadingEntity> query = new TableQuery<GasMeterReadingEntity>().Where(combinedFilter);
                    foreach (GasMeterReadingEntity entity in table.ExecuteQuery(query))
                    {
                        listofreadings.Add(entity);
                    }
                }
                SortedList = listofreadings.OrderBy(o => o.ReadingDateTime).ToList();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            Console.WriteLine("Check Time & the function to call");
            if (MktSeg.Trim() == "Electricity")
            {
                gap = 15;
            }
            else if (MktSeg.Trim() == "Gas")
            {
                gap = 60;
            }
            if (SortedList.Count > 0)
            {
                using (var connection = new C.SqlConnection("Server=tcp:vdbanalytics.database.windows.net,1433;Initial Catalog=vdbanalytics-dev;Persist Security Info=False;User ID=vdbadmin;Password=admin123$$;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"))
                {
                    connection.Open();
                    if (Convert.ToDateTime(From).Subtract(SortedList[0].ReadingDateTime).TotalMinutes > gap)
                    {
                        if (MktSeg.Trim() == "Electricity")
                        {
                            SelectRowsElecReInt(connection, Convert.ToDateTime(From), SortedList[0].ReadingDateTime, SortedList[0]);
                        }
                        else if (MktSeg.Trim() == "Gas")
                        {
                            SelectRowsGasReInt(connection, Convert.ToDateTime(From), SortedList[0].ReadingDateTime, SortedList[0]);
                        }
                        Console.WriteLine("From Re-Interploation between {0} and {1}", From, SortedList[0].ReadingDateTime);
                    }
                    for (var i = 1; i < SortedList.Count; i++)
                    {
                        MinutesGap = SortedList[i].ReadingDateTime.Subtract(SortedList[i - 1].ReadingDateTime).TotalMinutes;
                        Console.WriteLine("MinutesGap: " + MinutesGap);
                        if (MinutesGap > gap)
                        {
                            if (MktSeg.Trim() == "Electricity")
                            {
                                SelectRowsElecReInt(connection, SortedList[i - 1].ReadingDateTime, SortedList[i].ReadingDateTime, SortedList[i - 1]);
                            }
                            else if (MktSeg.Trim() == "Gas")
                            {
                                SelectRowsGasReInt(connection, SortedList[i - 1].ReadingDateTime, SortedList[i].ReadingDateTime, SortedList[i - 1]);
                            }
                            Console.WriteLine("Re-Interploation between {0} and {1}", SortedList[i - 1].ReadingDateTime, SortedList[i].ReadingDateTime);
                        }
                    }
                    if (Convert.ToDateTime(To).Subtract(SortedList[SortedList.Count - 1].ReadingDateTime).TotalMinutes > gap)
                    {
                        if (MktSeg.Trim() == "Electricity")
                        {
                            SelectRowsElecInt(connection, SortedList[SortedList.Count - 1].ReadingDateTime, Convert.ToDateTime(To), SortedList[SortedList.Count - 1]);
                        }
                        else if (MktSeg.Trim() == "Gas")
                        {
                            SelectRowsGasInt(connection, SortedList[SortedList.Count - 1].ReadingDateTime, Convert.ToDateTime(To), SortedList[SortedList.Count - 1]);
                        }
                        Console.WriteLine("From Interploation between {0} and {1}", SortedList[SortedList.Count - 1].ReadingDateTime, To);
                    }
                    connection.Close();
                }
            }
            Console.WriteLine("Finish...");
            Console.ReadKey(true);
        }
        static public void SelectRowsElecInt(C.SqlConnection connection, DateTime dtfrm, DateTime dtto, ElectricMeterReadingEntity Ent)
        {
            using (var command = new C.SqlCommand())
            {
                StorageCredentials creds = new StorageCredentials(accountName, accountKey);
                CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);
                CloudTableClient client = account.CreateCloudTableClient();
                CloudTable table = client.GetTableReference("InterpolationOutput");

                command.Connection = connection;
                command.CommandTimeout = 0;
                command.CommandText = @"select * from dbo.fn_elec_interpolate(@RLeft, @RRight, @Profile, @annualstdusage_peak, @annualstdusage_offpeak)";
                command.Parameters.AddWithValue("@RLeft", dtfrm);
                command.Parameters.AddWithValue("@RRight", dtto);
                command.Parameters.AddWithValue("@Profile", Ent.Profile);
                command.Parameters.AddWithValue("@annualstdusage_peak", Ent.AnnualStandardUsagePeak);
                command.Parameters.AddWithValue("@annualstdusage_offpeak", Ent.AnnualStandardUsageOffPeak);
                C.SqlDataReader reader = command.ExecuteReader();

                TableBatchOperation batchOperation = new TableBatchOperation();

                while (reader.Read())
                {
                    InterpolationOutputEntity IpEntity = new InterpolationOutputEntity();
                    IpEntity.PartitionKey = Ent.PartitionKey.ToString();
                    IpEntity.RowKey = Guid.NewGuid().ToString();
                    IpEntity.Timestamp = DateTime.Now.ToString();
                    IpEntity.LDNHighPosition = reader[0].Equals(DBNull.Value) ? 0 : Convert.ToInt32(reader[0]);
                    IpEntity.LDNLowPosition = reader[1].Equals(DBNull.Value) ? 0 : Convert.ToInt32(reader[1]);
                    IpEntity.ReadingDateTime = Convert.ToDateTime(reader[2]).ToUniversalTime();
                    IpEntity.Profile = reader[3].ToString();
                    IpEntity.pfraction = float.Parse(reader[4].ToString());
                    IpEntity.Ispeak = Convert.ToInt16(reader[5]);
                    IpEntity.RecordType = reader[6].ToString();
                    IpEntity.MarketSegment = "Electricity";
                    batchOperation.Insert(IpEntity);
                }
                table.ExecuteBatch(batchOperation);
            }
        }

        static public void SelectRowsElecReInt(C.SqlConnection connection, DateTime dtfrm, DateTime dtto, ElectricMeterReadingEntity Ent)
        {
            using (var command = new C.SqlCommand())
            {
                command.Connection = connection;
                command.CommandTimeout = 0;
                command.CommandText = @"select * from dbo.fn_elect_reinterpolate(@RLeft, @RRight, @Profile, @deltaLDNHigh, @deltaLDNLow)";
                command.Parameters.AddWithValue("@RLeft", dtfrm);
                command.Parameters.AddWithValue("@RRight", dtto);
                command.Parameters.AddWithValue("@Profile", Ent.Profile);
                command.Parameters.AddWithValue("@deltaLDNHigh", Ent.dLDNHighPosition);
                command.Parameters.AddWithValue("@deltaLDNLow", Ent.dLDNLowPosition);
                C.SqlDataReader reader = command.ExecuteReader();

                StorageCredentials creds = new StorageCredentials(accountName, accountKey);
                CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);
                CloudTableClient client = account.CreateCloudTableClient();
                CloudTable table = client.GetTableReference("InterpolationOutput");

                TableBatchOperation batchOperation = new TableBatchOperation();

                while (reader.Read())
                {
                    InterpolationOutputEntity IpEntity = new InterpolationOutputEntity();
                    IpEntity.PartitionKey = Ent.PartitionKey.ToString();
                    IpEntity.RowKey = Guid.NewGuid().ToString();
                    IpEntity.Timestamp = DateTime.Now.ToString();
                    IpEntity.LDNHighPosition = reader[0].Equals(DBNull.Value) ? 0 : Convert.ToInt32(reader[0]);
                    IpEntity.LDNLowPosition = reader[1].Equals(DBNull.Value) ? 0 : Convert.ToInt32(reader[1]);
                    IpEntity.ReadingDateTime = Convert.ToDateTime(reader[2]).ToUniversalTime();
                    IpEntity.Profile = reader[3].ToString();
                    IpEntity.pfraction = float.Parse(reader[4].ToString());
                    IpEntity.Ispeak = Convert.ToInt16(reader[5]);
                    IpEntity.RecordType = reader[6].ToString();
                    IpEntity.MarketSegment = "Electricity";
                    batchOperation.Insert(IpEntity);
                }
                table.ExecuteBatch(batchOperation);
            }
        }
        static public void SelectRowsGasInt(C.SqlConnection connection, DateTime dtfrm, DateTime dtto, GasMeterReadingEntity Ent)
        {
            using (var command = new C.SqlCommand())
            {
                command.Connection = connection;
                command.CommandTimeout = 1000;
                command.CommandText = @"select * from dbo.fn_gas_interpolate(@RLeft, @RRight, @Profile, @annualstdusage_single)";
                command.Parameters.AddWithValue("@RLeft", dtfrm);
                command.Parameters.AddWithValue("@RRight", dtto);
                command.Parameters.AddWithValue("@Profile", Ent.Profile);
                command.Parameters.AddWithValue("@annualstdusage_single", Ent.AnnualStandardUsageSingle);
                C.SqlDataReader reader = command.ExecuteReader();

                StorageCredentials creds = new StorageCredentials(accountName, accountKey);
                CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);
                CloudTableClient client = account.CreateCloudTableClient();
                CloudTable table = client.GetTableReference("InterpolationOutput");

                TableBatchOperation batchOperation = new TableBatchOperation();

                while (reader.Read())
                {
                    InterpolationOutputEntity IpEntity = new InterpolationOutputEntity();
                    IpEntity.PartitionKey = Ent.PartitionKey.ToString();
                    IpEntity.RowKey = Guid.NewGuid().ToString();
                    IpEntity.Timestamp = DateTime.Now.ToString();
                    IpEntity.LDNHighPosition = reader[0].Equals(DBNull.Value) ? 0 : Convert.ToInt32(reader[0]);
                    IpEntity.LDNLowPosition = reader[1].Equals(DBNull.Value) ? 0 : Convert.ToInt32(reader[1]);
                    IpEntity.ReadingDateTime = Convert.ToDateTime(reader[2]).ToUniversalTime();
                    IpEntity.Profile = reader[3].ToString();
                    IpEntity.pfraction = float.Parse(reader[4].ToString());
                    IpEntity.Ispeak = Convert.ToInt16(reader[5]);
                    IpEntity.RecordType = reader[6].ToString();
                    IpEntity.MarketSegment = "Gas";
                    batchOperation.Insert(IpEntity);
                }
                table.ExecuteBatch(batchOperation);
            }
        }
        static public void SelectRowsGasReInt(C.SqlConnection connection, DateTime dtfrm, DateTime dtto, GasMeterReadingEntity Ent)
        {
            using (var command = new C.SqlCommand())
            {
                command.Connection = connection;
                command.CommandTimeout = 1000;
                command.CommandText = @"select * from dbo.fn_gas_reinterpolate(@RLeft, @RRight, @Profile, @deltaLDN)";
                command.Parameters.AddWithValue("@RLeft", dtfrm);
                command.Parameters.AddWithValue("@RRight", dtto);
                command.Parameters.AddWithValue("@Profile", Ent.Profile);
                command.Parameters.AddWithValue("@deltaLDN", Ent.DeltaLDNGasUsage);
                C.SqlDataReader reader = command.ExecuteReader();

                StorageCredentials creds = new StorageCredentials(accountName, accountKey);
                CloudStorageAccount account = new CloudStorageAccount(creds, useHttps: true);
                CloudTableClient client = account.CreateCloudTableClient();
                CloudTable table = client.GetTableReference("InterpolationOutput");

                TableBatchOperation batchOperation = new TableBatchOperation();

                while (reader.Read())
                {
                    InterpolationOutputEntity IpEntity = new InterpolationOutputEntity();
                    IpEntity.PartitionKey = Ent.PartitionKey.ToString();
                    IpEntity.RowKey = Guid.NewGuid().ToString();
                    IpEntity.Timestamp = DateTime.Now.ToString();
                    IpEntity.LDNHighPosition = reader[0].Equals(DBNull.Value) ? 0 : Convert.ToInt32(reader[0]);
                    IpEntity.LDNLowPosition = reader[1].Equals(DBNull.Value) ? 0 : Convert.ToInt32(reader[1]);
                    IpEntity.ReadingDateTime = Convert.ToDateTime(reader[2]).ToUniversalTime();
                    IpEntity.Profile = reader[3].ToString();
                    IpEntity.pfraction = float.Parse(reader[4].ToString());
                    IpEntity.Ispeak = Convert.ToInt16(reader[5]);
                    IpEntity.RecordType = reader[6].ToString();
                    IpEntity.MarketSegment = "Gas";
                    batchOperation.Insert(IpEntity);
                }
                table.ExecuteBatch(batchOperation);
            }
        }
    }
}