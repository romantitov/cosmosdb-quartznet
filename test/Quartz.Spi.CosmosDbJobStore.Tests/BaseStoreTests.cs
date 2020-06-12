using System;
using System.Collections.Specialized;
using System.Threading.Tasks;

namespace Quartz.Spi.CosmosDbJobStore.Tests
{
    public abstract class BaseStoreTests
    {
        public const string Barrier = "BARRIER";
        public const string DateStamps = "DATE_STAMPS";
        public static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(125);

        protected static Task<IScheduler> CreateScheduler(string instanceName = "QUARTZ_TEST")
        {
            var properties = new NameValueCollection
            {
                [$"{HackedStdSchedulerFactory.PropertyObjectSerializer}.type"] = "json",
                [HackedStdSchedulerFactory.PropertySchedulerInstanceName] = instanceName,
                [HackedStdSchedulerFactory.PropertySchedulerInstanceId] = $"{Environment.MachineName}-{Guid.NewGuid()}",
                [HackedStdSchedulerFactory.PropertyJobStoreType] = typeof(CosmosDbJobStore).AssemblyQualifiedName,
                [$"{HackedStdSchedulerFactory.PropertyJobStorePrefix}.Endpoint"] = "https://notificationsdev.documents.azure.com:443/",
                [$"{HackedStdSchedulerFactory.PropertyJobStorePrefix}.Key"] = "Nl0t3U9hGO57Zf271Vew7k7cFwjDNiWytrf5IWurK2tR6t9JFTIbD8VXehOoQCvoOIYZ9ukQOm2MmTSfHpqhFg==",
                [$"{HackedStdSchedulerFactory.PropertyJobStorePrefix}.DatabaseId"] = "notificationservice",
                [$"{HackedStdSchedulerFactory.PropertyJobStorePrefix}.CollectionId"] = "Quartzzz",
                [$"{HackedStdSchedulerFactory.PropertyJobStorePrefix}.Clustered"] = "true"
            };

            var scheduler = new HackedStdSchedulerFactory(properties);
            return scheduler.GetScheduler();
        }
    }
}