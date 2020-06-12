using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    internal class JobRepository : CosmosDbRepositoryBase<PersistentJob>
    {
        public JobRepository(Container container, string instanceName)
            : base(container, PersistentJob.EntityType, instanceName)
        {
        }


        public Task<IReadOnlyCollection<string>> GetGroups()
        {
            return Task.FromResult<IReadOnlyCollection<string>>(_container.GetItemLinqQueryable<PersistentJob>(true, null, GetRequestOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .Select(x => x.Group)
                .Distinct()
                .ToList());
        }
    }
}