using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    internal class FiredTriggerRepository : CosmosDbRepositoryBase<PersistentFiredTrigger>
    {
        public FiredTriggerRepository(Container container, string instanceName)
            : base(container, PersistentFiredTrigger.EntityType, instanceName)
        {
        }


        public Task<IList<PersistentFiredTrigger>> GetAllByJob(string jobName, string jobGroup)
        {
            return Task.FromResult<IList<PersistentFiredTrigger>>(_container
                .GetItemLinqQueryable<PersistentFiredTrigger>(true, null, GetRequestOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.JobGroup == jobGroup && x.JobName == jobName)
                .ToList());
        }

        public Task<IList<PersistentFiredTrigger>> GetAllRecoverableByInstanceId(string instanceId)
        {
            return Task.FromResult<IList<PersistentFiredTrigger>>(_container
                .GetItemLinqQueryable<PersistentFiredTrigger>(true, null, GetRequestOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.InstanceId == instanceId && x.RequestsRecovery)
                .ToList());
        }
        
        public Task<IList<PersistentFiredTrigger>> GetAllByInstanceId(string instanceId)
        {
            return Task.FromResult<IList<PersistentFiredTrigger>>(_container
                .GetItemLinqQueryable<PersistentFiredTrigger>(true, null, GetRequestOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.InstanceId == instanceId)
                .ToList());
        }

        /// <summary>
        /// Deletes FiredTrigger + 
        /// </summary>
        /// <param name="instanceId"></param>
        /// <returns></returns>
        public async Task<int> DeleteAllByInstanceId(string instanceId)
        {
            // We may introduce paging if performance boost is necessary
            
            var triggerIds = _container
                .GetItemLinqQueryable<PersistentFiredTrigger>(true, null, GetRequestOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.InstanceId == instanceId)
                .Select(x => x.Id)
                .ToList();

            foreach (var trigger in triggerIds)
            {
                await Delete(trigger);
            }
            
            return triggerIds.Count;
        }

        public Task<IList<PersistentFiredTrigger>> GetAllByTrigger(string triggerKeyName, string triggerKeyGroup)
        {
            return Task.FromResult<IList<PersistentFiredTrigger>>(_container
                .GetItemLinqQueryable<PersistentFiredTrigger>(true, null, GetRequestOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.TriggerGroup == triggerKeyGroup && (x.TriggerName == null || x.TriggerName == triggerKeyName))
                .ToList());
        }
    }
}