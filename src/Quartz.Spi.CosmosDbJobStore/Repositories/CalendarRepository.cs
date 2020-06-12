using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    internal class CalendarRepository : CosmosDbRepositoryBase<PersistentCalendar>
    {
        public CalendarRepository(Container container, string instanceName)
            : base(container, PersistentCalendar.EntityType, instanceName)
        {
        }


        public Task<IReadOnlyCollection<string>> GetCalendarNames()
        {
            return Task.FromResult((IReadOnlyCollection<string>)_container.GetItemLinqQueryable<PersistentCalendar>(true, null, GetRequestOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .Select(x => x.CalendarName)
                .Distinct()
                .ToList());
        } 
    }
}