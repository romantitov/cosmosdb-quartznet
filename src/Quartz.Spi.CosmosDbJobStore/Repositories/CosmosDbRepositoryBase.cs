using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Common.Logging;
using Microsoft.Azure.Cosmos;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    public abstract class CosmosDbRepositoryBase<TEntity> where TEntity : QuartzEntityBase
    {
        protected static readonly ILog _logger = LogManager.GetLogger<CosmosDbRepositoryBase<TEntity>>();
        
        protected readonly string _type;
        protected readonly Container _container;
        protected readonly string _instanceName;


        protected CosmosDbRepositoryBase(Container container, string type, string instanceName)
        {
            _instanceName = instanceName;
            _container = container;
            _type = type;
        }

        
        public async Task<TEntity> Get(string id)
        {
            try
            {
                return await _container.ReadItemAsync<TEntity>(id, GetPartitionKey());
            }
            catch (CosmosException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }
        }

        public async Task<bool> Exists(string id)
        {
            try
            {
                var r = await _container.ReadItemStreamAsync(id, GetPartitionKey());
                return r.IsSuccessStatusCode;
            }
            catch (CosmosException e) when(e.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
        }

        public Task Update(TEntity entity)
        {
            return _container.UpsertItemAsync(entity, GetPartitionKey());
        }

        public Task Save(TEntity entity)
        {
            return _container.CreateItemAsync(entity, GetPartitionKey());
        }

        public async Task<bool> Delete(string id)
        {
            try
            {
                await _container.DeleteItemAsync<TEntity>(id, GetPartitionKey());
                return true;
            }
            catch (CosmosException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
        }
        
        public Task<int> Count()
        {
            return Task.FromResult(_container.GetItemLinqQueryable<TEntity>(true, null, GetRequestOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName).Count());
        }

        public Task<IList<TEntity>> GetAll()
        {
            return Task.FromResult((IList<TEntity>)_container.GetItemLinqQueryable<TEntity>(true, null, GetRequestOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName).ToList());

        }
        
        public async Task DeleteAll()
        {
            var all = _container.GetItemLinqQueryable<TEntity>(true, null, GetRequestOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .Select(x => x.Id);

            foreach (var id in all)
            {
                await Delete(id);
            }
        }

        
        protected QueryRequestOptions GetRequestOptions()
        {
            return new QueryRequestOptions { PartitionKey = GetPartitionKey() };
        }
        
        protected PartitionKey GetPartitionKey()
        {
            return new PartitionKey(_instanceName);
        }
    }
}