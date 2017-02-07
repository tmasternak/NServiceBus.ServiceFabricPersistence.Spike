using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using NServiceBus;
using NServiceBus.Extensibility;
using NServiceBus.Features;
using NServiceBus.Persistence;
using NServiceBus.Sagas;
using NServiceBus.Unicast.Subscriptions;
using NServiceBus.Unicast.Subscriptions.MessageDrivenSubscriptions;

namespace ReceiverEndpoint
{
    public class ServiceFabricPersistence : PersistenceDefinition
    {
        internal ServiceFabricPersistence()
        {
            Supports<StorageType.Sagas>(s => s.EnableFeatureByDefault<ServiceFabricSagasPersistence>());
        }
    }

    internal class ServiceFabricSagasPersistence : Feature
    {
        protected override void Setup(FeatureConfigurationContext context)
        {
            context.Container.ConfigureComponent<ServiceFabricSagaPersister>(DependencyLifecycle.SingleInstance);
        }
    }

    internal class ServiceFabricSagaPersister : ISagaPersister
    {
        public ServiceFabricSagaPersister(IReliableStateManager stateManager)
        {
            this.stateManager = stateManager;
        }

        public async Task Save(IContainSagaData sagaData, SagaCorrelationProperty correlationProperty, SynchronizedStorageSession session,
            ContextBag context)
        {
            var dictionary = await stateManager.GetOrAddAsync<IReliableDictionary<String, byte[]>>("sagas");
            var secondaryIndex = await stateManager.GetOrAddAsync<IReliableDictionary<Guid, string>>("sagas-index");

            using (ITransaction tx = stateManager.CreateTransaction())
            {
                var key = $"{correlationProperty.Name}_{correlationProperty.Value}";

                var serializer = new DataContractSerializer(sagaData.GetType());

                using (var memoryStream = new MemoryStream())
                {
                    serializer.WriteObject(memoryStream, sagaData);

                    var data = memoryStream.ToArray();

                    var result = await dictionary.TryAddAsync(tx, key, data);

                    if (result == false)
                    {
                        throw new Exception($"Failed to save new saga data for {correlationProperty.Name}={correlationProperty.Value}");
                    }

                    result = await secondaryIndex.TryAddAsync(tx, sagaData.Id, key);

                    if (result == false)
                    {
                        throw new Exception($"Failed to save index for saga data for {correlationProperty.Name}={correlationProperty.Value}");
                    }

                    await tx.CommitAsync();
                }
            }
        }

        public async Task Update(IContainSagaData sagaData, SynchronizedStorageSession session, ContextBag context)
        {
            var mainData = await stateManager.GetOrAddAsync<IReliableDictionary<String, byte[]>>("sagas");
            var secondaryIndex = await stateManager.GetOrAddAsync<IReliableDictionary<Guid, string>>("sagas-index");

            using (ITransaction tx = stateManager.CreateTransaction())
            {
                var indexKey = sagaData.Id;

                var correlationIdValue = secondaryIndex.TryGetValueAsync(tx, indexKey, LockMode.Default);

                if (correlationIdValue.Result.HasValue == false)
                {
                    throw new Exception("");
                }

                var correlationId = correlationIdValue.Result.Value;

                var serializer = new DataContractSerializer(sagaData.GetType());

                using (var memoryStream = new MemoryStream())
                {
                    serializer.WriteObject(memoryStream, sagaData);

                    var data = memoryStream.ToArray();

                    var result = await mainData.TryUpdateAsync(tx, correlationId, data, null);

                    if (result == false)
                    {
                        throw new Exception($"Failed to update new saga data for sagaId={correlationId}");
                    }

                    await tx.CommitAsync();
                }
            }
        }

        public Task<TSagaData> Get<TSagaData>(Guid sagaId, SynchronizedStorageSession session, ContextBag context) where TSagaData : IContainSagaData
        {
            throw new NotImplementedException();
        }

        public async Task<TSagaData> Get<TSagaData>(string propertyName, object propertyValue, SynchronizedStorageSession session, ContextBag context) where TSagaData : IContainSagaData
        {
            var dictionary = await stateManager.GetOrAddAsync<IReliableDictionary<String, byte[]>>("sagas");

            using (ITransaction tx = stateManager.CreateTransaction())
            {
                var key = $"{propertyName}_{propertyValue}";

                var result = await dictionary.TryGetValueAsync(tx, key, LockMode.Default);

                if (result.HasValue)
                {
                    var serializer = new DataContractSerializer(typeof(TSagaData));

                    var sagaData = (TSagaData) serializer.ReadObject(new MemoryStream(result.Value));

                    await tx.CommitAsync();

                    return sagaData;
                }

                return default(TSagaData);
            }
        }

        public Task Complete(IContainSagaData sagaData, SynchronizedStorageSession session, ContextBag context)
        {
            throw new NotImplementedException();
        }

        IReliableStateManager stateManager;
    }
}