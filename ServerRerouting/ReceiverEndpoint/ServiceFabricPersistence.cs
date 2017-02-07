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
            var sagas = await GetSagasDictionary().ConfigureAwait(false);
            var secondaryIndex = await GetIndexDicitonary().ConfigureAwait(false);

            using (var tx = stateManager.CreateTransaction())
            {
                var correlationId = BuildKey(correlationProperty);

                var data = Serialize(sagaData);

                var result = await sagas.TryAddAsync(tx, correlationId, data);

                if (result == false)
                {
                    throw new Exception($"Failed to save new saga data for {correlationProperty.Name}='{correlationProperty.Value}'");
                }

                result = await secondaryIndex.TryAddAsync(tx, sagaData.Id, correlationId);

                if (result == false)
                {
                    throw new Exception($"Failed to save index for saga data for {correlationProperty.Name}='{correlationProperty.Value}'");
                }

                await tx.CommitAsync();
            }
        }

        public async Task Update(IContainSagaData sagaData, SynchronizedStorageSession session, ContextBag context)
        {
            var sagas = await GetSagasDictionary().ConfigureAwait(false);
            var secondaryIndex = await GetIndexDicitonary().ConfigureAwait(false);

            using (var tx = stateManager.CreateTransaction())
            {
                var sagaId = sagaData.Id;

                var correlationId = await GetCorrelationId(secondaryIndex, tx, sagaId).ConfigureAwait(false);

                var data = Serialize(sagaData);

                var previousData = context.Get<Metadata>().SagaData[sagaData.Id];

                var result = await sagas.TryUpdateAsync(tx, correlationId, data, previousData);

                if (result == false)
                {
                    throw new Exception($"Optimistic concurrency failure for SagaId='{sagaData.Id}' and CorrelationId='{sagaId}'");
                }

                await tx.CommitAsync();
            }
        }

        public async Task<TSagaData> Get<TSagaData>(Guid sagaId, SynchronizedStorageSession session, ContextBag context) where TSagaData : IContainSagaData
        {
            var sagas = await GetSagasDictionary().ConfigureAwait(false);
            var secondaryIndex = await GetIndexDicitonary().ConfigureAwait(false);

            using (var tx = stateManager.CreateTransaction())
            {
                var correlationId = await secondaryIndex.TryGetValueAsync(tx, sagaId, LockMode.Default).ConfigureAwait(false);

                if (correlationId.HasValue == false)
                {
                    return default(TSagaData);
                }

                var sagaData = await sagas.TryGetValueAsync(tx, correlationId.Value, LockMode.Default).ConfigureAwait(false);
                if (sagaData.HasValue == false)
                {
                    throw new Exception($"Failed to update saga data for SagaId='{sagaId}' and CorrelationId='{correlationId.Value}'");
                }

                return Resolve<TSagaData>(sagaData.Value, context);
            }
        }

        public async Task<TSagaData> Get<TSagaData>(string propertyName, object propertyValue, SynchronizedStorageSession session, ContextBag context)
            where TSagaData : IContainSagaData
        {
            var dictionary = await GetSagasDictionary().ConfigureAwait(false);

            using (var tx = stateManager.CreateTransaction())
            {
                var correlationId = BuildKey(new SagaCorrelationProperty(propertyName, propertyValue));

                var result = await dictionary.TryGetValueAsync(tx, correlationId, LockMode.Default).ConfigureAwait(false);

                return result.HasValue ? Resolve<TSagaData>(result.Value, context) : default(TSagaData);
            }
        }

        public async Task Complete(IContainSagaData sagaData, SynchronizedStorageSession session, ContextBag context)
        {
            var sagas = await GetSagasDictionary().ConfigureAwait(false);
            var secondaryIndex = await GetIndexDicitonary().ConfigureAwait(false);

            using (var tx = stateManager.CreateTransaction())
            {
                var sagaId = sagaData.Id;
                var correlationId = await GetCorrelationId(secondaryIndex, tx, sagaId).ConfigureAwait(false);

                await secondaryIndex.TryRemoveAsync(tx, sagaId).ConfigureAwait(false);

                var previouslyExistingState = await sagas.TryRemoveAsync(tx, correlationId).ConfigureAwait(false);
                if (previouslyExistingState.HasValue == false)
                {
                    throw new Exception($"Failed to complete saga data for SagaId='{sagaId}' and CorrelationId='{correlationId}'");
                }

                var dataAtReading = context.Get<Metadata>().SagaData[sagaId];
                var dataAtDeletion = previouslyExistingState.Value;

                if (Compare(dataAtReading, dataAtDeletion) == false)
                {
                    throw new Exception($"Optimistic concurrency failure for SagaId='{sagaId}' and CorrelationId='{correlationId}'");
                }

                await tx.CommitAsync().ConfigureAwait(false);
            }
        }

        // ReSharper disable once SuggestBaseTypeForParameter
        static bool Compare(byte[] a1, byte[] a2)
        {
            if (ReferenceEquals(a1, a2))
                return true;

            if (a1 == null || a2 == null)
                return false;

            if (a1.Length != a2.Length)
                return false;

            for (var i = 0; i < a1.Length; i++)
            {
                if (a1[i] != a2[i])
                    return false;
            }

            return true;
        }

        static async Task<string> GetCorrelationId(IReliableDictionary<Guid, string> secondaryIndex, ITransaction tx, Guid indexKey)
        {
            var correlationIdValue = await secondaryIndex.TryGetValueAsync(tx, indexKey, LockMode.Default).ConfigureAwait(false);

            if (correlationIdValue.HasValue == false)
            {
                throw new Exception($"Can't look up saga by its SagaId='{indexKey}'");
            }

            return correlationIdValue.Value;
        }

        static TSagaData Resolve<TSagaData>(byte[] data, ContextBag context)
            where TSagaData : IContainSagaData
        {
            var serializer = new DataContractSerializer(typeof(TSagaData));
            using (var ms = new MemoryStream(data))
            {
                var saga = (TSagaData)serializer.ReadObject(ms);

                context.GetOrCreate<Metadata>().SagaData[saga.Id] = data;

                return saga;
            }
        }

        static string BuildKey(SagaCorrelationProperty correlationProperty)
        {
            return $"{correlationProperty.Name}_{correlationProperty.Value}";
        }

        Task<IReliableDictionary<string, byte[]>> GetSagasDictionary()
        {
            return stateManager.GetOrAddAsync<IReliableDictionary<string, byte[]>>("sagas");
        }

        Task<IReliableDictionary<Guid, string>> GetIndexDicitonary()
        {
            return stateManager.GetOrAddAsync<IReliableDictionary<Guid, string>>("sagas-index");
        }

        static byte[] Serialize(IContainSagaData sagaData)
        {
            var serializer = new DataContractSerializer(sagaData.GetType());
            using (var ms = new MemoryStream())
            {
                serializer.WriteObject(ms, sagaData);
                return ms.ToArray();
            }
        }

        readonly IReliableStateManager stateManager;

        class Metadata
        {
            public readonly Dictionary<Guid, byte[]> SagaData = new Dictionary<Guid, byte[]>();
        }
    }
}