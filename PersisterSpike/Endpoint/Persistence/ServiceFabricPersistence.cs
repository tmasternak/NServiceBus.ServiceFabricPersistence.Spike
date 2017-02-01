using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Client;
using Newtonsoft.Json.Linq;
using NServiceBus;
using NServiceBus.Extensibility;
using NServiceBus.Features;
using NServiceBus.Persistence;
using NServiceBus.Unicast.Subscriptions;
using NServiceBus.Unicast.Subscriptions.MessageDrivenSubscriptions;

namespace Endpoint.Persistence
{
    public class ServiceFabricPersistence : PersistenceDefinition
    {
        internal ServiceFabricPersistence()
        {
            Supports<StorageType.Subscriptions>(s => s.EnableFeatureByDefault<ServiceFabricSubscriptionPersistence>());
        }
    }

    internal class ServiceFabricSubscriptionPersistence : Feature
    {
        protected override void Setup(FeatureConfigurationContext context)
        {
            context.Container.ConfigureComponent<ServiceFabricSubscriptionStorage>(DependencyLifecycle.SingleInstance);
        }
    }

    internal class ServiceFabricSubscriptionStorage : ISubscriptionStorage
    {
        async Task<string> GetPrimaryReplicaAddress(MessageType messageType)
        {
            // The partitioning scheme of the processing service is a range of integers from 0 - 25.
            // This generates a partition key within that range by converting the first letter of the input name
            // into its numerica position in the alphabet.
            var partitionKey = new ServicePartitionKey(Char.ToUpper(messageType.TypeName.First()) - 'A');

            // This contacts the Service Fabric Naming Services to get the addresses of the replicas of the processing service
            // for the partition with the partition key generated above.
            // Note that this gets the most current addresses of the partition's replicas,
            // however it is possible that the replicas have moved between the time this call is made and the time that the address is actually used
            // a few lines below.
            // For a complete solution, a retry mechanism is required.
            // For more information, see http://aka.ms/servicefabricservicecommunication
            var partition =
                await servicePartitionResolver.ResolveAsync(persistenceServiceUri, partitionKey, CancellationToken.None);

            var endpoint = partition.GetEndpoint();

            var addresses = JObject.Parse(endpoint.Address);
            var primaryReplicaAddress = (string) addresses["Endpoints"].First();
            return primaryReplicaAddress;
        }

        public async Task Subscribe(Subscriber subscriber, MessageType messageType, ContextBag context)
        {
            var primaryReplicaAddress = await GetPrimaryReplicaAddress(messageType);

            var primaryReplicaUriBuilder = new UriBuilder(primaryReplicaAddress);
            primaryReplicaUriBuilder.Query = "action=subscribe";

            var requestText = Newtonsoft.Json.JsonConvert.SerializeObject(new { Subscrber = subscriber, MessageType = messageType });

            var result = await new HttpClient().PostAsync(primaryReplicaUriBuilder.Uri, new StringContent(requestText)).ConfigureAwait(false);
        }

        public async Task Unsubscribe(Subscriber subscriber, MessageType messageType, ContextBag context)
        {
           throw new NotImplementedException();
        }

        public async Task<IEnumerable<Subscriber>> GetSubscriberAddressesForMessage(IEnumerable<MessageType> messageTypes, ContextBag context)
        {
            //TODO: iterate through all messageTypes
            var messageType = messageTypes.First();

            var primaryReplicaAddress = await GetPrimaryReplicaAddress(messageType);

            var primaryReplicaUriBuilder = new UriBuilder(primaryReplicaAddress);
            primaryReplicaUriBuilder.Query = "action=get-subscriptions";

            var requestText = Newtonsoft.Json.JsonConvert.SerializeObject(new { MessageType = messageType });

            var response = await new HttpClient().PostAsync(primaryReplicaUriBuilder.Uri, new StringContent(requestText));

            //TODO: parse the responses here
            throw new NotImplementedException();
        }

        ServicePartitionResolver servicePartitionResolver = ServicePartitionResolver.GetDefault();
        Uri persistenceServiceUri = new Uri(@"fabric:/PersisterSpike/PersistenceService");
    }
}