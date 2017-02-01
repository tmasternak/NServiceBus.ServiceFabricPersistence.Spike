using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Description;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Newtonsoft.Json.Linq;

namespace PersistenceService
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class PersistenceService : StatefulService
    {
        public PersistenceService(StatefulServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new[] { new ServiceReplicaListener(context => this.CreateInternalListener(context)) };
        }

        private ICommunicationListener CreateInternalListener(ServiceContext context)
        {
            // Partition replica's URL is the node's IP, port, PartitionId, ReplicaId, Guid
            EndpointResourceDescription internalEndpoint = context.CodePackageActivationContext.GetEndpoint("ServiceEndpoint");

            // Multiple replicas of this service may be hosted on the same machine,
            // so this address needs to be unique to the replica which is why we have partition ID + replica ID in the URL.
            // HttpListener can listen on multiple addresses on the same port as long as the URL prefix is unique.
            // The extra GUID is there for an advanced case where secondary replicas also listen for read-only requests.
            // When that's the case, we want to make sure that a new unique address is used when transitioning from primary to secondary
            // to force clients to re-resolve the address.
            // '+' is used as the address here so that the replica listens on all available hosts (IP, FQDM, localhost, etc.)

            string uriPrefix = String.Format(
                "{0}://+:{1}/{2}/{3}-{4}/",
                internalEndpoint.Protocol,
                internalEndpoint.Port,
                context.PartitionId,
                context.ReplicaOrInstanceId,
                Guid.NewGuid());

            string nodeIP = FabricRuntime.GetNodeContext().IPAddressOrFQDN;

            // The published URL is slightly different from the listening URL prefix.
            // The listening URL is given to HttpListener.
            // The published URL is the URL that is published to the Service Fabric Naming Service,
            // which is used for service discovery. Clients will ask for this address through that discovery service.
            // The address that clients get needs to have the actual IP or FQDN of the node in order to connect,
            // so we need to replace '+' with the node's IP or FQDN.
            string uriPublished = uriPrefix.Replace("+", nodeIP);
            return new HttpCommunicationListener(uriPrefix, uriPublished, this.ProcessInternalRequest);
        }

        private async Task ProcessInternalRequest(HttpListenerContext context, CancellationToken cancelRequest)
        {
            string output = null;
            string actionName = context.Request.QueryString["action"];

            try
            {
                if (actionName == "subscribe")
                {
                    string requestText;
                    using (var reader = new StreamReader(context.Request.InputStream))
                    {
                        requestText = await reader.ReadToEndAsync();
                    }

                    dynamic request = JObject.Parse(requestText);

                    await AddSubscription(
                        request.MessageType.TypeName.ToString(),
                        request.Subscrber.TransportAddress.ToString(),
                        request.Subscrber.ToString());
                }
                else if (actionName == "get-subscriptions")
                {

                }

            }
            catch (Exception ex)
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            }
        }

        async Task AddSubscription(string messageType, string transportAddress, string subscriber)
        {
            var dictionary = await StateManager.GetOrAddAsync<IReliableDictionary<string, ConcurrentDictionary<string, string>>>("subscriptions");

            using (ITransaction tx = this.StateManager.CreateTransaction())
            {
                await dictionary.AddOrUpdateAsync(
                    tx,
                    messageType,
                    _ =>
                    {
                        var value = new ConcurrentDictionary<string, string>();
                        value.TryAdd(transportAddress, subscriber);
                        return value;
                    },
                    (_, oldValue) =>
                    {
                        oldValue.AddOrUpdate(transportAddress, subscriber, (___, __) => subscriber);
                        return oldValue;
                    });

                await tx.CommitAsync();

            }
        }
    }
}
