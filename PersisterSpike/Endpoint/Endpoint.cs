using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Endpoint.Persistence;
using Messages;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using NServiceBus;
using NServiceBus.Features;
using NServiceBus.Persistence;
using NServiceBus.Routing;
using AzureStorageQueueTransport = NServiceBus.AzureStorageQueueTransport;

namespace Endpoint
{
    /// <summary>
    /// An instance of this class is created for each service instance by the Service Fabric runtime.
    /// </summary>
    internal sealed class Endpoint : StatelessService
    {

        public Endpoint(StatelessServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (e.g., TCP, HTTP) for this service replica to handle client or user requests.
        /// </summary>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            return new ServiceInstanceListener[0];
        }

        /// <summary>
        /// This is the main entry point for your service instance.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service instance.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            long iterations = 0;

            try
            {
                var configuration = new EndpointConfiguration("Endpoint");

                configuration.UseTransport<AzureStorageQueueTransport>().ConnectionString(Configuration.ConnectionString);

                configuration.UsePersistence<ServiceFabricPersistence, StorageType.Subscriptions>();
                configuration.UsePersistence<InMemoryPersistence, StorageType.Timeouts>();
                configuration.UsePersistence<InMemoryPersistence, StorageType.Sagas>();
                configuration.UsePersistence<InMemoryPersistence, StorageType.GatewayDeduplication>();
                configuration.UsePersistence<InMemoryPersistence, StorageType.Outbox>();


                configuration.EnableFeature<MessageDrivenSubscriptions>();

                var endpoint = await NServiceBus.Endpoint.Start(configuration);
            }
            catch (Exception e)
            {

            }

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                ServiceEventSource.Current.ServiceMessage(this.Context, "Working-{0}", ++iterations);

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
    }
}
