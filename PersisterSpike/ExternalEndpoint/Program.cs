using System;
using System.Threading.Tasks;
using Messages;
using NServiceBus;
using NServiceBus.Features;
using AzureStorageQueueTransport = NServiceBus.AzureStorageQueueTransport;

namespace ExternalEndpoint
{
    class Program
    {
        static void Main(string[] args)
        {
            AsyncMain().GetAwaiter().GetResult();
        }

        static async Task AsyncMain()
        {
            var configuration = new EndpointConfiguration("ExternalEndpoint");

            configuration.UseTransport<AzureStorageQueueTransport>()
                .ConnectionString(Configuration.ConnectionString)
                .Routing().RegisterPublisher(typeof(SampleEvent), "Endpoint");

            configuration.UsePersistence<InMemoryPersistence>();

            configuration.EnableFeature<MessageDrivenSubscriptions>();

            var endpoint = await Endpoint.Start(configuration);

            Console.WriteLine("Subscriber started");
            Console.WriteLine("Press any <key> to exit...");
            Console.ReadKey();
        }
    }
}
