using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Persistence;
using Shared;

namespace SenderEndpoint
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync().GetAwaiter().GetResult();
        }

        static async Task MainAsync()
        {
            var configuration = new EndpointConfiguration("AAA-SenderEndpoint");

            configuration.UseTransport<AzureStorageQueueTransport>()
                         .ConnectionString(Configuration.ConnectionString)
                         .Routing().RouteToEndpoint(typeof(MsgStartingSagaA), "AAA-ReceiverEndpoint");

            configuration.UsePersistence<InMemoryPersistence>();

            configuration.SendFailedMessagesTo("error");

            var endpoint = await Endpoint.Start(configuration);

            while (true)
            {
                Console.WriteLine("Press any <key> to send saga start msg...");
                Console.ReadKey();

                await endpoint.Send(new MsgStartingSagaA { BusinessCorrelationId = "this-is-id" });

                Console.WriteLine("Mesage sent.");

            }
        }
    }
}
