using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Messages;
using NServiceBus;

namespace ExternalEndpoint
{
    public class SampleEventHandler : IHandleMessages<SampleEvent>
    {
        public Task Handle(SampleEvent message, IMessageHandlerContext context)
        {
            Console.WriteLine($"Received event with Id={message.Id}");

            return Task.FromResult(0);
        }
    }
}
