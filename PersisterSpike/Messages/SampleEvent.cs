using System;
using NServiceBus;

namespace Messages
{
    public class SampleEvent : IEvent
    {
        public Guid Id { get; set; }
    }
}
