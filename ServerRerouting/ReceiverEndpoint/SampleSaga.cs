using System.Threading.Tasks;
using NServiceBus;
using Shared;

namespace ReceiverEndpoint
{
    public class SampleSaga : Saga<SampleSagaData>,
                              IAmStartedByMessages<MsgStartingSagaA>
    {
        protected override void ConfigureHowToFindSaga(SagaPropertyMapper<SampleSagaData> mapper)
        {
            mapper.ConfigureMapping<MsgStartingSagaA>(m => m.BusinessCorrelationId).ToSaga(s => s.BussinessId);
        }

        public Task Handle(MsgStartingSagaA message, IMessageHandlerContext context)
        {
            return Task.FromResult(0);
        }
    }

    public class SampleSagaData : ContainSagaData
    {
        public string BussinessId { get; set; }

        public int Counter { get; set; }
    }
}