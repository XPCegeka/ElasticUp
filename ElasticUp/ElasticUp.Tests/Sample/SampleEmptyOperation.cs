using ElasticUp.Operation;
using Nest;

namespace ElasticUp.Tests.Sample
{
    public class SampleEmptyOperation : ElasticUpOperation
    {
        public SampleEmptyOperation(int operationNumber) : base(operationNumber) {}

        public override void Execute(IElasticClient elasticClient, string fromIndex, string toIndex) {}
    }
}