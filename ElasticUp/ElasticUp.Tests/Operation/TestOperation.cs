using ElasticUp.Operation;
using Nest;

namespace ElasticUp.Tests.Operation
{
    public class TestOperation : ElasticUpOperation
    {
        public TestOperation(int operationNumber) : base(operationNumber)
        {
        }

        public override void Execute(IElasticClient elasticClient, string fromIndex, string toIndex)
        {
        }
    }
}