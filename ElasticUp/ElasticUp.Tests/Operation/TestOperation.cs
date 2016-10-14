using ElasticUp.Migration.Meta;
using ElasticUp.Operation;

namespace ElasticUp.Tests.Operation
{
    public class TestOperation : ElasticUpOperation
    {
        public TestOperation(int operationNumber) 
            : base(operationNumber) {}

        public override void Execute(string fromIndex, string toIndex)
        {
            // Magic goes here
        }
    }
}
