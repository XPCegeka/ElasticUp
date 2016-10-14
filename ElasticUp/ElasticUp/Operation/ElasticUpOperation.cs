using ElasticUp.Migration.Meta;
using Nest;

namespace ElasticUp.Operation
{
    public class ElasticUpOperation
    {
        public int OperationNumber { get; private set; }

        public ElasticUpOperation(int operationNumber)
        {
            OperationNumber = operationNumber;
        }

        public void Execute(IElasticClient elasticClient, string fromIndex, string toIndex)
        {
        }
    }
}