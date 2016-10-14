using ElasticUp.Migration.Meta;
using Nest;

namespace ElasticUp.Operation
{
    public abstract class ElasticUpOperation
    {
        public int OperationNumber { get; private set; }

        protected ElasticUpOperation(int operationNumber)
        {
            OperationNumber = operationNumber;
        }

        public abstract void Execute(IElasticClient elasticClient, string fromIndex, string toIndex);
    }
}