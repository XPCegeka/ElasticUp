using ElasticUp.Migration.Meta;

namespace ElasticUp.Operation
{
    public abstract class ElasticUpOperation
    {
        public int OperationNumber { get; private set; }

        protected ElasticUpOperation(int operationNumber)
        {
            OperationNumber = operationNumber;
        }

        public abstract void Execute(string fromIndex, string toIndex);
    }
}