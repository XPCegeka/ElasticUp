using Nest;

namespace ElasticUp.Operation
{
    public abstract class ElasticUpOperation
    {
        public abstract void Execute(IElasticClient elasticClient, string fromIndex, string toIndex);
    }
}