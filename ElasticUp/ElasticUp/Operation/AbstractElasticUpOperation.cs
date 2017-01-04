using Nest;

namespace ElasticUp.Operation
{
    public abstract class AbstractElasticUpOperation
    {
        public abstract void Execute(IElasticClient elasticClient);

        public sealed override string ToString()
        {
            return GetType().Name.ToLowerInvariant();
        }
    }
}
