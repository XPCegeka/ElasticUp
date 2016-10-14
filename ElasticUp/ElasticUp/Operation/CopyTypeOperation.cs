using ElasticUp.History;
using Nest;

namespace ElasticUp.Operation
{
    public class CopyTypeOperation<T> : ElasticUpOperation where T : class
    {
        public CopyTypeOperation(int operationNumber) : base(operationNumber)
        {
        }

        public new void Execute(IElasticClient elasticClient, string fromIndex, string toIndex)
        {
            var docs = elasticClient.Search<T>(s => s
                .Index(fromIndex)
                .Type<T>()).Documents;

            elasticClient.IndexMany(docs, toIndex);
        }
    }
}