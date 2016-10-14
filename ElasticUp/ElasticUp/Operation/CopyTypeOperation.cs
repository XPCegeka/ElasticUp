using ElasticUp.History;
using Nest;

namespace ElasticUp.Operation
{
    public class CopyTypeOperation<T> : ElasticUpOperation where T : class
    {
        public CopyTypeOperation(int operationNumber) : base(operationNumber)
        {
        }

        public override void Execute(IElasticClient elasticClient, string fromIndex, string toIndex)
        {
            var searchResponse = elasticClient.Search<T>(s => s
                .Index(fromIndex)
                .Type<T>());

            elasticClient.IndexMany(searchResponse.Documents, toIndex);
        }
    }
}