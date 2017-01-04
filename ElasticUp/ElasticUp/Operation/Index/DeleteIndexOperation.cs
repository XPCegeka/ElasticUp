using ElasticUp.Elastic;
using Nest;

namespace ElasticUp.Operation.Index
{
    public class DeleteIndexOperation : AbstractElasticUpOperation
    {
        protected string IndexName;

        public DeleteIndexOperation(string indexName)
        {
            IndexName = indexName?.ToLowerInvariant();
        }

        public override void Execute(IElasticClient elasticClient)
        {
            if (string.IsNullOrWhiteSpace(IndexName)) throw new ElasticUpException($"DeleteIndexOperation: Invalid indexName {IndexName}");
            if (!elasticClient.IndexExists(IndexName).Exists) throw new ElasticUpException($"DeleteIndexOperation: index {IndexName} does not exist.");

            elasticClient.DeleteIndex(IndexName);
        }
    }
}
