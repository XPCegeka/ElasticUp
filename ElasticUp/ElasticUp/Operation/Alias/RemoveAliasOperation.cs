using ElasticUp.Alias;
using ElasticUp.Elastic;
using Nest;

namespace ElasticUp.Operation.Alias
{
    public class RemoveAliasOperation : AbstractElasticUpOperation
    {
        protected string Alias;
        protected string IndexName;

        public RemoveAliasOperation(string alias)
        {
            Alias = alias?.ToLowerInvariant();
        }

        public virtual RemoveAliasOperation FromIndex(string indexName)
        {
            IndexName = indexName?.ToLowerInvariant();
            return this;
        }

        public override void Validate(IElasticClient elasticClient)
        {
            if (string.IsNullOrWhiteSpace(Alias)) throw new ElasticUpException($"RemoveAliasOperation: Invalid alias {Alias}");
            if (string.IsNullOrWhiteSpace(IndexName)) throw new ElasticUpException($"RemoveAliasOperation: Invalid indexName {IndexName}");
            if (!elasticClient.IndexExists(IndexName).Exists) throw new ElasticUpException($"RemoveAliasOperation: index {IndexName} does not exist.");
        }

        public override void Execute(IElasticClient elasticClient)
        {
            new AliasHelper(elasticClient).RemoveAliasFromIndex(Alias, IndexName);
        }
    }
}
