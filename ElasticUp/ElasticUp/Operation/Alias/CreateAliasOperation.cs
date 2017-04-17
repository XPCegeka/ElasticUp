using ElasticUp.Alias;
using ElasticUp.Elastic;
using Nest;

namespace ElasticUp.Operation.Alias
{
    public class CreateAliasOperation : AbstractElasticUpOperation
    {
        protected string Alias;
        protected string IndexName;

        public CreateAliasOperation(string alias)
        {
            Alias = alias?.ToLowerInvariant();
        }

        public virtual CreateAliasOperation OnIndex(string indexName)
        {
            IndexName = indexName?.ToLowerInvariant();
            return this;
        }

        public override void Validate(IElasticClient elasticClient)
        {
            if (string.IsNullOrWhiteSpace(Alias)) throw new ElasticUpException($"CreateAliasOperation: Invalid alias {Alias}");
            if (string.IsNullOrWhiteSpace(IndexName)) throw new ElasticUpException($"CreateAliasOperation: Invalid indexName {IndexName}");
            if (!elasticClient.IndexExists(IndexName).Exists) throw new ElasticUpException($"CreateAliasOperation: index {IndexName} does not exist.");
        }

        public override void Execute(IElasticClient elasticClient)
        {
            new AliasHelper(elasticClient).PutAliasOnIndex(Alias, IndexName);
        }
    }
}
