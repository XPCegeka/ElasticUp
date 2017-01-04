using ElasticUp.Alias;
using ElasticUp.Elastic;
using Nest;

namespace ElasticUp.Operation.Index
{
    public class CreateIndexOperation : AbstractElasticUpOperation
    {
        protected string IndexName;
        protected string Alias;
        protected string JsonMappingsAsString;
        protected bool? CreateWithoutMapping = null;

        public CreateIndexOperation(string indexName)
        {
            IndexName = indexName?.ToLowerInvariant();
        }

        public virtual CreateIndexOperation WithAlias(string alias)
        {
            Alias = alias?.ToLowerInvariant();
            return this;
        }

        public override void Execute(IElasticClient elasticClient)
        {
            if (string.IsNullOrWhiteSpace(IndexName)) throw new ElasticUpException($"CreateIndexOperation: Invalid indexName {IndexName}");
            if (!string.IsNullOrEmpty(Alias) && string.IsNullOrWhiteSpace(Alias)) throw new ElasticUpException($"CreateIndexOperation: Invalid alias {Alias}");
            if (elasticClient.IndexExists(IndexName).Exists) throw new ElasticUpException($"CreateIndexOperation: index {IndexName} already exists.");

            elasticClient.CreateIndex(IndexName);

            if (!string.IsNullOrWhiteSpace(Alias))
            {
                new AliasHelper(elasticClient).PutAliasOnIndex(Alias, IndexName);
            }
        }
    }
}
