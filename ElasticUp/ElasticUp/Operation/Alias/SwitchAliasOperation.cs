using ElasticUp.Alias;
using ElasticUp.Elastic;
using Nest;

namespace ElasticUp.Operation.Alias
{
    public class SwitchAliasOperation : AbstractElasticUpOperation
    {
        protected string Alias;
        protected string FromIndexName;
        protected string ToIndexName;

        public SwitchAliasOperation(string alias)
        {
            Alias = alias?.ToLowerInvariant();
        }

        public virtual SwitchAliasOperation FromIndex(string fromIndexName)
        {
            FromIndexName = fromIndexName?.ToLowerInvariant();
            return this;
        }

        public virtual SwitchAliasOperation ToIndex(string toIndexName)
        {
            ToIndexName = toIndexName?.ToLowerInvariant();
            return this;
        }

        public override void Validate(IElasticClient elasticClient)
        {
            if (string.IsNullOrWhiteSpace(Alias)) throw new ElasticUpException($"SwitchAliasOperation: Invalid alias {Alias}");
            if (string.IsNullOrWhiteSpace(FromIndexName)) throw new ElasticUpException($"SwitchAliasOperation: Invalid fromIndexName {FromIndexName}");
            if (string.IsNullOrWhiteSpace(ToIndexName)) throw new ElasticUpException($"SwitchAliasOperation: Invalid toIndexName {ToIndexName}");
            if (!elasticClient.IndexExists(FromIndexName).Exists) throw new ElasticUpException($"SwitchAliasOperation: fromIndex {FromIndexName} does not exist.");
            if (!elasticClient.IndexExists(ToIndexName).Exists) throw new ElasticUpException($"SwitchAliasOperation: toIndex {ToIndexName} does not exist.");
        }

        public override void Execute(IElasticClient elasticClient)
        {
            new AliasHelper(elasticClient).SwitchAlias(Alias, FromIndexName, ToIndexName);
        }
    }
}
