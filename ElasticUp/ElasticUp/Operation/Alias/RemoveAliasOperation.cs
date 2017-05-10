using ElasticUp.Helper;
using Nest;
using static ElasticUp.Validations.IndexValidations;
using static ElasticUp.Validations.StringValidations;

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
            StringValidationsFor<RemoveAliasOperation>()
                .IsNotBlank(Alias, RequiredMessage("Alias"))
                .IsNotBlank(IndexName, RequiredMessage("IndexName"));

            IndexValidationsFor<RemoveAliasOperation>(elasticClient)
                .IndexExists(IndexName);
        }

        public override void Execute(IElasticClient elasticClient)
        {
            new AliasHelper(elasticClient).RemoveAliasFromIndex(Alias, IndexName);
        }
    }
}
