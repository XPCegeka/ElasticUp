using ElasticUp.Helper;
using Nest;
using static ElasticUp.Validations.IndexValidations;
using static ElasticUp.Validations.StringValidations;

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
            StringValidationsFor<CreateAliasOperation>()
                .IsNotBlank(Alias, RequiredMessage("Alias"))
                .IsNotBlank(IndexName, RequiredMessage("IndexName"));

            IndexValidationsFor<CreateAliasOperation>(elasticClient)
                .IndexExists(IndexName);
        }

        public override void Execute(IElasticClient elasticClient)
        {
            new AliasHelper(elasticClient).PutAliasOnIndex(Alias, IndexName);
        }
    }
}
