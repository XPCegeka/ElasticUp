using ElasticUp.Helper;
using Nest;
using static ElasticUp.Validations.IndexValidations;
using static ElasticUp.Validations.StringValidations;

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
            StringValidationsFor<SwitchAliasOperation>()
                .IsNotBlank(Alias, RequiredMessage("Alias"))
                .IsNotBlank(FromIndexName, RequiredMessage("FromIndexName"))
                .IsNotBlank(ToIndexName, RequiredMessage("ToIndexName"));

            IndexValidationsFor<SwitchAliasOperation>(elasticClient)
                .IndexExists(FromIndexName)
                .IndexExists(ToIndexName);
        }

        public override void Execute(IElasticClient elasticClient)
        {
            new AliasHelper(elasticClient).SwitchAlias(Alias, FromIndexName, ToIndexName);
        }
    }
}
