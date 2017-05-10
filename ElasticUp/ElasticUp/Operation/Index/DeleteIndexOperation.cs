using Nest;
using static ElasticUp.Validations.IndexValidations;
using static ElasticUp.Validations.StringValidations;

namespace ElasticUp.Operation.Index
{
    public class DeleteIndexOperation : AbstractElasticUpOperation
    {
        protected string IndexName;

        public DeleteIndexOperation(string indexName)
        {
            IndexName = indexName?.ToLowerInvariant();
        }

        public override void Validate(IElasticClient elasticClient)
        {
            StringValidationsFor<DeleteIndexOperation>()
                .IsNotBlank(IndexName, RequiredMessage("IndexName"));

            IndexValidationsFor<DeleteIndexOperation>(elasticClient)
                .IndexExists(IndexName);
        }

        public override void Execute(IElasticClient elasticClient)
        {
            elasticClient.DeleteIndex(IndexName);
        }
    }
}
