using Nest;
using static ElasticUp.Validations.IndexValidations;
using static ElasticUp.Validations.StringValidations;

namespace ElasticUp.Operation.Mapping
{
    public class CopyTypeMappingOperation : AbstractElasticUpOperation
    {
        protected  string Type;
        protected string FromIndexName;
        protected string ToIndexName;

        public CopyTypeMappingOperation(string type)
        {
            Type = type;
        }

        public virtual CopyTypeMappingOperation FromIndex(string fromIndex)
        {
            FromIndexName = fromIndex?.ToLowerInvariant();
            return this;
        }

        public virtual CopyTypeMappingOperation ToIndex(string toIndex)
        {
            ToIndexName = toIndex?.ToLowerInvariant();
            return this;
        }

        public override void Validate(IElasticClient elasticClient)
        {
            StringValidationsFor<CopyTypeMappingOperation>()
                .IsNotBlank(Type, RequiredMessage("Type"))
                .IsNotBlank(FromIndexName, RequiredMessage("FromIndexName"))
                .IsNotBlank(ToIndexName, RequiredMessage("ToIndexName"));

            IndexValidationsFor<CopyTypeMappingOperation>(elasticClient)
                .IndexExists(FromIndexName)
                .IndexExists(ToIndexName);
        }

        public override void Execute(IElasticClient elasticClient)
        {
            var mapping = elasticClient.GetMapping(new GetMappingRequest(Indices.Parse(FromIndexName))).Mapping;
            
            elasticClient.Map(new PutMappingRequest(ToIndexName, Type)
            {
                Properties = mapping.Properties
            });
        }
    }
}
