using Elasticsearch.Net;
using Nest;
using static ElasticUp.Validations.IndexValidations;
using static ElasticUp.Validations.StringValidations;

namespace ElasticUp.Operation.Mapping
{
    public class PutTypeMappingOperation : AbstractElasticUpOperation
    {
        protected string IndexName;
        protected string Type;
        protected string JsonMappingAsString;

        public PutTypeMappingOperation(string type)
        {
            Type = type?.ToLowerInvariant();
        }

        public virtual PutTypeMappingOperation OnIndex(string indexName)
        {
            IndexName = indexName?.ToLowerInvariant();
            return this;
        }

        public virtual PutTypeMappingOperation WithMapping(string jsonMappingAsString)
        {
            JsonMappingAsString = jsonMappingAsString;
            return this;
        }

        public override void Validate(IElasticClient elasticClient)
        {
            StringValidationsFor<PutTypeMappingOperation>()
                .IsNotBlank(Type, RequiredMessage("Type"))
                .IsNotBlank(IndexName, RequiredMessage("IndexName"))
                .IsNotBlank(JsonMappingAsString, RequiredMessage("JsonMappingAsString"));

            IndexValidationsFor<PutTypeMappingOperation>(elasticClient)
                .IndexExists(IndexName);
        }

        public override void Execute(IElasticClient elasticClient)
        {
            elasticClient.LowLevel.IndicesPutMapping<byte[]>(IndexName, Type, new PostData<object>(JsonMappingAsString));
        }
    }

    public class PutTypeMappingOperation<T> : PutTypeMappingOperation where T : class
    {
        public PutTypeMappingOperation() : base(typeof(T).Name.ToLowerInvariant()) {}
    }
}
