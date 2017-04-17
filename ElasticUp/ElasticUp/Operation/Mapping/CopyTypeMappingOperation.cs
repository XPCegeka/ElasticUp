using ElasticUp.Elastic;
using Nest;

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
            if (string.IsNullOrWhiteSpace(Type)) throw new ElasticUpException($"CopyMappingOperation: Invalid Type {Type}");
            if (string.IsNullOrWhiteSpace(FromIndexName)) throw new ElasticUpException($"CopyMappingOperation: Invalid indexName {FromIndexName}");
            if (string.IsNullOrWhiteSpace(ToIndexName)) throw new ElasticUpException($"CopyMappingOperation: Invalid indexName {ToIndexName}");
            if (!elasticClient.IndexExists(FromIndexName).Exists) throw new ElasticUpException($"CopyMappingOperation: fromIndex {FromIndexName} does not exist.");
            if (!elasticClient.IndexExists(ToIndexName).Exists) throw new ElasticUpException($"CopyMappingOperation: toIndex {ToIndexName} does not exist.");
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
