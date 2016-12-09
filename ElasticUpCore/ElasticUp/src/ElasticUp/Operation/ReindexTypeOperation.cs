using Elasticsearch.Net;
using Nest;

namespace ElasticUp.Operation
{
    public class ReindexTypeOperation : ElasticUpOperation
    {
        public VersionType VersionType { get; set; } = VersionType.External;
        public string TypeName { get; set; }

        public ReindexTypeOperation WithTypeName(string typeName)
        {
            TypeName = typeName;
            return this;
        }

        public override void Execute(IElasticClient elasticClient, string fromIndex, string toIndex)
        {
            if (!elasticClient.IndexExists(toIndex).Exists)
                elasticClient.CreateIndex(toIndex);

            elasticClient.ReindexOnServer(descriptor => descriptor
                .Source(sourceDescriptor => sourceDescriptor
                    .Type(TypeName)
                    .Index(fromIndex))
                .Destination(destinationDescriptor => destinationDescriptor
                    .VersionType(VersionType)
                    .Index(toIndex))
                .WaitForCompletion());
        }
    }

    public class ReindexTypeOperation<T> : ReindexTypeOperation where T : class
    {
        public ReindexTypeOperation()
        {
            TypeName = typeof (T).Name.ToLowerInvariant();
        }
    }
}