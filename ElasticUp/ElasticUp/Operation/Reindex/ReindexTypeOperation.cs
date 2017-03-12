using System;
using Elasticsearch.Net;
using ElasticUp.Elastic;
using Nest;

namespace ElasticUp.Operation.Reindex
{
    public class ReindexTypeOperation : AbstractElasticUpOperation
    {
        protected string FromIndexName;
        protected string ToIndexName;
        protected VersionType VersionType = VersionType.External;
        public string TypeName;
        protected string Script;

        public ReindexTypeOperation(string typeName)
        {
            TypeName = typeName?.ToLowerInvariant();
        }

        public virtual ReindexTypeOperation FromIndex(string fromIndex)
        {
            FromIndexName = fromIndex?.ToLowerInvariant();
            return this;
        }

        public virtual ReindexTypeOperation ToIndex(string toIndex)
        {
            ToIndexName = toIndex?.ToLowerInvariant();
            return this;
        }

        public virtual ReindexTypeOperation WithInlineScript(string script)
        {
            Script = script;
            return this;
        }

        public override void Execute(IElasticClient elasticClient)
        {
            if (string.IsNullOrWhiteSpace(FromIndexName)) throw new ElasticUpException($"ReindexTypeOperation: Invalid fromIndexName {FromIndexName}");
            if (string.IsNullOrWhiteSpace(ToIndexName)) throw new ElasticUpException($"ReindexTypeOperation: Invalid toIndexName {ToIndexName}");
            if (string.IsNullOrWhiteSpace(TypeName)) throw new ElasticUpException($"ReindexTypeOperation: Invalid type {TypeName}");
            if (!elasticClient.IndexExists(FromIndexName).Exists) throw new ElasticUpException($"ReindexTypeOperation: Invalid fromIndex {FromIndexName} does not exist.");
            if (!elasticClient.IndexExists(ToIndexName).Exists) throw new ElasticUpException($"ReindexTypeOperation: Invalid toIndex {ToIndexName} does not exist."); 

            var response = elasticClient.ReindexOnServer(descriptor =>
            {
                descriptor
                    .Source(sourceDescriptor => sourceDescriptor
                        .Type(TypeName)
                        .Index(FromIndexName))
                    .Destination(destinationDescriptor => destinationDescriptor
                        .VersionType(VersionType)
                        .Index(ToIndexName))
                    .WaitForCompletion();

                if (!string.IsNullOrWhiteSpace(Script))
                    descriptor.Script(scriptDescriptor => scriptDescriptor.Inline(Script));

                return descriptor;
            });
        }
    }

    public class ReindexTypeOperation<T> : ReindexTypeOperation where T : class
    {
        public ReindexTypeOperation() : base(typeof(T).Name.ToLowerInvariant()) {}
    }
}