using Elasticsearch.Net;
using Nest;
using static ElasticUp.Validations.IndexValidations;
using static ElasticUp.Validations.StringValidations;

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

        public override void Validate(IElasticClient elasticClient)
        {
            StringValidationsFor<ReindexTypeOperation>()
                .IsNotBlank(FromIndexName, RequiredMessage("FromIndexName"))
                .IsNotBlank(ToIndexName, RequiredMessage("ToIndexName"))
                .IsNotBlank(TypeName, RequiredMessage("TypeName"));

            IndexValidationsFor<ReindexTypeOperation>(elasticClient)
                .IndexExists(FromIndexName)
                .IndexExists(ToIndexName);
        }

        public override void Execute(IElasticClient elasticClient)
        {
            elasticClient.ReindexOnServer(descriptor =>
            {
                descriptor
                    .Source(sourceDescriptor => sourceDescriptor
                        .Type(TypeName)
                        .Index(FromIndexName))
                    .Destination(destinationDescriptor => destinationDescriptor
                        .VersionType(VersionType)
                        .Index(ToIndexName))
                    .WaitForCompletion()
                    .Refresh();

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