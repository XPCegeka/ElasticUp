using System;
using Nest;

namespace ElasticUp.Operation
{
    public class ReindexTypeOperation : ElasticUpOperation
    {
        public string TypeName { get; set; }

        public ReindexTypeOperation WithTypeName(string typeName)
        {
            TypeName = typeName;
            return this;
        }

        public override void Execute(IElasticClient elasticClient, string fromIndex, string toIndex)
        {
            var response = elasticClient.ReindexOnServer(descriptor => descriptor
                    .Source(sourceDescriptor => sourceDescriptor
                        .Type(TypeName)
                        .Index(fromIndex))
                    .Destination(destinationDescriptor => destinationDescriptor
                        .Index(toIndex))
                    .WaitForCompletion());

            if (response.ServerError != null)
            {
                throw new Exception($"Could not execute {typeof(ReindexTypeOperation<>).Name}. Error information: '{response.DebugInformation}'");
            }
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