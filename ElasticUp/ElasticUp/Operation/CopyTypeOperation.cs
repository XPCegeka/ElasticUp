using System;
using Nest;

namespace ElasticUp.Operation
{
    public class CopyTypeOperation : ElasticUpOperation
    {
        public string TypeName { get; set; }

        public CopyTypeOperation(int operationNumber) : base(operationNumber) {}

        public CopyTypeOperation WithTypeName(string typeName)
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
                throw new Exception($"Could not execute {typeof(CopyTypeOperation<>).Name}. Error information: '{response.DebugInformation}'");
            }
        }
    }

    public class CopyTypeOperation<T> : CopyTypeOperation where T : class
    {
        public CopyTypeOperation(int operationNumber) : base(operationNumber)
        {
            TypeName = typeof (T).Name.ToLowerInvariant();
        }
    }
}