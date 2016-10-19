using System;
using Elasticsearch.Net;
using ElasticUp.History;
using Nest;

namespace ElasticUp.Operation
{
    public class CopyTypeOperation<T> : ElasticUpOperation where T : class
    {
        public CopyTypeOperation(int operationNumber) : base(operationNumber)
        {
        }

        public override void Execute(IElasticClient elasticClient, string fromIndex, string toIndex)
        {
            var typename = typeof (T).Name.ToLowerInvariant();

            var response = elasticClient.ReindexOnServer(descriptor =>
                descriptor.Source(sourceDescriptor =>
                    sourceDescriptor
                        .Type(typename)
                        .Index(fromIndex))
                    .Destination(destinationDescriptor =>
                        destinationDescriptor.Index(toIndex))
                    .WaitForCompletion());

            if (response.ServerError != null)
            {
                throw new Exception($"Could not execute {typeof(CopyTypeOperation<>).Name}. Error information: '{response.DebugInformation}'");
            }
        }
    }
}