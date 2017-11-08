using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Elasticsearch.Net;
using ElasticUp.Util;
using Nest;
using static ElasticUp.Validations.IndexValidations;
using static ElasticUp.Validations.StringValidations;

namespace ElasticUp.Operation.Reindex
{
    public class BatchUpdateOperation<TTransformFromType, TTransformToType> : AbstractElasticUpOperation
                        where TTransformFromType : class
                        where TTransformToType : class
    {
        private readonly BatchUpdateArguments<TTransformFromType, TTransformToType> _arguments;

        public BatchUpdateOperation(Func<BatchUpdateDescriptor<TTransformFromType, TTransformToType>, BatchUpdateArguments<TTransformFromType, TTransformToType>> arguments)
        {
            _arguments = arguments.Invoke(new BatchUpdateDescriptor<TTransformFromType, TTransformToType>());
        }

        public override void Validate(IElasticClient elasticClient)
        {
            StringValidationsFor<BatchUpdateOperation<TTransformFromType, TTransformToType>>()
                .IsNotBlank(_arguments.FromIndexName, RequiredMessage("FromIndexName"))
                .IsNotBlank(_arguments.ToIndexName, RequiredMessage("ToIndexName"))
                .IsNotBlank(_arguments.FromTypeName, RequiredMessage("FromTypeName"))
                .IsNotBlank(_arguments.ToTypeName, RequiredMessage("ToTypeName"));

            IndexValidationsFor<BatchUpdateOperation<TTransformFromType, TTransformToType>>(elasticClient)
                .IndexExists(_arguments.FromIndexName)
                .IndexExists(_arguments.ToIndexName);
        }

        public async Task Search(IElasticClient elasticClient, BufferBlock<IEnumerable<IHit<TTransformFromType>>> bufferQueue)
        {
            var searchResponse = Search(elasticClient);
            if (!searchResponse.Documents.Any()) { bufferQueue.Complete(); return; }

            do
            {
                await bufferQueue.SendAsync(searchResponse.Hits);
                searchResponse = elasticClient.Scroll<TTransformFromType>(_arguments.ScrollTimeout, searchResponse.ScrollId);
            }
            while (searchResponse.Documents.Any());
            bufferQueue.Complete();
        }

        public override void Execute(IElasticClient elasticClient)
        {
            var buffer = new BufferBlock<IEnumerable<IHit<TTransformFromType>>>(new DataflowBlockOptions { BoundedCapacity = _arguments.DegreeOfBatchParallellism * 2 });

            var consumer = new ActionBlock<IEnumerable<IHit<TTransformFromType>>>(
                                        hits => ProcessHits(elasticClient, hits),
                                        new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = _arguments.DegreeOfBatchParallellism });
            
            buffer.LinkTo(consumer, new DataflowLinkOptions { PropagateCompletion = true });

            var producer = Search(elasticClient, buffer);

            // Wait for everything to complete.
            Task.WaitAll(producer);
            consumer.Completion.Wait();
        }

        protected ISearchResponse<TTransformFromType> Search(IElasticClient elasticClient)
        {
            return elasticClient
                .Search<TTransformFromType>(descriptor => _arguments.SearchDescriptor(descriptor
                    .Index(_arguments.FromIndexName)
                    .Type(_arguments.FromTypeName)
                    .Version()
                    .Scroll(_arguments.ScrollTimeout)
                    .Size(_arguments.BatchSize)));
        }

        protected virtual void ProcessHits(IElasticClient elasticClient, IEnumerable<IHit<TTransformFromType>> hits)
        {
            var transformedDocuments = TransformDocuments(hits).ToList();
            BulkIndex(elasticClient, transformedDocuments);

            Parallel.ForEach(
                transformedDocuments, 
                new ParallelOptions { MaxDegreeOfParallelism = _arguments.DegreeOfTransformationParallellism }, 
                doc => _arguments.OnDocumentProcessed?.Invoke(doc.TransformedHit));
            
            //transformedDocuments.ForEach(doc => _arguments.OnDocumentProcessed?.Invoke(doc.TransformedHit));
        }

        protected void BulkIndex(IElasticClient elasticClient, IEnumerable<TransformedDocument<TTransformFromType, TTransformToType>> transformedDocuments)
        {
            var bulkDescriptor = new BulkDescriptor();

            foreach (var document in transformedDocuments)
            {
                if (document.TransformedHit == null)
                    continue;

                if (document.Hit.Version.HasValue)
                {
                    var version = document.Hit.Version;
                    if (_arguments.IncrementVersionInSameIndex)
                    {
                        version++;
                    }
                    bulkDescriptor.Index<object>(
                        descr => descr
                            .Index(_arguments.ToIndexName)
                            .Id(document.Hit.Id)
                            .Type(_arguments.ToTypeName)
                            .VersionType(VersionType.External)
                            .Version(version)
                            .Document(document.TransformedHit));
                }
                else
                {
                    bulkDescriptor.Index<object>(
                        descr => descr
                            .Index(_arguments.ToIndexName)
                            .Id(document.Hit.Id)
                            .Type(_arguments.ToTypeName)
                            .Document(document.TransformedHit));
                }
            }


            var bulkResponse = elasticClient.Bulk(bulkDescriptor);
            if (!bulkResponse.IsValid) throw new ElasticUpException("BatchUpdateOperation: failed to bulkIndex models:" + bulkResponse.DebugInformation);
        }

        protected IEnumerable<TransformedDocument<TTransformFromType, TTransformToType>> TransformDocuments(IEnumerable<IHit<TTransformFromType>> hits)
        {
            return hits
                .Where(hit => hit.Source != null)
                .Select(hit => new TransformedDocument<TTransformFromType, TTransformToType>
                {
                    Hit = hit,
                    TransformedHit = _arguments.Transformation(hit.Source)
                });
        }
    }
}