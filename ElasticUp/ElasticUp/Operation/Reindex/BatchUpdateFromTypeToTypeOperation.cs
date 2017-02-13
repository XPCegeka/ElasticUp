using System;
using System.Collections.Generic;
using System.Linq;
using Elasticsearch.Net;
using ElasticUp.Elastic;
using Nest;

namespace ElasticUp.Operation.Reindex
{
    public class BatchUpdateFromTypeToTypeOperation<TSourceType, TTargetType> : AbstractElasticUpOperation where TSourceType : class 
                                                                                                    where TTargetType : class
    {
        protected Time ScrollTimeout => new Time(TimeSpan.FromSeconds(ScrollTimeoutInSeconds));
        protected double ScrollTimeoutInSeconds = 360;
        protected int BatchSize = 5000;

        protected string FromIndexName;
        protected string ToIndexName;
        protected string SourceType;
        protected string TargetType;

        protected Func<SearchDescriptor<TSourceType>, ISearchRequest> SearchDescriptor = descriptor => descriptor.Type(typeof(TSourceType).Name.ToLowerInvariant());
        protected Func<TSourceType, TTargetType> Transformation = doc => doc as TTargetType;
        protected Action<TTargetType> OnDocumentProcessed;


        public BatchUpdateFromTypeToTypeOperation()
        {
            SourceType = typeof(TSourceType).Name.ToLowerInvariant();
            TargetType = typeof(TTargetType).Name.ToLowerInvariant();
        }

        public override void Execute(IElasticClient elasticClient)
        {
            if (string.IsNullOrWhiteSpace(FromIndexName)) throw new ElasticUpException($"BatchUpdateFromTypeToTypeOperation: Invalid fromIndexName {FromIndexName}");
            if (string.IsNullOrWhiteSpace(ToIndexName)) throw new ElasticUpException($"BatchUpdateFromTypeToTypeOperation: Invalid toIndexName {ToIndexName}");
            if (string.IsNullOrWhiteSpace(SourceType)) throw new ElasticUpException($"BatchUpdateFromTypeToTypeOperation: Invalid sourceType {SourceType}");
            if (string.IsNullOrWhiteSpace(TargetType)) throw new ElasticUpException($"BatchUpdateFromTypeToTypeOperation: Invalid targetType {TargetType}");
            if (!elasticClient.IndexExists(FromIndexName).Exists) throw new ElasticUpException($"BatchUpdateFromTypeToTypeOperation: Invalid fromIndex {FromIndexName} does not exist.");
            if (!elasticClient.IndexExists(ToIndexName).Exists) throw new ElasticUpException($"BatchUpdateFromTypeToTypeOperation: Invalid toIndex {ToIndexName} does not exist.");
            
            var searchResponse = elasticClient
                                    .Search<TSourceType>(descriptor => SearchDescriptor(descriptor
                                        .Index(FromIndexName)
                                        .Type(SourceType)
                                        .Version()
                                        .Scroll(ScrollTimeout)
                                        .Size(BatchSize)));

            if (!searchResponse.Documents.Any()) return;

            ProcessBatch(elasticClient, searchResponse.Hits, ToIndexName);

            var scrollId = searchResponse.ScrollId;
            var scrollResponse = elasticClient.Scroll<TSourceType>(ScrollTimeout, scrollId);
            while (scrollResponse.Documents.Any())
            {
                ProcessBatch(elasticClient, scrollResponse.Hits, ToIndexName);
                scrollResponse = elasticClient.Scroll<TSourceType>(ScrollTimeout, scrollResponse.ScrollId);
            }
        }

        protected virtual void ProcessBatch(IElasticClient elasticClient, IEnumerable<IHit<TSourceType>> hits, string toIndex)
        {
            var transformedDocuments = TransformDocuments(hits).ToList();
            IndexMany(elasticClient, transformedDocuments, toIndex, TargetType);
            
            foreach (var transformedDocument in transformedDocuments)
            {
                OnDocumentProcessed?.Invoke(transformedDocument.TransformedDocment); //TODO Pass transformedDocument here?
            }
        }

        protected void IndexMany(IElasticClient elasticClient, IEnumerable<TransformedDocument<TSourceType, TTargetType>> transformedDocuments, string indexName, string typeName)
        {
            var bulkDescriptor = new BulkDescriptor();

            foreach (var document in transformedDocuments)
            {
                if (document.TransformedDocment == null)
                    continue;

                if (document.Hit.Version.HasValue)
                {
                    bulkDescriptor.Index<object>(
                        descr => descr.Index(indexName)
                            .Id(document.Hit.Id)
                            .Type(typeName)
                            .VersionType(VersionType.External)
                            .Version(document.Hit.Version)
                            .Document(document.TransformedDocment));
                }
                else
                {
                    bulkDescriptor.Index<object>(
                    descr => descr.Index(indexName)
                        .Id(document.Hit.Id)
                        .Type(typeName)
                        .Document(document.TransformedDocment));
                }
            }

            elasticClient.Bulk(bulkDescriptor);
        }

        protected IEnumerable<TransformedDocument<TSourceType, TTargetType>> TransformDocuments(IEnumerable<IHit<TSourceType>> hits)
        {
            return hits
                .Where(hit => hit.Source != null)
                .Select(hit => new TransformedDocument<TSourceType, TTargetType>
                {
                    Hit = hit,
                    TransformedDocment = Transformation(hit.Source)
                });
        }
        
        public virtual BatchUpdateFromTypeToTypeOperation<TSourceType, TTargetType> FromIndex(string fromIndex)
        {
            FromIndexName = fromIndex?.ToLowerInvariant();
            return this;
        }

        public virtual BatchUpdateFromTypeToTypeOperation<TSourceType, TTargetType> ToIndex(string toIndex)
        {
            ToIndexName = toIndex?.ToLowerInvariant();
            return this;
        }

        public virtual BatchUpdateFromTypeToTypeOperation<TSourceType, TTargetType> WithDocumentTransformation(Func<TSourceType, TTargetType> transformation)
        {
            if (transformation == null) throw new ArgumentNullException(nameof(transformation));
            Transformation = transformation;
            return this;
        }

        public virtual BatchUpdateFromTypeToTypeOperation<TSourceType, TTargetType> WithSearchDescriptor(Func<SearchDescriptor<TSourceType>, ISearchRequest> selector)
        {
            if (selector == null) throw new ArgumentNullException(nameof(selector));
            SearchDescriptor = selector;
            return this;
        }

        public virtual BatchUpdateFromTypeToTypeOperation<TSourceType, TTargetType> WithScrollTimeout(double scrollTimeoutInSeconds)
        {
            if (scrollTimeoutInSeconds <= 0) throw new ArgumentException($"{nameof(scrollTimeoutInSeconds)} cannot be negative or zero");
            ScrollTimeoutInSeconds = scrollTimeoutInSeconds;
            return this;
        }

        public virtual BatchUpdateFromTypeToTypeOperation<TSourceType, TTargetType> WithOnDocumentProcessed(Action<TTargetType> onDocumentProcessed)
        {
            if (onDocumentProcessed == null)
                throw new ArgumentNullException(nameof(onDocumentProcessed));

            OnDocumentProcessed = onDocumentProcessed;
            return this;
        }

        public virtual BatchUpdateFromTypeToTypeOperation<TSourceType, TTargetType> WithBatchSize(int batchSize)
        {
            if (batchSize <= 0) throw new ArgumentException($"{nameof(batchSize)} cannot be negative or zero");
            BatchSize = batchSize;
            return this;
        }

        public virtual BatchUpdateFromTypeToTypeOperation<TSourceType, TTargetType> WithSameId()
        {
            return this;
        }
    }
}