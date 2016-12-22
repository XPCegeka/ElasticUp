using System;
using System.Collections.Generic;
using System.Linq;
using Nest;

namespace ElasticUp.Operation
{
    public class BatchUpdateFromTypeToTypeOperation<TSourceType, TTargetType> : ElasticUpOperation where TSourceType : class 
                                                                                                          where TTargetType : class
    {
        protected Time ScrollTimeout => new Time(TimeSpan.FromSeconds(ScrollTimeoutInSeconds));
        protected double ScrollTimeoutInSeconds { get; set; } = 360;
        protected int BatchSize { get; set; } = 5000;

        protected string SourceType { get; set; }
        protected string TargetType { get; set; }

        protected Func<SearchDescriptor<TSourceType>, ISearchRequest> SearchDescriptor { get; set; } = descriptor => descriptor.Type(typeof(TSourceType).Name.ToLowerInvariant());
        private Func<TSourceType, TTargetType> Transformation { get; set; } = doc => doc as TTargetType;
        protected Action<TTargetType> OnDocumentProcessed { get; set; }


        public BatchUpdateFromTypeToTypeOperation()
        {
            SourceType = typeof(TSourceType).Name.ToLowerInvariant();
            TargetType = typeof(TTargetType).Name.ToLowerInvariant();
        }

        public override void Execute(IElasticClient elasticClient, string fromIndex, string toIndex)
        {
            var searchResponse = elasticClient
                                    .Search<TSourceType>(descriptor => SearchDescriptor(descriptor
                                        .Index(fromIndex)
                                        .Type(SourceType)
                                        .Scroll(ScrollTimeout)
                                        .Size(BatchSize)));

            if (!searchResponse.Documents.Any()) return;

            ProcessBatch(elasticClient, searchResponse.Documents, toIndex);

            var scrollId = searchResponse.ScrollId;
            var scrollResponse = elasticClient.Scroll<TSourceType>(ScrollTimeout, scrollId);
            while (scrollResponse.Documents.Any())
            {
                ProcessBatch(elasticClient, scrollResponse.Documents, toIndex);
                scrollResponse = elasticClient.Scroll<TSourceType>(ScrollTimeout, scrollResponse.ScrollId);
            }
        }

        protected virtual void ProcessBatch(IElasticClient elasticClient, IEnumerable<TSourceType> documents, string toIndex)
        {
            var transformedDocuments = TransformDocuments(documents).ToList();
            elasticClient.IndexMany(transformedDocuments, toIndex, TargetType);
            foreach (var transformedDocument in transformedDocuments)
            {
                OnDocumentProcessed?.Invoke(transformedDocument);
            }
        }

        protected IEnumerable<TTargetType> TransformDocuments(IEnumerable<TSourceType> documents)
        {
            return documents
                .Select(Transformation)
                .Where(o => o != null);
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
    }
}