using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Nest;
using static ElasticUp.Elastic.ElasticClientHelper;

namespace ElasticUp.Operation
{
    public class BatchedElasticUpOperation<TDocument> : ElasticUpOperation where TDocument : class
    {
        public virtual double ScrollTimeoutInSeconds { get; set; } = 360;
        public virtual int BatchSize { get; set; } = 5000;
        public virtual Func<SearchDescriptor<TDocument>, ISearchRequest> SearchDescriptor { get; set; } = descriptor => descriptor.Type(typeof(TDocument).Name.ToLowerInvariant());
        public Func<TDocument, TDocument> Transformation { get; set; } = doc => doc;
        public Action<TDocument> OnDocumentProcessed { get; set; }

        public BatchedElasticUpOperation(int operationNumber) : base(operationNumber)
        {
        }

        public BatchedElasticUpOperation<TDocument> WithSearchDescriptor(Func<SearchDescriptor<TDocument>, ISearchRequest> selector)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            SearchDescriptor = selector;
            return this;
        }

        public BatchedElasticUpOperation<TDocument> WithDocumentTransformation(Func<TDocument, TDocument> transformation)
        {
            if (transformation == null)
                throw new ArgumentNullException(nameof(transformation));

            Transformation = transformation;
            return this;
        }

        public BatchedElasticUpOperation<TDocument> WithScrollTimeout(double scrollTimeoutInSeconds)
        {
            if (scrollTimeoutInSeconds <= 0)
                throw new ArgumentException($"{nameof(scrollTimeoutInSeconds)} cannot be negative or zero");

            ScrollTimeoutInSeconds = scrollTimeoutInSeconds;
            return this;
        }

        public BatchedElasticUpOperation<TDocument> WithOnDocumentProcessed(Action<TDocument> onDocumentProcessed)
        {
            if (onDocumentProcessed == null)
                throw new ArgumentNullException(nameof(onDocumentProcessed));

            OnDocumentProcessed = onDocumentProcessed;
            return this;
        }


        public BatchedElasticUpOperation<TDocument> WithBatchSize(int batchSize)
        {
            if (batchSize <= 0)
                throw new ArgumentException($"{nameof(batchSize)} cannot be negative or zero");

            BatchSize = batchSize;
            return this;
        }
        

        public override void Execute(IElasticClient elasticClient, string fromIndex, string toIndex)
        {
            var scrollTimeout = new Time(TimeSpan.FromSeconds(ScrollTimeoutInSeconds));

            var searchResponse = ValidateElasticResponse(elasticClient.Search<TDocument>(descriptor => SearchDescriptor(descriptor.Index(fromIndex).Scroll(scrollTimeout).Size(BatchSize))));
            if (searchResponse.ServerError != null)
                throw new Exception($"Could not complete Search call. Debug information: '{searchResponse.DebugInformation}'");

            if (!searchResponse.Documents.Any())
                return;

            ProcessBatch(elasticClient, searchResponse.Documents.ToList(), toIndex);

            var scrollId = searchResponse.ScrollId;
            var scrollResponse = ValidateElasticResponse(elasticClient.Scroll<TDocument>(scrollTimeout, scrollId));

            while (scrollResponse.Documents.Any())
            {
                if (scrollResponse.ServerError != null)
                    throw new Exception($"Could not complete Search call. Debug information: '{scrollResponse.DebugInformation}'");

                ProcessBatch(elasticClient, scrollResponse.Documents.ToList(), toIndex);

                scrollResponse = ValidateElasticResponse(elasticClient.Scroll<TDocument>(scrollTimeout, scrollId));
            }
        }

        private void ProcessBatch(IElasticClient elasticClient, IList<TDocument> documentBatch, string toIndex)
        {

            var transformedDocuments = documentBatch.Select(Transformation).Where(doc => doc != null).ToList();
            ValidateElasticResponse(elasticClient.IndexMany(transformedDocuments, index: toIndex));

            foreach (var transformedDocument in transformedDocuments)
            {
                OnDocumentProcessed?.Invoke(transformedDocument);
            }
        }
    }
}