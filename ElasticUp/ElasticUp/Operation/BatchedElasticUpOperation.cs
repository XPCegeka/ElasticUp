using System;
using System.Collections.Generic;
using System.Linq;
using Nest;

namespace ElasticUp.Operation
{
    public class BatchedElasticUpOperation<TDocument> : ElasticUpOperation where TDocument : class
    {
        public virtual double ScrollTimeoutInSeconds { get; set; } = 360;
        public virtual int BatchSize { get; set; } = 5000;
        public virtual Func<SearchDescriptor<TDocument>, ISearchRequest> SearchDescriptor { get; set; } = descriptor => descriptor.Type(typeof(TDocument).Name.ToLowerInvariant());
        public Func<IEnumerable<TDocument>, IEnumerable<TDocument>> BatchTransformation { get; set; } = batchDocuments => new List<TDocument>(batchDocuments);

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

        public BatchedElasticUpOperation<TDocument> WithBatchTransformation(Func<IEnumerable<TDocument>, IEnumerable<TDocument>> batchOperation)
        {
            if (batchOperation == null)
                throw new ArgumentNullException(nameof(batchOperation));

            BatchTransformation = batchOperation;
            return this;
        }

        public BatchedElasticUpOperation<TDocument> WithScrollTimeout(double scrollTimeoutInSeconds)
        {
            if (scrollTimeoutInSeconds <= 0)
                throw new ArgumentException($"{nameof(scrollTimeoutInSeconds)} cannot be negative or zero");

            ScrollTimeoutInSeconds = scrollTimeoutInSeconds;
            return this;
        }

        public BatchedElasticUpOperation<TDocument> WithOnBatchProcessed(BatchProcessedHandler eventHandler)
        {
            if (eventHandler == null)
                throw new ArgumentNullException(nameof(eventHandler));

            BatchProcessed += eventHandler;
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

            var searchResponse = elasticClient.Search<TDocument>(descriptor => SearchDescriptor(descriptor.Index(fromIndex).Scroll(scrollTimeout).Size(BatchSize)));
            if (searchResponse.ServerError != null)
                throw new Exception($"Could not complete Search call. Debug information: '{searchResponse.DebugInformation}'");

            if (!searchResponse.Documents.Any())
                return;

            ProcessBatch(elasticClient, searchResponse.Documents.ToList(), toIndex);

            var scrollId = searchResponse.ScrollId;
            var scrollResponse = elasticClient.Scroll<TDocument>(scrollTimeout, scrollId);

            while (scrollResponse.Documents.Any())
            {
                if (scrollResponse.ServerError != null)
                    throw new Exception($"Could not complete Search call. Debug information: '{scrollResponse.DebugInformation}'");

                ProcessBatch(elasticClient, scrollResponse.Documents.ToList(), toIndex);

                scrollResponse = elasticClient.Scroll<TDocument>(scrollTimeout, scrollId);
            }
        }

        private void ProcessBatch(IElasticClient elasticClient, IList<TDocument> documentBatch, string toIndex)
        {
            var transformedDocuments = BatchTransformation.Invoke(documentBatch)?.ToList() ?? new List<TDocument>();
            elasticClient.IndexMany(transformedDocuments, index: toIndex);
            BatchProcessed?.Invoke(documentBatch, transformedDocuments);
        }

        public event BatchProcessedHandler BatchProcessed;

        public delegate void BatchProcessedHandler(IEnumerable<TDocument> originalDocuments, IEnumerable<TDocument> transformedDocuments);
    }

}