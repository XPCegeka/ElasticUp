using System;
using System.Collections.Generic;
using System.Linq;
using ElasticUp.Elastic;
using Nest;

namespace ElasticUp.Operation.Delete
{
    public class DeleteByTypeOperation : AbstractElasticUpOperation
    {
        public TimeSpan ScrollTimeout { get; set; } = TimeSpan.FromMinutes(3);
        public int BatchSize { get; set; } = 5000;
        public string IndexName { get; set; }
        public string TypeName { get; set; }

        public DeleteByTypeOperation WithScrollTimeout(TimeSpan scrollTimeout)
        {
            ScrollTimeout = scrollTimeout;
            return this;
        }

        public DeleteByTypeOperation WithBatchSize(int batchSize)
        {
            if (batchSize <= 0)
                throw new ElasticUpException($"{nameof(batchSize)} cannot be 0 or negative", new ArgumentException(nameof(batchSize)));

            BatchSize = batchSize;
            return this;
        }

        public DeleteByTypeOperation WithIndexName(string indexName)
        {
            IndexName = indexName;
            return this;
        }

        public DeleteByTypeOperation WithTypeName(string typeName)
        {
            TypeName = typeName;
            return this;
        }

        public DeleteByTypeOperation WithTypeName<TType>()
        {
            TypeName = typeof (TType).Name.ToLowerInvariant();
            return this;
        }

        public override void Validate(IElasticClient elasticClient)
        {
            if (string.IsNullOrEmpty(IndexName)) throw new ElasticUpException($"Could not execute {nameof(DeleteByTypeOperation)}", new ArgumentNullException(nameof(IndexName)));
            if (string.IsNullOrEmpty(TypeName)) throw new ElasticUpException($"Could not execute {nameof(DeleteByTypeOperation)}", new ArgumentNullException(nameof(TypeName)));
        }

        public override void Execute(IElasticClient elasticClient)
        {
            var idsScrollResponse = elasticClient.Search<object>(descriptor => descriptor
                .MatchAll()
                .Index(IndexName)
                .Type(TypeName)
                .Scroll(new Time(ScrollTimeout))
                .Take(BatchSize)
                .Fields(fieldsDescr => fieldsDescr.Fields(Enumerable.Empty<Field>())));

            while (idsScrollResponse.Documents.Any())
            {
                DeleteIds(elasticClient, idsScrollResponse.Hits);
                idsScrollResponse = elasticClient.Scroll<object>(new Time(ScrollTimeout), idsScrollResponse.ScrollId);
            }
        }

        private void DeleteIds(IElasticClient elasticClient, IEnumerable<IHit<object>> documentHits)
        {
            var bulkDeleteRequest = new BulkDescriptor()
                .Index(IndexName)
                .Type(TypeName);

            foreach (var hit in documentHits)
            {
                bulkDeleteRequest.Delete<object>(descr => descr.Id(hit.Id));
            }

            elasticClient.Bulk(bulkDeleteRequest);
        }
    }
}