using System;
using ElasticUp.Util;
using Nest;

namespace ElasticUp.Extension
{
    public class ReadOnlyIndexContext : IDisposable
    {
        private readonly IElasticClient _elasticClient;
        private readonly string _indexName;

        public ReadOnlyIndexContext(IElasticClient elasticClient, string indexName)
        {
            if (elasticClient == null)
                throw new ElasticUpException($"{nameof(elasticClient)} cannot be null.", new ArgumentNullException(nameof(elasticClient)));

            if (string.IsNullOrEmpty(indexName))
                throw new ElasticUpException($"{nameof(indexName)} cannot be null or empty.", new ArgumentNullException(nameof(indexName)));

            _elasticClient = elasticClient;
            _indexName = indexName;

            _elasticClient.SetIndexBlocksReadOnly(_indexName, true);
        }

        public void Dispose()
        {
            _elasticClient.SetIndexBlocksReadOnly(_indexName, false);
        }
    }
}