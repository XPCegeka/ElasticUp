using System;
using System.Linq;
using ElasticUp.Migration;
using ElasticUp.Operation;
using Nest;

namespace ElasticUp.History
{
    public class MigrationHistoryHelper
    {
        private readonly IElasticClient _elasticClient;

        public MigrationHistoryHelper(IElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
        }

        public void CopyMigrationHistory(string fromIndex, string toIndex)
        {
            if (string.IsNullOrEmpty(fromIndex))
                throw new ArgumentNullException(nameof(fromIndex));
            if (string.IsNullOrEmpty(toIndex))
                throw new ArgumentNullException(nameof(toIndex));

            var copyTypeOperation = new ReindexTypeOperation<ElasticUpMigrationHistory>(0);
            copyTypeOperation.Execute(_elasticClient, fromIndex, toIndex);
        }

        public void AddMigrationToHistory(AbstractElasticUpMigration migration, string indexName)
        {
            AddMigrationToHistory(migration?.ToString(), indexName, null);
        }

        public void AddMigrationToHistory(AbstractElasticUpMigration migration, string indexName, Exception exception)
        {
            AddMigrationToHistory(migration?.ToString(), indexName, exception);
        }

        private void AddMigrationToHistory(string migrationName, string indexName, Exception exception)
        {
            if (string.IsNullOrWhiteSpace(migrationName))
                throw new ArgumentNullException(nameof(migrationName));
            if (string.IsNullOrEmpty(indexName))
                throw new ArgumentNullException(nameof(indexName));

            var history = new ElasticUpMigrationHistory(migrationName, exception);

            _elasticClient.Index(history, descriptor => descriptor.Index(indexName));
        }

        public bool HasMigrationAlreadyBeenApplied(AbstractElasticUpMigration migration, string indexName)
        {
            return HasMigrationAlreadyBeenApplied(migration?.ToString(), indexName);
        }

        private bool HasMigrationAlreadyBeenApplied(string migrationName, string indexName)
        {
            if (string.IsNullOrWhiteSpace(migrationName))
                throw new ArgumentNullException(nameof(migrationName));
            if (string.IsNullOrEmpty(indexName))
                throw new ArgumentNullException(nameof(indexName));

            var searchResponse = _elasticClient.Search<ElasticUpMigrationHistory>(sd =>
                sd.Index(indexName)
                  .From(0).Size(5000)
                  .Query(q => q.Term(f => f.ElasticUpMigrationName, migrationName)));
            
            var foundMigration = searchResponse.Documents.SingleOrDefault(); //count should be 0 or 1 - but search to prevent 404
            return foundMigration != null && foundMigration.HasBeenAppliedSuccessfully;
        }
    }
}

