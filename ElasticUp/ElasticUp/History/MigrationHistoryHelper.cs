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
        public readonly string MigrationHistoryIndexName;

        public MigrationHistoryHelper(IElasticClient elasticClient, string migrationHistoryIndexName)
        {
            _elasticClient = elasticClient;
            MigrationHistoryIndexName = migrationHistoryIndexName;
        }

        public void CopyMigrationHistory(string fromIndex, string toIndex)
        {
            if (string.IsNullOrEmpty(fromIndex))
                throw new ArgumentNullException(nameof(fromIndex));
            if (string.IsNullOrEmpty(toIndex))
                throw new ArgumentNullException(nameof(toIndex));

            var reindexTypeOperation = new ReindexTypeOperation<ElasticUpMigrationHistory>();
            reindexTypeOperation.Execute(_elasticClient, fromIndex, toIndex);
        }

        public void AddMigrationToHistory(AbstractElasticUpMigration migration)
        {
            AddMigrationToHistory(migration?.ToString(), null);
        }

        public void AddMigrationToHistory(AbstractElasticUpMigration migration, Exception exception)
        {
            AddMigrationToHistory(migration?.ToString(), exception);
        }

        private void AddMigrationToHistory(string migrationName, Exception exception)
        {
            if (string.IsNullOrWhiteSpace(migrationName))
                throw new ArgumentNullException(nameof(migrationName));
            if (string.IsNullOrEmpty(MigrationHistoryIndexName))
                throw new ArgumentNullException(nameof(MigrationHistoryIndexName));

            var history = new ElasticUpMigrationHistory(migrationName, exception);

            _elasticClient.Index(history, descriptor => descriptor.Index(MigrationHistoryIndexName));
        }

        public bool HasMigrationAlreadyBeenApplied(AbstractElasticUpMigration migration)
        {
            return HasMigrationAlreadyBeenApplied(migration?.ToString());
        }

        private bool HasMigrationAlreadyBeenApplied(string migrationName)
        {
            if (string.IsNullOrWhiteSpace(migrationName))
                throw new ArgumentNullException(nameof(migrationName));
            if (string.IsNullOrEmpty(MigrationHistoryIndexName))
                throw new ArgumentNullException(nameof(MigrationHistoryIndexName));

            if (!_elasticClient.IndexExists(MigrationHistoryIndexName).Exists) return false;

            var searchResponse = _elasticClient.Search<ElasticUpMigrationHistory>(sd =>
                sd.Index(MigrationHistoryIndexName)
                  .From(0).Size(5000)
                  .Query(q => q.Term(f => f.ElasticUpMigrationName, migrationName)));
            
            var foundMigration = searchResponse.Documents.SingleOrDefault(); //count should be 0 or 1 - but search to prevent 404
            return foundMigration != null && foundMigration.HasBeenAppliedSuccessfully;
        }
    }
}

