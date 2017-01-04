using System;
using System.Linq;
using ElasticUp.Migration;
using ElasticUp.Migration.Meta;
using ElasticUp.Operation.Reindex;
using Nest;

namespace ElasticUp.History
{
    public class MigrationHistoryHelper
    {
        private readonly IElasticClient _elasticClient;
        public readonly string MigrationHistoryIndexAlias;

        public MigrationHistoryHelper(IElasticClient elasticClient, string migrationHistoryIndexAlias)
        {
            _elasticClient = elasticClient;
            MigrationHistoryIndexAlias = migrationHistoryIndexAlias;
        }

        public void InitMigrationHistory()
        {
            var catAliasesRecords = _elasticClient.CatAliases(selector => selector.Name(MigrationHistoryIndexAlias)).Records;
            if (!catAliasesRecords.Any())
            {
                var migrationHistoryIndexName = new VersionedIndexName(MigrationHistoryIndexAlias, 0).IndexNameWithVersion();
                _elasticClient.CreateIndex(migrationHistoryIndexName);
                _elasticClient.PutAlias(migrationHistoryIndexName, MigrationHistoryIndexAlias);
            }
        }

        public void CopyMigrationHistory(string fromIndex, string toIndex)
        {
            if (string.IsNullOrEmpty(fromIndex))
                throw new ArgumentNullException(nameof(fromIndex));
            if (string.IsNullOrEmpty(toIndex))
                throw new ArgumentNullException(nameof(toIndex));

            var reindexTypeOperation = new ReindexTypeOperation<ElasticUpMigrationHistory>().FromIndex(fromIndex).ToIndex(toIndex);
            reindexTypeOperation.Execute(_elasticClient);
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
            if (string.IsNullOrEmpty(MigrationHistoryIndexAlias))
                throw new ArgumentNullException(nameof(MigrationHistoryIndexAlias));
            
            var history = new ElasticUpMigrationHistory(migrationName, exception);

            _elasticClient.Index(history, descriptor => descriptor.Index(MigrationHistoryIndexAlias));
        }

        public bool HasMigrationAlreadyBeenApplied(AbstractElasticUpMigration migration)
        {
            return HasMigrationAlreadyBeenApplied(migration?.ToString());
        }

        private bool HasMigrationAlreadyBeenApplied(string migrationName)
        {
            if (string.IsNullOrWhiteSpace(migrationName))
                throw new ArgumentNullException(nameof(migrationName));
            if (string.IsNullOrEmpty(MigrationHistoryIndexAlias))
                throw new ArgumentNullException(nameof(MigrationHistoryIndexAlias));

            if (!_elasticClient.IndexExists(MigrationHistoryIndexAlias).Exists) return false;

            var searchResponse = _elasticClient.Search<ElasticUpMigrationHistory>(sd =>
                sd.Index(MigrationHistoryIndexAlias)
                  .From(0).Size(5000)
                  .Query(q => q.Term(f => f.ElasticUpMigrationName, migrationName)));
            
            var foundMigration = searchResponse.Documents.SingleOrDefault(); //count should be 0 or 1 - but search to prevent 404
            return foundMigration != null && foundMigration.HasBeenAppliedSuccessfully;
        }
    }
}

