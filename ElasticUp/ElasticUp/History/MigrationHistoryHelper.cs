using System;
using System.Linq;
using ElasticUp.Migration;
using ElasticUp.Operation.Reindex;
using ElasticUp.Util;
using Nest;
using static ElasticUp.Validations.IndexValidations;
using static ElasticUp.Validations.StringValidations;

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

                IndexValidationsFor<MigrationHistoryHelper>(_elasticClient).IndexDoesNotExists(migrationHistoryIndexName);

                _elasticClient.CreateIndex(migrationHistoryIndexName);
                _elasticClient.PutAlias(migrationHistoryIndexName, MigrationHistoryIndexAlias);
            }
        }

        public void CopyMigrationHistory(string fromIndex, string toIndex)
        {
            var reindexTypeOperation = new ReindexTypeOperation<ElasticUpMigrationHistory>()
                                            .FromIndex(fromIndex)
                                            .ToIndex(toIndex);

            reindexTypeOperation.Validate(_elasticClient);
            reindexTypeOperation.Execute(_elasticClient);
        }

        public void AddMigrationToHistory(AbstractElasticUpMigration migration)
        {
            AddMigrationToHistory(migration?.ToString());
        }

        public void AddMigrationToHistory(AbstractElasticUpMigration migration, Exception exception)
        {
            AddMigrationToHistory(migration?.ToString(), exception);
        }

        private void AddMigrationToHistory(string migrationName, Exception exception = null)
        {
            StringValidationsFor<MigrationHistoryHelper>()
                .IsNotBlank(migrationName, RequiredMessage(nameof(migrationName)))
                .IsNotBlank(MigrationHistoryIndexAlias, RequiredMessage(nameof(MigrationHistoryIndexAlias)));
            
            var history = new ElasticUpMigrationHistory(migrationName, exception);

            _elasticClient.Index(history, descriptor => descriptor.Index(MigrationHistoryIndexAlias));
        }

        public bool HasMigrationAlreadyBeenApplied(AbstractElasticUpMigration migration)
        {
            return HasMigrationAlreadyBeenApplied(migration?.ToString());
        }

        private bool HasMigrationAlreadyBeenApplied(string migrationName)
        {
            StringValidationsFor<MigrationHistoryHelper>()
                .IsNotBlank(migrationName, RequiredMessage(nameof(migrationName)))
                .IsNotBlank(MigrationHistoryIndexAlias, RequiredMessage(nameof(MigrationHistoryIndexAlias)));
            
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

