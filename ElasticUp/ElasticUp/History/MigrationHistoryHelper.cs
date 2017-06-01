using System.Linq;
using ElasticUp.Helper;
using ElasticUp.Migration;
using ElasticUp.Operation.Index;
using ElasticUp.Operation.Reindex;
using ElasticUp.Util;
using Nest;
using static ElasticUp.Validations.IndexValidations;
using static ElasticUp.Validations.StringValidations;

namespace ElasticUp.History
{
    public class MigrationHistoryHelper
    {
        public readonly string MigrationHistoryIndexAlias;

        private readonly IElasticClient _elasticClient;
        private readonly IndexHelper _indexHelper;

        public MigrationHistoryHelper(IElasticClient elasticClient, string migrationHistoryIndexAlias)
        {
            StringValidationsFor<MigrationHistoryHelper>()
                .IsNotBlank(migrationHistoryIndexAlias, RequiredMessage(nameof(MigrationHistoryIndexAlias)));

            _elasticClient = elasticClient;
            _indexHelper = new IndexHelper(_elasticClient);
            MigrationHistoryIndexAlias = migrationHistoryIndexAlias.ToLowerInvariant();
        }

        public void InitMigrationHistory()
        {
            var existingAliases = _elasticClient.CatAliases(selector => selector.Name(MigrationHistoryIndexAlias)).Records;

            if (!existingAliases.Any())
            {
                var migrationHistoryIndexName = new VersionedIndexName(MigrationHistoryIndexAlias, 0);

                var createIndexOperation = new CreateIndexOperation(migrationHistoryIndexName.IndexNameWithVersion())
                                                    .WithAlias(migrationHistoryIndexName.AliasName)
                                                    .WithMapping(ElasticUpMigrationHistoryConfig.Mapping);
                                                    //TODO specific settings?

                createIndexOperation.Validate(_elasticClient);
                createIndexOperation.Execute(_elasticClient);

                IndexValidationsFor<MigrationHistoryHelper>(_elasticClient)
                    .IndexExistsWithAlias(migrationHistoryIndexName);
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

        private void AddMigrationToHistory(string migrationName)
        {
            StringValidationsFor<MigrationHistoryHelper>()
                .IsNotBlank(migrationName, RequiredMessage(nameof(migrationName)))
                .IsNotBlank(MigrationHistoryIndexAlias, RequiredMessage(nameof(MigrationHistoryIndexAlias)));
            
            var history = new ElasticUpMigrationHistory { ElasticUpMigrationName = migrationName };

            _elasticClient.Index(history, descriptor => descriptor.Index(MigrationHistoryIndexAlias));
        }

        public bool HasMigrationAlreadyBeenApplied(AbstractElasticUpMigration migration)
        {
            return HasMigrationAlreadyBeenApplied(migration?.ToString());
        }

        private bool HasMigrationAlreadyBeenApplied(string migrationName)
        {
            StringValidationsFor<MigrationHistoryHelper>()
                .IsNotBlank(migrationName, RequiredMessage(nameof(migrationName)));

            if (_indexHelper.IndexDoesNotExist(MigrationHistoryIndexAlias)) return false;

            var searchResponse = _elasticClient.Search<ElasticUpMigrationHistory>(sd =>
                sd.Index(MigrationHistoryIndexAlias)
                  .From(0).Size(5000)
                  .Query(q => q.Term(f => f.ElasticUpMigrationName, migrationName)));
            
            var foundMigration = searchResponse.Documents.SingleOrDefault(); //count should be 0 or 1 - but search to prevent 404
            return foundMigration != null;
        }
    }
}

