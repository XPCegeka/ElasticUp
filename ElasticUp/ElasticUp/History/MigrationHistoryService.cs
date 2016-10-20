using System;
using ElasticUp.Migration;
using ElasticUp.Operation;
using Nest;

namespace ElasticUp.History
{
    public class MigrationHistoryService
    {
        private readonly IElasticClient _elasticClient;

        public MigrationHistoryService(IElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
        }

        public void CopyMigrationHistory(string fromIndex, string toIndex)
        {
            if (string.IsNullOrEmpty(fromIndex))
                throw new ArgumentNullException(nameof(fromIndex));
            if (string.IsNullOrEmpty(toIndex))
                throw new ArgumentNullException(nameof(toIndex));

            var copyTypeOperation = new CopyTypeOperation<MigrationHistory>(0);
            copyTypeOperation.Execute(_elasticClient, fromIndex, toIndex);
        }

        public void AddMigrationToHistory(ElasticUpMigration migration, string indexName)
        {
            AddMigrationToHistory(migration, indexName, null);
        }

        public void AddMigrationToHistory(ElasticUpMigration migration, string indexName, Exception exception)
        {
            if (migration == null)
                throw new ArgumentNullException(nameof(migration));
            if (string.IsNullOrEmpty(indexName))
                throw new ArgumentNullException(nameof(indexName));

            var history = new MigrationHistory
            {
                Applied = DateTime.UtcNow,
                Id = migration.ToString(),
                Exception = exception
            };

            _elasticClient.Index(history, descriptor => descriptor.Index(indexName));
        }

        public bool HasMigrationAlreadyBeenApplied(ElasticUpMigration migration)
        {
            /*var appliedMigrationInPreviousIndex = _elasticClient.Get<ExecutedOperation>(migration.OperationId.ToString()).Source;
            var appliedMigrationInNewIndex = _elasticClient.Get<ExecutedOperation>(migration.OperationId.ToString(), idx => idx.Index(migration.GetTargetIndexName(_elasticClient))).Source;

            return appliedMigrationInPreviousIndex != null && appliedMigrationInPreviousIndex.HasBeenAppliedSuccessfully
                || appliedMigrationInNewIndex != null && appliedMigrationInNewIndex.HasBeenAppliedSuccessfully;*/
            return false;
        }
        /*
        public void OperationSucceeded(IElasticSearchOperation migrationThatSucceeded)
        {
            _elasticClient.Index(new ExecutedOperation(migrationThatSucceeded), idx => idx.Index(migrationThatSucceeded.GetTargetIndexName(_elasticClient)));
        }

        public void OperationFailed(IElasticSearchOperation migrationThatFailed, Exception e)
        {
            _elasticClient.Index(new ExecutedOperation(migrationThatFailed, e), idx => idx.Index(migrationThatFailed.GetTargetIndexName(_elasticClient)));
        }*/
    }
}

