using ElasticUp.Migration;
using ElasticUp.Migration.Meta;
using Nest;

namespace ElasticUp.History
{
    public class MigrationHistoryService
    {
        private readonly IElasticClient _elasticClient;
        private readonly VersionedIndexName _fromIndexName;
        private readonly VersionedIndexName _toIndex;

        public MigrationHistoryService(IElasticClient elasticClient, VersionedIndexName fromIndexName, VersionedIndexName toIndex)
        {
            _elasticClient = elasticClient;
            _fromIndexName = fromIndexName;
            _toIndex = toIndex;
        }

        public void CopyMigrationHistory()
        {
            //TODO
        }

        public void AddMigrationToHistory(ElasticUpMigration migration)
        {
            //TODO
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
