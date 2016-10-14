using ElasticUp.Migration;
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
