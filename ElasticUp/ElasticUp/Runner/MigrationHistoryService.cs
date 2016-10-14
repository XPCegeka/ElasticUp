using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ElasticUp.Migration;
using Nest;

namespace ElasticUp.Runner
{
    public class MigrationHistoryService
    {
        private readonly IElasticClient _elasticClient;

        public MigrationHistoryService(IElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
        }

        public bool HasOperationBeenApplied(ElasticUpMigration migration)
        {
            var appliedMigrationInPreviousIndex = _elasticClient.Get<ExecutedOperation>(migration.OperationId.ToString()).Source;
            var appliedMigrationInNewIndex = _elasticClient.Get<ExecutedOperation>(migration.OperationId.ToString(), idx => idx.Index(migration.GetTargetIndexName(_elasticClient))).Source;

            return appliedMigrationInPreviousIndex != null && appliedMigrationInPreviousIndex.HasBeenAppliedSuccessfully
                || appliedMigrationInNewIndex != null && appliedMigrationInNewIndex.HasBeenAppliedSuccessfully;
        }

        public void OperationSucceeded(IElasticSearchOperation migrationThatSucceeded)
        {
            _elasticClient.Index(new ExecutedOperation(migrationThatSucceeded), idx => idx.Index(migrationThatSucceeded.GetTargetIndexName(_elasticClient)));
        }

        public void OperationFailed(IElasticSearchOperation migrationThatFailed, Exception e)
        {
            _elasticClient.Index(new ExecutedOperation(migrationThatFailed, e), idx => idx.Index(migrationThatFailed.GetTargetIndexName(_elasticClient)));
        }
    }
}
