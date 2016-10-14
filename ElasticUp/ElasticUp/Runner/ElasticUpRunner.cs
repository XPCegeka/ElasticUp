using System.Collections.Generic;
using ElasticUp.Migration;
using Nest;

namespace ElasticUp.Runner
{
    public class ElasticUpRunner
    {
        private IElasticClient _elasticClient;
        public readonly IList<ElasticUpMigration> Migrations = new List<ElasticUpMigration>();
        
        private ElasticUpRunner(IElasticClient elasticClient)
        {
            this._elasticClient = elasticClient;
        }

        public static ElasticUpRunner CreateElasticUpRunner(IElasticClient elasticClient)
        {
            return new ElasticUpRunner(elasticClient);
        }

        public ElasticUpRunner Migration(ElasticUpMigration migration)
        {

            Migrations.Add(migration);
            return this;
        }

        public void Run()
        {
            //TODO 
        }

        /*private void AssertMigrationsValid(List<IElasticSearchOperation> allOperationsGroupedByMigration)
        {
            if (allOperationsGroupedByMigration.Any(operation => operation.OperationId == null))
                throw new ArgumentException($"OperationId is required for an {nameof(ElasticSearchOperation)}");

            if (allOperationsGroupedByMigration.Select(operation => operation.OperationId.ToString()).Distinct().Count() != allOperationsGroupedByMigration.Count)
                throw new ArgumentException($"OperationId should be a unique identifier for an {nameof(ElasticSearchOperation)}");
        }*/

    }
}
