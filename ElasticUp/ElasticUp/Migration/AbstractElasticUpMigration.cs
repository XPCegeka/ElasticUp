using System.Collections.Generic;
using ElasticUp.Operation;
using Nest;

namespace ElasticUp.Migration
{
    public abstract class AbstractElasticUpMigration
    {
        protected readonly IElasticClient ElasticClient;
        protected List<ElasticUpOperation> Operations { get; }

        protected string SourceIndex;
        protected string TargetIndex;

        protected AbstractElasticUpMigration(IElasticClient elasticClient)
        {
            ElasticClient = elasticClient;
        }

        public virtual void Run()
        {
            PreMigrationTasks();
            DoMigrationTasks();
            PostMigrationTasks();
        }

        public abstract void PreMigrationTasks();
        public abstract void DoMigrationTasks();
        public abstract void PostMigrationTasks();
    }
}
