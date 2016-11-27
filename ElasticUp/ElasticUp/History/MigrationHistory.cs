using Nest;
using System;

namespace ElasticUp.History
{
    public class ElasticUpMigrationHistory
    {
        public string ElasticUpMigrationName { get; set; }
        public DateTime ElasticUpMigrationApplied { get; set; }
        public Exception ElasticUpMigrationException { get; set; }

        [Boolean(Ignore = true)]
        public bool HasBeenAppliedSuccessfully => ElasticUpMigrationException == null;

        public ElasticUpMigrationHistory() {}

        public ElasticUpMigrationHistory(string migrationElasticUpMigrationName)
        {
            ElasticUpMigrationName = migrationElasticUpMigrationName;
            ElasticUpMigrationApplied = DateTime.UtcNow;
        }

        public ElasticUpMigrationHistory(string migrationElasticUpMigrationName, Exception e) : this(migrationElasticUpMigrationName)
        {
            ElasticUpMigrationException = e;
        }
    }
}
