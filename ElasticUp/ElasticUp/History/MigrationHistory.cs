using Nest;
using System;
using ElasticUp.Migration;

namespace ElasticUp.History
{
    public class MigrationHistory
    {
        public string Id { get; set; }
        public DateTime Applied { get; set; }
        public Exception Exception { get; set; }

        [Boolean(Ignore = true)]
        public bool HasBeenAppliedSuccessfully => Exception == null;

        public MigrationHistory() {}

        public MigrationHistory(ElasticUpMigration migration)
        {
            Id = migration.ToString();
            Applied = DateTime.UtcNow;
        }

        public MigrationHistory(ElasticUpMigration migration, Exception e) : this(migration)
        {
            Exception = e;
        }
    }
}
