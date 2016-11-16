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

        public MigrationHistory(string migrationName)
        {
            Id = migrationName;
            Applied = DateTime.UtcNow;
        }

        public MigrationHistory(string migrationName, Exception e) : this(migrationName)
        {
            Exception = e;
        }
    }
}
