using Nest;
using System;

namespace ElasticUp.History
{
    public class ElasticUpMigrationHistory
    {
        [String(Index = FieldIndexOption.NotAnalyzed)]
        public string ElasticUpMigrationName { get; set; }

        [String(Index = FieldIndexOption.NotAnalyzed)]
        public string ElasticUpOperationName { get; set; }

        public DateTime ElasticUpMigrationApplied { get; set; } = DateTime.UtcNow;
    }
}
