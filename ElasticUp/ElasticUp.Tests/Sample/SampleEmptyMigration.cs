using ElasticUp.Migration;

namespace ElasticUp.Tests.Sample
{
    public class SampleEmptyMigration : ElasticUpMigration
    {
        public SampleEmptyMigration(int migrationNumber) : base(migrationNumber) {}
    }
}
