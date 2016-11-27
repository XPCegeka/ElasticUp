using ElasticUp.Migration;

namespace ElasticUp.Tests.Sample
{
    public class SampleEmptyMigration : DefaultElasticUpMigration
    {
        public SampleEmptyMigration(int migrationNumber) : base(migrationNumber, "sampleIndex") {}
        public SampleEmptyMigration(int migrationNumber, string indexAlias) : base(migrationNumber, indexAlias) {}
    }
}
