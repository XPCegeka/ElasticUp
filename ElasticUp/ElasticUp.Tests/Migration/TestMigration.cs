using ElasticUp.Migration;

namespace ElasticUp.Tests.Migration
{
    public class TestMigration : ElasticUpMigration
    {
        public TestMigration(int migrationNumber) : base(migrationNumber) {}
    }
}
