using ElasticUp.Migration;

namespace ElasticUp.Tests.Sample
{
    public class SampleEmptyMigration : DefaultElasticUpMigration
    {
        public SampleEmptyMigration(string indexAlias) : base(indexAlias) {}
    }
}
