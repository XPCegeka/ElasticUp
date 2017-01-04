using ElasticUp.Migration;

namespace ElasticUp.Tests.Sample
{
    public class SampleEmptyVersionedIndexMigration : ElasticUpVersionedIndexMigration
    {
        public SampleEmptyVersionedIndexMigration(string alias) : base(alias) {}

        protected override void DefineOperations()
        {
            //no operations defined in this sample
        }
    }
}
