using ElasticUp.Migration;
using ElasticUp.Operation;

namespace ElasticUp.Tests.Sample
{
    public class SampleMigrationWithCopyTypeOperation : ElasticUpMigration
    {
        public SampleMigrationWithCopyTypeOperation(int migrationNumber) : base(migrationNumber)
        {
            OnIndexAlias("sample-index")
                .Operation(new CopyTypeOperation<SampleObject>(0));
        }
    }
}