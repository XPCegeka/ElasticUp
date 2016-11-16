using ElasticUp.Migration;
using ElasticUp.Operation;

namespace ElasticUp.Tests.Sample
{
    public class SampleMigrationWithCopyTypeOperation : DefaultElasticUpMigration
    {
        public SampleMigrationWithCopyTypeOperation(int migrationNumber) : base(migrationNumber, "sample-index")
        {
            Operation(new CopyTypeOperation<SampleObject>(0));
        }
    }
}