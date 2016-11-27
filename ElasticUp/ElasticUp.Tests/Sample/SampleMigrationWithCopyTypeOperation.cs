using ElasticUp.Migration;
using ElasticUp.Operation;
using NUnit.Framework;

namespace ElasticUp.Tests.Sample
{
    public class SampleMigrationWithCopyTypeOperation : DefaultElasticUpMigration
    {
        public SampleMigrationWithCopyTypeOperation(int migrationNumber) : base(migrationNumber, TestContext.CurrentContext.Test.MethodName.ToLowerInvariant())
        {
            Operation(new CopyTypeOperation<SampleObject>(0));
        }
    }
}