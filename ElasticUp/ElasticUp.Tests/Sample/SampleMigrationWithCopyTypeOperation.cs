using ElasticUp.Migration;
using ElasticUp.Operation;
using NUnit.Framework;

namespace ElasticUp.Tests.Sample
{
    public class SampleMigrationWithCopyTypeOperation : DefaultElasticUpMigration
    {
        public SampleMigrationWithCopyTypeOperation() : base(TestContext.CurrentContext.Test.MethodName.ToLowerInvariant())
        {
            Operation(new ReindexTypeOperation<SampleObject>(0));
        }
    }
}