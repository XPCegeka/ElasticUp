using ElasticUp.Migration;
using ElasticUp.Operation.Reindex;
using NUnit.Framework;

namespace ElasticUp.Tests.Sample
{
    public class SampleVersionedIndexMigrationWithReindexTypeOperation : ElasticUpVersionedIndexMigration
    {
        public SampleVersionedIndexMigrationWithReindexTypeOperation() : base(TestContext.CurrentContext.Test.MethodName.ToLowerInvariant()) {}

        protected override void DefineOperations()
        {
            Operation(new ReindexTypeOperation<SampleObject>()
                            .FromIndex(FromIndexName)
                            .ToIndex(ToIndexName));
        }
    }
}