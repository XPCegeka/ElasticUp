using ElasticUp.Tests.Sample;
using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Migration
{
    [TestFixture]
    public class AbstractElasticUpMigrationTest
    {
        [Test]
        public void Operation_AddsOperationToOperations()
        {
            var migration = new SampleEmptyVersionedIndexMigration(TestContext.CurrentContext.Test.MethodName.ToLowerInvariant());
            migration.Operation(new SampleEmptyOperation());
            migration.Operations.Should().HaveCount(1);
        }

        [Test]
        public void ToString_ReturnsClassName()
        {
            new SampleEmptyVersionedIndexMigration("index").ToString().Should().Be("sampleemptyversionedindexmigration");
        }
    }
}
