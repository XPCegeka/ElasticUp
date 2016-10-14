using ElasticUp.History;
using ElasticUp.Migration.Meta;
using ElasticUp.Operation;
using ElasticUp.Tests.Documents;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation
{
    [TestFixture]
    public class CopyTypeOperationIntegrationTest : AbstractIntegrationTest
    {
        public CopyTypeOperationIntegrationTest() : base(ElasticServiceStartup.OneTimeStartup)
        {
        }

        [Test]
        public void Execute_CopiesTypeToNewIndex()
        {
            // GIVEN
            var operation = new CopyTypeOperation<TestDocument>(0);
            
            var index0 = new VersionedIndexName("test", 0);
            var index1 = index0.GetIncrementedVersion();

            ElasticClient.IndexMany(new [] {new TestDocument()}, index0.Name);

            // WHEN
            operation.Execute(ElasticClient, index0.Name, index1.Name);

            // THEN
            var docs = ElasticClient.Search<TestDocument>(s => s.Index(index1.Name).Type<TestDocument>()).Documents;
            docs.Should().HaveCount(1);
        }
    }
}