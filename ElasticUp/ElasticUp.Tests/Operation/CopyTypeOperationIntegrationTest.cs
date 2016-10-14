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
        public CopyTypeOperationIntegrationTest() : base(ElasticServiceStartup.StartupForEach)
        {
        }

        [Test]
        public void Execute_CopiesTypeToNewIndex()
        {
            // GIVEN
            var operation = new CopyTypeOperation<TestDocument>(0);
            
            var oldIndex = new VersionedIndexName("test", 0);
            var newIndex = oldIndex.GetIncrementedVersion();

            ElasticClient.IndexMany(new [] {new TestDocument()}, oldIndex.ToString());
            ElasticClient.Refresh(Indices.All);

            // WHEN
            operation.Execute(ElasticClient, oldIndex.ToString(), newIndex.ToString());

            // THEN
            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<TestDocument>(descriptor => descriptor.Index(newIndex.ToString()));
            countResponse.Count.Should().Be(1);
        }
    }
}