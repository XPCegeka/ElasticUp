using ElasticUp.History;
using ElasticUp.Migration.Meta;
using ElasticUp.Operation;
using ElasticUp.Tests.Sample;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation
{
    [TestFixture]
    public class CopyTypeOperationIntegrationTest : AbstractIntegrationTest
    {
        public CopyTypeOperationIntegrationTest() : base(ElasticServiceStartupType.StartupForEach)
        {
        }

        [Test]
        public void Execute_CopiesTypeToNewIndex()
        {
            // GIVEN
            var operation = new CopyTypeOperation<SampleDocument>(0);
            
            var oldIndex = new VersionedIndexName("test", 0);
            var newIndex = oldIndex.GetIncrementedVersion();

            ElasticClient.IndexMany(new [] {new SampleDocument()}, oldIndex.ToString());
            ElasticClient.Refresh(Indices.All);

            // WHEN
            operation.Execute(ElasticClient, oldIndex.ToString(), newIndex.ToString());

            // THEN
            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleDocument>(descriptor => descriptor.Index(newIndex.ToString()));
            countResponse.Count.Should().Be(1);
        }
    }
}