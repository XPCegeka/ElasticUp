using Elasticsearch.Net;
using ElasticUp.Migration.Meta;
using ElasticUp.Operation;
using ElasticUp.Tests.Sample;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation
{
    [TestFixture]
    public class ReindexTypeOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void Execute_CopiesTypeToNewIndex()
        {
            // GIVEN
            ElasticClient.IndexMany(new[] { new SampleDocument() }, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);

            // WHEN
            var operation = new ReindexTypeOperation(0).WithTypeName("sampledocument");
            operation.Execute(ElasticClient, TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());

            // THEN
            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleDocument>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()));
            countResponse.Count.Should().Be(1);
        }

        [Test]
        public void Execute_ThrowsWhenFromIndexDoesNotExist()
        {
            // GIVEN
            var oldIndex = new VersionedIndexName("this-index-does-not-exist", 0);
            var newIndex = oldIndex.NextVersion();

            // WHEN
            var operation = new ReindexTypeOperation(0).WithTypeName("sampledocument");
            Assert.Throws<ElasticsearchClientException>(() => operation.Execute(ElasticClient, oldIndex, newIndex));
        }

        [Test]
        public void Execute_DoesNotThrowWhenNoDocumentsOfTypeAvailableInFromIndex()
        {
            // WHEN
            var operation = new ReindexTypeOperation(0).WithTypeName("sampledocument");
            operation.Execute(ElasticClient, TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());

            // THEN
            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleDocument>(descriptor => descriptor.Index(TestIndex.IndexNameWithVersion()));
            countResponse.Count.Should().Be(0);
            ElasticClient.IndexExists(TestIndex.NextIndexNameWithVersion()).Exists.Should().BeFalse();
        }
    }
}