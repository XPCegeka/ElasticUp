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
        public void ReindexCopiesDocumentsFromAnIndexToAnotherIndex()
        {
            // GIVEN
            ElasticClient.IndexMany(new[] { new SampleDocument(), new SampleDocument() }, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);

            // WHEN
            var operation = new ReindexTypeOperation().WithTypeName("sampledocument");
            operation.Execute(ElasticClient, TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());

            // THEN
            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleDocument>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()));
            countResponse.Count.Should().Be(2);
        }

        [Test]
        public void ReindexPreservesTheVersionOfTheDocumentsWhenReindexing()
        {
            // GIVEN
            var sampleDocument = new SampleDocument { Id = "1" };
            ElasticClient.Index(sampleDocument, descriptor => descriptor.Index(TestIndex.IndexNameWithVersion()));
            ElasticClient.Index(sampleDocument, descriptor => descriptor.Index(TestIndex.IndexNameWithVersion()));
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Get<SampleDocument>("1", descriptor => descriptor.Index(TestIndex.IndexNameWithVersion())).Version.Should().Be(2);

            // WHEN
            var operation = new ReindexTypeOperation().WithTypeName("sampledocument");
            operation.Execute(ElasticClient, TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());

            // THEN
            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleDocument>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()));
            countResponse.Count.Should().Be(1);
            ElasticClient.Get<SampleDocument>("1", descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion())).Version.Should().Be(2);
        }

        [Test]
        public void ReindexDoesNotThrowWhenNoDocumentsOfTypeAvailableInSourceIndex()
        {
            // WHEN
            var operation = new ReindexTypeOperation().WithTypeName("sampledocument");
            operation.Execute(ElasticClient, TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());
            
            // THEN
            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleDocument>(descriptor => descriptor.Index(TestIndex.IndexNameWithVersion()));
            countResponse.Count.Should().Be(0);
            ElasticClient.IndexExists(TestIndex.NextIndexNameWithVersion()).Exists.Should().BeTrue();
        }
    }
}