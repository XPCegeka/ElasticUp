using System.Collections.Generic;
using System.Linq;
using Elasticsearch.Net;
using ElasticUp.Migration.Meta;
using ElasticUp.Operation;
using ElasticUp.Tests.Sample;
using ElasticUp.Tests.Sample.IntValue;
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
        public void ReindexThrowsWhenFromIndexDoesNotExist()
        {
            // GIVEN
            var oldIndex = new VersionedIndexName("this-index-does-not-exist", 0);
            var newIndex = oldIndex.NextVersion();

            // WHEN
            var operation = new ReindexTypeOperation().WithTypeName("sampledocument");
            Assert.Throws<ElasticsearchClientException>(() => operation.Execute(ElasticClient, oldIndex, newIndex));
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

        [Test]
        [Ignore("experimental. Uses groovy but groovy not enabled. In Elastic 5+ should use 'painless' script")]
        public void ReindexWithScriptToModifyAField()
        {
            // GIVEN
            ElasticClient.Index(new Sample.IntValue.SampleDocumentWithValue { Id = "1", Value = 123456});
            ElasticClient.Refresh(Indices.All);
            
            //Check mapping of Value = number
            var propertyMappingBeforeReindex = ElasticClient.GetMapping<SampleDocumentWithValue>().IndexTypeMappings["reindexwithscripttomodifyafield-v0"]["sampledocumentwithvalue"].Properties.ToList()[1];
            propertyMappingBeforeReindex.Key.Name.Should().Be("value");
            propertyMappingBeforeReindex.Value.GetType().Should().Be(typeof(NumberProperty));

            // WHEN
            new ReindexTypeOperation()
                .WithTypeName("sampledocumentwithvalue")
                .WithInlineScript("def temp = ctx._source.value; ctx._source.remove('value'); ctx._source.value =  String.valueOf(temp);")
                .Execute(ElasticClient, TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());

            //Check mapping of Value = number
            var propertyMappingAfterReindex = ElasticClient.GetMapping<SampleDocumentWithValue>(d => d.Index(TestIndex.NextIndexNameWithVersion())).IndexTypeMappings["reindexwithscripttomodifyafield-v1"]["sampledocumentwithvalue"].Properties.ToList()[1];
            propertyMappingAfterReindex.Key.Name.Should().Be("value");
            propertyMappingAfterReindex.Value.GetType().Should().Be(typeof(StringProperty));
            
            // THEN
            ElasticClient.Refresh(Indices.All);
            var documents = ElasticClient
                .Search<Sample.StringValue.SampleDocumentWithValue>(descriptor => descriptor
                    .Type("sampledocumentwithvalue")
                    .Index(TestIndex.NextIndexNameWithVersion()))
                .Documents.ToList();
            documents.Count.Should().Be(1);
            documents[0].Value.Should().Be("123456");
        }
    }
}