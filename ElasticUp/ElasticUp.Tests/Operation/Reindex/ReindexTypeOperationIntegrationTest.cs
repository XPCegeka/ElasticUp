using System;
using System.Linq;
using System.Threading;
using ElasticUp.Operation.Mapping;
using ElasticUp.Operation.Reindex;
using ElasticUp.Tests.Infrastructure;
using ElasticUp.Tests.Sample;
using ElasticUp.Tests.Sample.IntValue;
using ElasticUp.Util;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Reindex
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
            new ReindexTypeOperation("sampledocument")
                    .FromIndex(TestIndex.IndexNameWithVersion())
                    .ToIndex(TestIndex.NextIndexNameWithVersion())
                    .Execute(ElasticClient);

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
            new ReindexTypeOperation("sampledocument")
                    .FromIndex(TestIndex.IndexNameWithVersion())
                    .ToIndex(TestIndex.NextIndexNameWithVersion())
                    .Execute(ElasticClient);

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
            var operation = new ReindexTypeOperation("sampledocument").FromIndex(oldIndex).ToIndex(newIndex);
            Assert.Throws<ElasticUpException>(() => operation.Validate(ElasticClient));
        }

        [Test]
        public void ReindexThrowsWhenToIndexDoesNotExist()
        {
            var operation = new ReindexTypeOperation("sampledocument")
                                    .FromIndex(TestIndex.IndexNameWithVersion())
                                    .ToIndex("this-index-does-not-exist");
            Assert.Throws<ElasticUpException>(() => operation.Validate(ElasticClient));
        }

        [Test]
        public void ReindexDoesNotThrowWhenNoDocumentsOfTypeAvailableInFromIndex()
        {
            // WHEN
            new ReindexTypeOperation("sampledocument")
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .Execute(ElasticClient);
            
            // THEN
            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleDocument>(descriptor => descriptor.Index(TestIndex.IndexNameWithVersion()));
            countResponse.Count.Should().Be(0);
            ElasticClient.IndexExists(TestIndex.NextIndexNameWithVersion()).Exists.Should().BeTrue();
        }

        [Test]
        public void AppearantlyMappingIsNotCopiedWhenReindexing()
        {
            //Given
            new PutTypeMappingOperation<SampleDocument>()
                .OnIndex(TestIndex.AliasName)
                .WithMapping(ResourceUtilities.FromResourceFileToString("mapping_v0_sampledocument.json"))
                .Execute(ElasticClient);

            var responseTestIndex = ElasticClient.GetMapping(new GetMappingRequest(Indices.Parse(TestIndex.IndexNameWithVersion())));
            responseTestIndex.Mappings.ToList()[0].Key.Should().Be(TestIndex.IndexNameWithVersion());
            ((StringProperty)responseTestIndex.Mappings.ToList()[0].Value[0].Properties.ToList()[1].Value).Index.Should().Be(FieldIndexOption.NotAnalyzed);

            ElasticClient.Index(new SampleDocument { Id = "1", Name = "jabba/the/hut" }, id => id.Index(TestIndex.AliasName));
            ElasticClient.Refresh(Indices.All);

            //When
            new ReindexTypeOperation("sampledocument")
                    .FromIndex(TestIndex.IndexNameWithVersion())
                    .ToIndex(TestIndex.NextIndexNameWithVersion())
                    .Execute(ElasticClient);

            // After ElasticUpMigration, the Alias will have been moved to the new versioned index. Since this index contains the updated mapping the sampledocument should now be found using the Term query
            ElasticClient.Get<SampleDocument>("1", g => g.Index(TestIndex.NextIndexNameWithVersion())).Should().NotBeNull();
            SpinWait.SpinUntil(() => ElasticClient.Search<SampleDocument>(s => s.Index(TestIndex.NextIndexNameWithVersion()).Query(q => q.Term(t => t.Name, "jabba/the/hut"))).Documents.Count() == 1, TimeSpan.FromSeconds(10));

            var responseNextTestIndex = ElasticClient.GetMapping(new GetMappingRequest(Indices.Parse(TestIndex.NextIndexNameWithVersion())));
            responseNextTestIndex.Mappings.ToList()[0].Key.Should().Be(TestIndex.NextIndexNameWithVersion());
            ((StringProperty)responseNextTestIndex.Mappings.ToList()[0].Value[0].Properties.ToList()[1].Value).Index.Should().NotBe(FieldIndexOption.NotAnalyzed);
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
            new ReindexTypeOperation("sampledocumentwithvalue")
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .WithInlineScript("def temp = ctx._source.value; ctx._source.remove('value'); ctx._source.value =  String.valueOf(temp);")
                .Execute(ElasticClient);

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


        [Test]
        [Ignore("test with refresh interval")]
        public void TomsRefreshIntervalExperimentWithReindex()
        {
            // GIVEN
            const int expectedDocumentCount = 1000000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);

            //zonder 58,995
            //met    52,767
            //bedenking: dit zijn natuurlijk erg kleine test objecten
            //ook een test draaien met grote objecten
            var response = ElasticClient.UpdateIndexSettings(TestIndex.NextIndexNameWithVersion(), s => s.IndexSettings(p => p.RefreshInterval(new Time(-1))));

            // WHEN
            using(new ElasticUpTimer("TomsRefreshIntervalExperimentWithReindex")) { 
                new ReindexTypeOperation("sampleobject")
                        .FromIndex(TestIndex.IndexNameWithVersion())
                        .ToIndex(TestIndex.NextIndexNameWithVersion())
                        .Execute(ElasticClient);
            }
            
            // THEN
            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()));
            countResponse.Count.Should().Be(expectedDocumentCount);
        }

        [Test]
        public void WaitForCompletionDoesNotWaitAnymoreSinceNest_2_4_7_AddRefreshInReindexOperation()
        {
            ElasticClient.IndexMany(new[] { new SampleDocument(), new SampleDocument() }, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);

            new ReindexTypeOperation("sampledocument")
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .Execute(ElasticClient);

            // ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleDocument>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()));
            countResponse.Count.Should().Be(2);
        }

    }
}