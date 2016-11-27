using System.Linq;
using Elasticsearch.Net;
using ElasticUp.Alias;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Alias
{
    [TestFixture]
    [Parallelizable]
    public class AliasHelperIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void PutAliasOnIndex_CreatesNewAliasOnGivenIndex()
        {
            // GIVEN
            var sampleObjects = Enumerable.Range(1, 100).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleObjects, index: TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);

            // TEST
            var aliasHelper = new AliasHelper(ElasticClient);
            aliasHelper.PutAliasOnIndex(TestIndex.AliasName, TestIndex.IndexNameWithVersion());

            // VERIFY
            var indicesPointingToAlias = ElasticClient.GetIndicesPointingToAlias(TestIndex.AliasName);
            indicesPointingToAlias.Should().HaveCount(1);
            indicesPointingToAlias[0].Should().Be(TestIndex.IndexNameWithVersion());
        }

        [Test]
        public void PutAliasOnIndex_ThrowsExceptionWhenAliasCreationFails()
        {
            // GIVEN
            var sampleObjects = Enumerable.Range(1, 100).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleObjects, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);

            // TEST
            var aliasHelper = new AliasHelper(ElasticClient);
            Assert.Throws<ElasticsearchClientException>(() => aliasHelper.PutAliasOnIndex(TestIndex.AliasName, "unknown index"));
        }

        [Test]
        public void RemoveAliasFromIndex_DeletesAliasFromGivenIndex()
        {
            // GIVEN
            var sampleObjects = Enumerable.Range(1, 100).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleObjects, TestIndex.IndexNameWithVersion());
            ElasticClient.PutAlias(TestIndex.IndexNameWithVersion(), TestIndex.AliasName);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var aliasHelper = new AliasHelper(ElasticClient);
            aliasHelper.RemoveAliasFromIndex(TestIndex.AliasName, TestIndex.IndexNameWithVersion());

            // VERIFY
            var getIndexResponse = ElasticClient.GetIndex(TestIndex.IndexNameWithVersion());
            getIndexResponse.Indices[TestIndex.IndexNameWithVersion()].Aliases.ContainsKey(TestIndex.AliasName).Should().BeFalse();
        }

        [Test]
        public void RemoveAliasFromIndex_ThrowsExceptionWhenAliasDeletionFails()
        {
            // GIVEN
            var sampleObjects = Enumerable.Range(1, 100).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleObjects, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);

            // TEST
            var aliasHelper = new AliasHelper(ElasticClient);
            Assert.Throws<ElasticsearchClientException>(() => aliasHelper.RemoveAliasFromIndex("unknown alias", TestIndex.IndexNameWithVersion()));
        }
    }
}