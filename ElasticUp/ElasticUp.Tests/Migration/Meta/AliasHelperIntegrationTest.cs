using System;
using System.Linq;
using ElasticUp.Migration.Meta;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Migration.Meta
{
    [TestFixture]
    public class AliasHelperIntegrationTest : AbstractIntegrationTest
    {
        public AliasHelperIntegrationTest()
            : base(ElasticServiceStartup.OneTimeStartup)
        {
        }

        [Test]
        public void AddAliasOnIndices_CreatesNewAliasOnGivenIndex()
        {
            // GIVEN
            const string indexName = "sample-index";
            const string aliasName = "sample-alias";

            var sampleObjects = Enumerable
                .Range(1, 100)
                .Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleObjects, index: indexName);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var aliasHelper = new AliasHelper(ElasticClient);
            aliasHelper.AddAliasOnIndices(aliasName, indexName);

            // VERIFY
            var indicesPointingToAlias = ElasticClient.GetIndicesPointingToAlias(aliasName);
            indicesPointingToAlias.Should().HaveCount(1);
            indicesPointingToAlias[0].Should().Be(indexName);
        }

        [Test]
        public void AddAliasOnIndices_ThrowsWhenAliasCreationFailes()
        {
            // GIVEN
            const string indexName = "sample-index2";
            const string aliasName = "sample-alias2";

            var sampleObjects = Enumerable
                .Range(1, 100)
                .Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleObjects, index: indexName);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var aliasHelper = new AliasHelper(ElasticClient);
            Assert.Throws<Exception>(() => aliasHelper.AddAliasOnIndices(aliasName, "unknown index"));
        }

        [Test]
        public void RemoveAliasOnIndices_DeletesAliasFromGivenIndex()
        {
            // GIVEN
            const string indexName = "sample-index3";
            const string aliasName = "sample-alias3";

            var sampleObjects = Enumerable
                .Range(1, 100)
                .Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleObjects, index: indexName);
            ElasticClient.PutAlias(indexName, aliasName);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var aliasHelper = new AliasHelper(ElasticClient);
            aliasHelper.RemoveAliasOnIndices(aliasName, indexName);

            // VERIFY
            var aliasesPointingToIndex = ElasticClient.GetAliasesPointingToIndex(indexName);
            aliasesPointingToIndex.Should().HaveCount(0);
        }

        [Test]
        public void RemoveAliasOnIndices_ThrowsWhenAliasDeletionFailes()
        {
            // GIVEN
            const string indexName = "sample-index4";

            var sampleObjects = Enumerable
                .Range(1, 100)
                .Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleObjects, index: indexName);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var aliasHelper = new AliasHelper(ElasticClient);
            Assert.Throws<Exception>(() => aliasHelper.RemoveAliasOnIndices("unknown alias", indexName));
        }


        private class SampleObject
        {
            public int Number { get; set; }
        }
    }
}