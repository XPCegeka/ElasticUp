using System;
using System.Linq;
using ElasticUp.Migration.Meta;
using ElasticUp.Tests.Infrastructure;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Migration.Meta
{
    [TestFixture]
    public class AliasHelperIntegrationTest : AbstractIntegrationTest
    {
        private readonly IElasticClient _elasticClient = new ElasticClient(new Uri("http://localhost:9200"));

        public AliasHelperIntegrationTest()
            : base(ElasticServiceStartup.StartupForEach)
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
            _elasticClient.IndexMany(sampleObjects, index: indexName);
            _elasticClient.Refresh(Indices.All);

            // TEST
            var aliasHelper = new AliasHelper(_elasticClient);
            aliasHelper.AddAliasOnIndices(aliasName, indexName);

            // VERIFY
            var indicesPointingToAlias = _elasticClient.GetIndicesPointingToAlias(aliasName);
            indicesPointingToAlias.Should().HaveCount(1);
            indicesPointingToAlias[0].Should().Be(indexName);
        }

        [Test]
        public void AddAliasOnIndices_ThrowsWhenAliasCreationFailes()
        {
            // GIVEN
            const string indexName = "sample-index";
            const string aliasName = "sample-alias";

            var sampleObjects = Enumerable
                .Range(1, 100)
                .Select(n => new SampleObject { Number = n });
            _elasticClient.IndexMany(sampleObjects, index: indexName);
            _elasticClient.Refresh(Indices.All);

            // TEST
            var aliasHelper = new AliasHelper(_elasticClient);
            Assert.Throws<Exception>(() => aliasHelper.AddAliasOnIndices(aliasName, "unknown index"));
        }

        [Test]
        public void RemoveAliasOnIndices_DeletesAliasFromGivenIndex()
        {
            // GIVEN
            const string indexName = "sample-index";
            const string aliasName = "sample-alias";

            var sampleObjects = Enumerable
                .Range(1, 100)
                .Select(n => new SampleObject { Number = n });
            _elasticClient.IndexMany(sampleObjects, index: indexName);
            _elasticClient.PutAlias(indexName, aliasName);
            _elasticClient.Refresh(Indices.All);

            // TEST
            var aliasHelper = new AliasHelper(_elasticClient);
            aliasHelper.RemoveAliasOnIndices(aliasName, indexName);

            // VERIFY
            var aliasesPointingToIndex = _elasticClient.GetAliasesPointingToIndex(indexName);
            aliasesPointingToIndex.Should().HaveCount(0);
        }

        [Test]
        public void RemoveAliasOnIndices_ThrowsWhenAliasDeletionFailes()
        {
            // GIVEN
            const string indexName = "sample-index";

            var sampleObjects = Enumerable
                .Range(1, 100)
                .Select(n => new SampleObject { Number = n });
            _elasticClient.IndexMany(sampleObjects, index: indexName);
            _elasticClient.Refresh(Indices.All);

            // TEST
            var aliasHelper = new AliasHelper(_elasticClient);
            Assert.Throws<Exception>(() => aliasHelper.RemoveAliasOnIndices("unknown alias", indexName));
        }


        private class SampleObject
        {
            public int Number { get; set; }
        }
    }
}