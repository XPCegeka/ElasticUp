using ElasticUp.Helper;
using ElasticUp.Operation.Index;
using ElasticUp.Util;
using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Index
{
    [TestFixture]
    public class CreateIndexOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void CreatesIndex()
        {
            var customIndexName = TestIndex.IndexNameWithVersion() + "-custom";
            new IndexHelper(ElasticClient).IndexExists(customIndexName).Should().BeFalse();

            new CreateIndexOperation(customIndexName).WithMapping(mapping => mapping).Execute(ElasticClient);

            new IndexHelper(ElasticClient).IndexExists(customIndexName).Should().BeTrue();
        }

        [Test]
        public void CreatesIndex_AlsoPutsAliasOnIndexWhenRequested()
        {
            var customIndexName = TestIndex.IndexNameWithVersion() + "-custom";
            var customAlias = TestIndex.AliasName + "-custom";

            new IndexHelper(ElasticClient).IndexExists(customIndexName).Should().BeFalse();

            new CreateIndexOperation(customIndexName).WithAlias(customAlias).WithMapping(mapping => mapping).Execute(ElasticClient);

            new AliasHelper(ElasticClient).AliasExistsOnIndex(customAlias, customIndexName).Should().BeTrue();
        }

        [Test]
        public void CreatesIndex_WithCustomMapping_CanMapMultiFields()
        {
            var customIndexName = TestIndex.IndexNameWithVersion() + "-custom";
            var customAlias = TestIndex.AliasName + "-custom";

            new IndexHelper(ElasticClient).IndexExists(customIndexName).Should().BeFalse();

            new CreateIndexOperation(customIndexName).WithAlias(customAlias).WithMapping(mapping => mapping).Execute(ElasticClient);

            new AliasHelper(ElasticClient).AliasExistsOnIndex(customAlias, customIndexName).Should().BeTrue();
        }

        [Test]
        public void CreateIndex_ValidatesSettings()
        {
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(null).WithMapping(mapping => mapping).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(" ").WithMapping(mapping => mapping).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(TestIndex.IndexNameWithVersion()).WithMapping(mapping => mapping).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(TestIndex.IndexNameWithVersion() + "-custom").WithAlias(" ").WithMapping(mapping => mapping).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(TestIndex.IndexNameWithVersion() + "-custom").WithMapping(null).Validate(ElasticClient));
        }
    }
}