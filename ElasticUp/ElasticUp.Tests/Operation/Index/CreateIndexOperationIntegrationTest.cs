using System.Linq;
using ElasticUp.Operation.Index;
using ElasticUp.Util;
using FluentAssertions;
using Nest;
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
            ElasticClient.IndexExists(customIndexName).Exists.Should().BeFalse();

            new CreateIndexOperation(customIndexName).WithMapping(mapping => mapping).Execute(ElasticClient);
            
            ElasticClient.IndexExists(customIndexName).Exists.Should().BeTrue();
        }

        [Test]
        public void CreatesIndex_AlsoPutsAliasOnIndexWhenRequested()
        {
            var customIndexName = TestIndex.IndexNameWithVersion() + "-custom";
            var customAlias = TestIndex.AliasName + "-custom";

            ElasticClient.IndexExists(customIndexName).Exists.Should().BeFalse();

            new CreateIndexOperation(customIndexName).WithAlias(customAlias).WithMapping(mapping => mapping).Execute(ElasticClient);

            ElasticClient.IndexExists(customIndexName).Exists.Should().BeTrue();
            ElasticClient.GetIndicesPointingToAlias(customAlias).Single().Should().Be(customIndexName);
        }

        [Test]
        public void CreatesIndex_WithCustomMapping_CanMapMultiFields()
        {
            var customIndexName = TestIndex.IndexNameWithVersion() + "-custom";
            var customAlias = TestIndex.AliasName + "-custom";

            ElasticClient.IndexExists(customIndexName).Exists.Should().BeFalse();

            new CreateIndexOperation(customIndexName).WithAlias(customAlias).WithMapping(mapping => mapping).Execute(ElasticClient);

            ElasticClient.IndexExists(customIndexName).Exists.Should().BeTrue();
            ElasticClient.GetIndicesPointingToAlias(customAlias).Single().Should().Be(customIndexName);
        }

        [Test]
        public void CreateIndex_ValidatesSettings()
        {
            var customIndexName = TestIndex.IndexNameWithVersion() + "-custom";
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(null).WithMapping(mapping => mapping).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(" ").WithMapping(mapping => mapping).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(TestIndex.IndexNameWithVersion()).WithMapping(mapping => mapping).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(customIndexName).WithAlias(" ").WithMapping(mapping => mapping).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(customIndexName).WithMapping(null).Validate(ElasticClient));
        }
    }
}