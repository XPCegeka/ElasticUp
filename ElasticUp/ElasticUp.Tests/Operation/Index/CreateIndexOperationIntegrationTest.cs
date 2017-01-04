using System.Linq;
using ElasticUp.Elastic;
using ElasticUp.Operation.Index;
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

            new CreateIndexOperation(customIndexName).Execute(ElasticClient);
            
            ElasticClient.IndexExists(customIndexName).Exists.Should().BeTrue();
        }

        [Test]
        public void CreatesIndex_AlsoPutsAliasOnIndexWhenRequested()
        {
            var customIndexName = TestIndex.IndexNameWithVersion() + "-custom";
            var customAlias = TestIndex.AliasName + "-custom";

            ElasticClient.IndexExists(customIndexName).Exists.Should().BeFalse();

            new CreateIndexOperation(customIndexName).WithAlias(customAlias).Execute(ElasticClient);
            
            ElasticClient.IndexExists(customIndexName).Exists.Should().BeTrue();
            ElasticClient.GetIndicesPointingToAlias(customAlias).Single().Should().Be(customIndexName);
        }

        [Test]
        public void CreateIndex_ValidatesSettings()
        {
            var customIndexName = TestIndex.IndexNameWithVersion() + "-custom";
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(null).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(" ").Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(TestIndex.IndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateIndexOperation(customIndexName).WithAlias(" ").Execute(ElasticClient));
        }
    }
}