using System.Linq;
using ElasticUp.Elastic;
using ElasticUp.Operation.Alias;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Alias
{
    [TestFixture]
    public class CreateAliasOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void CreateAlias_SetsAliasOnCorrectIndex()
        {
            new CreateAliasOperation("my-custom-alias")
                .OnIndex(TestIndex.IndexNameWithVersion())
                .Execute(ElasticClient);

            ElasticClient.GetIndicesPointingToAlias("my-custom-alias").Single().Should().Be(TestIndex.IndexNameWithVersion());
        }

        [Test]
        public void CreateAlias_ValidatesSettings()
        {
            var customAlias = TestIndex.AliasName + "-custom";
            Assert.Throws<ElasticUpException>(() => new CreateAliasOperation(null).OnIndex(TestIndex.IndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateAliasOperation(" ").OnIndex(TestIndex.IndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateAliasOperation(customAlias).OnIndex("index-does-not-exist").Execute(ElasticClient));
        }
    }
}