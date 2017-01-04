using ElasticUp.Elastic;
using ElasticUp.Operation.Alias;
using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Alias
{
    [TestFixture]
    public class RemoveAliasOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void RemoveAlias_RemovesAliasOnCorrectIndex()
        {
            ElasticClient.CatAliases(cat => cat.Name(TestIndex.AliasName)).Records.Should().HaveCount(1);

            new RemoveAliasOperation(TestIndex.AliasName)
                .FromIndex(TestIndex.IndexNameWithVersion())
                .Execute(ElasticClient);

            ElasticClient.CatAliases(cat => cat.Name(TestIndex.AliasName)).Records.Should().HaveCount(0);
        }

        [Test]
        public void RemoveAlias_ValidatesSettings()
        {
            var customAlias = TestIndex.AliasName + "-custom";
            Assert.Throws<ElasticUpException>(() => new RemoveAliasOperation(null).FromIndex(TestIndex.IndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new RemoveAliasOperation(" ").FromIndex(TestIndex.IndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new RemoveAliasOperation(customAlias).FromIndex("index-does-not-exist").Execute(ElasticClient));
        }
    }
}