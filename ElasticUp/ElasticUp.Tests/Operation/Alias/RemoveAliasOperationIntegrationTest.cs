using ElasticUp.Helper;
using ElasticUp.Operation.Alias;
using ElasticUp.Util;
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
            new AliasHelper(ElasticClient).AliasExistsOnIndex(TestIndex).Should().BeTrue();

            //WHEN
            new RemoveAliasOperation(TestIndex.AliasName)
                .FromIndex(TestIndex.IndexNameWithVersion())
                .Execute(ElasticClient);

            //THEN
            new AliasHelper(ElasticClient).AliasDoesNotExistOnIndex(TestIndex).Should().BeTrue();
        }

        [Test]
        public void RemoveAlias_ValidatesSettings()
        {
            var customAlias = TestIndex.AliasName + "-custom";
            Assert.Throws<ElasticUpException>(() => new RemoveAliasOperation(null).FromIndex(TestIndex.IndexNameWithVersion()).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new RemoveAliasOperation(" ").FromIndex(TestIndex.IndexNameWithVersion()).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new RemoveAliasOperation(customAlias).FromIndex("index-does-not-exist").Validate(ElasticClient));
        }
    }
}