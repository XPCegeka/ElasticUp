using ElasticUp.Helper;
using ElasticUp.Operation.Alias;
using ElasticUp.Util;
using FluentAssertions;
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

            //THEN
            new AliasHelper(ElasticClient).AliasExistsOnIndex("my-custom-alias", TestIndex.IndexNameWithVersion()).Should().BeTrue();
        }

        [Test]
        public void CreateAlias_ValidatesSettings()
        {
            Assert.Throws<ElasticUpException>(() => new CreateAliasOperation(null).OnIndex(TestIndex.IndexNameWithVersion()).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateAliasOperation(" ").OnIndex(TestIndex.IndexNameWithVersion()).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CreateAliasOperation(TestIndex.AliasName + "-custom").OnIndex("index-does-not-exist").Validate(ElasticClient));
        }
    }
}