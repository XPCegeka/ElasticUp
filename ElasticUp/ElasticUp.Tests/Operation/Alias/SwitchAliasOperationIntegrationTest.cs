using ElasticUp.Helper;
using ElasticUp.Operation.Alias;
using ElasticUp.Util;
using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Alias
{
    [TestFixture]
    public class SwitchAliasOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void SwitchAlias_MovesAliasToCorrectIndex()
        {
            new AliasHelper(ElasticClient).AliasExistsOnIndex(TestIndex.AliasName, TestIndex.IndexNameWithVersion()).Should().BeTrue();
            new AliasHelper(ElasticClient).AliasDoesNotExistOnIndex(TestIndex.AliasName, TestIndex.NextIndexNameWithVersion()).Should().BeTrue();

            new SwitchAliasOperation(TestIndex.AliasName)
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .Execute(ElasticClient);

            new AliasHelper(ElasticClient).AliasDoesNotExistOnIndex(TestIndex.AliasName, TestIndex.IndexNameWithVersion()).Should().BeTrue();
            new AliasHelper(ElasticClient).AliasExistsOnIndex(TestIndex.AliasName, TestIndex.NextIndexNameWithVersion()).Should().BeTrue();
        }

        [Test]
        public void SwitchAlias_ValidatesSettings()
        {
            Assert.Throws<ElasticUpException>(() => new SwitchAliasOperation(null).FromIndex(TestIndex.IndexNameWithVersion()).ToIndex(TestIndex.NextIndexNameWithVersion()).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new SwitchAliasOperation(" ").FromIndex(TestIndex.IndexNameWithVersion()).ToIndex(TestIndex.NextIndexNameWithVersion()).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new SwitchAliasOperation(TestIndex.AliasName + "-custom").FromIndex("index-does-not-exist").ToIndex(TestIndex.NextIndexNameWithVersion()).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new SwitchAliasOperation(TestIndex.AliasName + "-custom").FromIndex(TestIndex.IndexNameWithVersion()).ToIndex("index-does-not-exist").Validate(ElasticClient));
        }
    }
}