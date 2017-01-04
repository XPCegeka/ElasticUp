using System.Linq;
using ElasticUp.Elastic;
using ElasticUp.Operation.Alias;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Alias
{
    [TestFixture]
    public class SwitchAliasOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void SwitchAlias_MovesAliasToCorrectIndex()
        {
            ElasticClient.GetIndicesPointingToAlias(TestIndex.AliasName).Single().Should().Be(TestIndex.IndexNameWithVersion());

            new SwitchAliasOperation(TestIndex.AliasName)
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .Execute(ElasticClient);

            ElasticClient.GetIndicesPointingToAlias(TestIndex.AliasName).Single().Should().Be(TestIndex.NextIndexNameWithVersion());
        }

        [Test]
        public void SwitchAlias_ValidatesSettings()
        {
            var customAlias = TestIndex.AliasName + "-custom";
            Assert.Throws<ElasticUpException>(() => new SwitchAliasOperation(null).FromIndex(TestIndex.IndexNameWithVersion()).ToIndex(TestIndex.NextIndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new SwitchAliasOperation(" ").FromIndex(TestIndex.IndexNameWithVersion()).ToIndex(TestIndex.NextIndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new SwitchAliasOperation(customAlias).FromIndex("index-does-not-exist").ToIndex(TestIndex.NextIndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new SwitchAliasOperation(customAlias).FromIndex(TestIndex.IndexNameWithVersion()).ToIndex("index-does-not-exist").Execute(ElasticClient));
        }
    }
}