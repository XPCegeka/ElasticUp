using Elasticsearch.Net;
using ElasticUp.Helper;
using ElasticUp.Util;
using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Helper
{
    [TestFixture]
    public class AliasHelperIntegrationTest : AbstractIntegrationTest
    {
        private AliasHelper _aliasHelper;

        [SetUp]
        public void Setup()
        {
            _aliasHelper = new AliasHelper(ElasticClient);
        }

        [Test]
        public void PutAliasOnIndex_CreatesNewAliasOnGivenIndex()
        {
            _aliasHelper.AliasDoesNotExistOnIndex(TestIndex.AliasName, TestIndex.NextIndexNameWithVersion()).Should().BeTrue();

            //WHEN
            _aliasHelper.PutAliasOnIndex(TestIndex.AliasName, TestIndex.NextIndexNameWithVersion());
            
            //THEN
            _aliasHelper.AliasExistsOnIndex(TestIndex.AliasName, TestIndex.NextIndexNameWithVersion()).Should().BeTrue();
        }

        [Test]
        public void PutAliasOnIndex_ThrowsExceptionWhenAliasCreationFails()
        {
            Assert.Throws<ElasticsearchClientException>(() => _aliasHelper.PutAliasOnIndex(TestIndex.AliasName, "unknown index"));
        }

        [Test]
        public void RemoveAliasFromIndex_DeletesAliasFromGivenIndex()
        {
            _aliasHelper.PutAliasOnIndex(TestIndex.AliasName, TestIndex.IndexNameWithVersion());
            _aliasHelper.AliasExistsOnIndex(TestIndex.AliasName, TestIndex.IndexNameWithVersion()).Should().BeTrue();

            //WHEN
            _aliasHelper.RemoveAliasFromIndex(TestIndex.AliasName, TestIndex.IndexNameWithVersion());

            //THEN
            _aliasHelper.AliasDoesNotExistOnIndex(TestIndex.AliasName, TestIndex.IndexNameWithVersion()).Should().BeTrue();
        }

        [Test]
        public void RemoveAliasFromIndex_ThrowsExceptionWhenAliasDeletionFails()
        {
            Assert.Throws<ElasticsearchClientException>(() => _aliasHelper.RemoveAliasFromIndex("unknown alias", TestIndex.IndexNameWithVersion()));
        }

        [Test]
        public void SwitchAlias_RemovesAliasFromOldIndexAndPutsAliasOnNewIndex()
        {
            //WHEN
            _aliasHelper.SwitchAlias(TestIndex.AliasName, TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());
            
            //THEN
            _aliasHelper.AliasExistsOnIndex(TestIndex.AliasName, TestIndex.NextIndexNameWithVersion()).Should().BeTrue();
            _aliasHelper.AliasDoesNotExistOnIndex(TestIndex.AliasName, TestIndex.IndexNameWithVersion()).Should().BeTrue();
        }

        [Test]
        public void AliasExistsOnIndex()
        {
            _aliasHelper.AliasExistsOnIndex(TestIndex.AliasName, TestIndex.IndexNameWithVersion()).Should().BeTrue();
            _aliasHelper.AliasExistsOnIndex("unexisting-alias", TestIndex.IndexNameWithVersion()).Should().BeFalse();
            _aliasHelper.AliasExistsOnIndex(TestIndex.AliasName, TestIndex.NextIndexNameWithVersion()).Should().BeFalse();
            _aliasHelper.AliasExistsOnIndex(TestIndex.AliasName, "unexisting-index").Should().BeFalse();

            _aliasHelper.AliasExistsOnIndex(TestIndex).Should().BeTrue();
            _aliasHelper.AliasExistsOnIndex(new VersionedIndexName("unexisting",0)).Should().BeFalse(); //because index does not exist either
        }

        [Test]
        public void AliasDoesNotExistOnIndex()
        {
            _aliasHelper.AliasDoesNotExistOnIndex(TestIndex.AliasName, TestIndex.IndexNameWithVersion()).Should().BeFalse();
            _aliasHelper.AliasDoesNotExistOnIndex("unexisting-alias", TestIndex.IndexNameWithVersion()).Should().BeTrue();
            _aliasHelper.AliasDoesNotExistOnIndex(TestIndex.AliasName, TestIndex.NextIndexNameWithVersion()).Should().BeTrue();
            _aliasHelper.AliasDoesNotExistOnIndex(TestIndex.AliasName, "unexisting-index").Should().BeTrue();

            _aliasHelper.AliasDoesNotExistOnIndex(TestIndex).Should().BeFalse();
            _aliasHelper.AliasDoesNotExistOnIndex(new VersionedIndexName("unexisting", 0)).Should().BeTrue(); //index does not exist so alias doesn't exist either
        }
    }
}