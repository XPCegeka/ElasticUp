using ElasticUp.Helper;
using ElasticUp.Util;
using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Helper
{
    [TestFixture]
    public class IndexHelperIntegrationTest : AbstractIntegrationTest
    {
        private IndexHelper _indexHelper;

        [SetUp]
        public void Setup()
        {
            _indexHelper = new IndexHelper(ElasticClient);
        }
        
        [Test]
        public void IndexExists()
        {
            _indexHelper.IndexExists(TestIndex.IndexNameWithVersion()).Should().BeTrue();
            _indexHelper.IndexExists("unexisting-index").Should().BeFalse();
            
            _indexHelper.IndexExists(TestIndex).Should().BeTrue();
            _indexHelper.IndexExists(new VersionedIndexName("unexisting",0)).Should().BeFalse();
        }

        [Test]
        public void IndexDoesNotExist()
        {
            _indexHelper.IndexDoesNotExist(TestIndex.IndexNameWithVersion()).Should().BeFalse();
            _indexHelper.IndexDoesNotExist("unexisting-index").Should().BeTrue();
            
            _indexHelper.IndexDoesNotExist(TestIndex).Should().BeFalse();
            _indexHelper.IndexDoesNotExist(new VersionedIndexName("unexisting",0)).Should().BeTrue();
        }

        [Test]
        public void IndexExistsWithAlias()
        {
            _indexHelper.IndexExistsWithAlias(TestIndex.IndexNameWithVersion(), TestIndex.AliasName).Should().BeTrue();
            _indexHelper.IndexExistsWithAlias("unexisting-index", TestIndex.AliasName).Should().BeFalse();
            _indexHelper.IndexExistsWithAlias(TestIndex.IndexNameWithVersion(), "unexisting-alias").Should().BeFalse();

            _indexHelper.IndexExistsWithAlias(TestIndex).Should().BeTrue();
            _indexHelper.IndexExistsWithAlias(new VersionedIndexName("unexisting", 0)).Should().BeFalse();
        }

        [Test]
        public void IndexDoesNotExistWithAlias()
        {
            _indexHelper.IndexExistsWithoutAlias(TestIndex.IndexNameWithVersion(), TestIndex.AliasName).Should().BeFalse();
            _indexHelper.IndexExistsWithoutAlias("unexisting-index", TestIndex.AliasName).Should().BeFalse();
            _indexHelper.IndexExistsWithoutAlias(TestIndex.IndexNameWithVersion(), "unexisting-alias").Should().BeTrue();

            _indexHelper.IndexExistsWithoutAlias(TestIndex).Should().BeFalse();
            _indexHelper.IndexExistsWithoutAlias(new VersionedIndexName("unexisting", 0)).Should().BeFalse();
        }
    }
}