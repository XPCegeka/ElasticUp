using ElasticUp.Util;
using ElasticUp.Validations;
using NUnit.Framework;

namespace ElasticUp.Tests.Validations
{
    [TestFixture]
    public class IndexValidationsIntegrationTest : AbstractIntegrationTest
    {
        private IndexValidations _indexValidations;

        [SetUp]
        public void Setup()
        {
            _indexValidations = IndexValidations.IndexValidationsFor<IndexValidationsIntegrationTest>(ElasticClient);
        }

        [Test]
        public void ValidateIndexExists_ThrowExceptionIfNotExists()
        {
            Assert.DoesNotThrow(() => _indexValidations.IndexExists(TestIndex.IndexNameWithVersion()));
            Assert.Throws<ElasticUpException>(() => _indexValidations.IndexExists("fantasy-index"));
        }

        [Test]
        public void ValidateIndexWithAliasExists_ThrowExceptionIfIndexOrAliasDoesNotExist()
        {
            Assert.DoesNotThrow(() => _indexValidations.IndexExistsWithAlias(TestIndex));
            Assert.Throws<ElasticUpException>(() => _indexValidations.IndexExistsWithAlias(TestIndex.NextVersion()));
            Assert.Throws<ElasticUpException>(() => _indexValidations.IndexExistsWithAlias(VersionedIndexName.CreateFromIndexName("fantasy-index")));
        }
    }
}