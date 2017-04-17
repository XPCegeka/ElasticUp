using ElasticUp.Elastic;
using ElasticUp.Operation.Validations;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Validator
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
        public void GivenExistingIndex_WhenValidatingIfIndexExists_DoesNotThrowException()
        {
            Assert.DoesNotThrow(() => _indexValidations.IndexExists(TestIndex.IndexNameWithVersion()));
        }

        [Test]
        public void GivenUnexistingIndex_WhenValidatingIfIndexExists_DoesThrowException()
        {
            Assert.Throws<ElasticUpException>(() => _indexValidations.IndexExists("fantasy-index"));
        }


    }
}