using ElasticUp.Helper;
using ElasticUp.Operation.Index;
using ElasticUp.Util;
using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Index
{
    [TestFixture]
    public class DeleteIndexOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void DeletesIndex()
        {
            var customIndexName = TestIndex.IndexNameWithVersion() + "-custom";
            ElasticClient.CreateIndex(customIndexName);
            new IndexHelper(ElasticClient).IndexExists(customIndexName).Should().BeTrue();

            new DeleteIndexOperation(customIndexName).Execute(ElasticClient);

            new IndexHelper(ElasticClient).IndexDoesNotExist(customIndexName).Should().BeTrue();
        }

        [Test]
        public void DeleteIndex_ValidatesSettings()
        {
            var customIndexName = TestIndex.IndexNameWithVersion() + "-custom";
            Assert.Throws<ElasticUpException>(() => new DeleteIndexOperation(null).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new DeleteIndexOperation(" ").Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new DeleteIndexOperation(customIndexName).Validate(ElasticClient));
        }
    }
}