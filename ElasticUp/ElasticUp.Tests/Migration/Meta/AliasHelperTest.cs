using System.Linq;
using ElasticUp.Migration.Meta;
using ElasticUp.Tests.Infrastructure;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Migration.Meta
{
    [TestFixture]
    public class AliasHelperTest : AbstractIntegrationTest
    {
        private ElasticSearchContainer _setupElasticSearchService;
        private IElasticClient _elasticClient = new ElasticClient();

        [SetUp]
        public void SetUp()
        {
            _setupElasticSearchService = SetupElasticSearchService();
        }

        [TearDown]
        public void TearDown()
        {
            _setupElasticSearchService.Dispose();
        }

        [Test]
        public void AddAliasOnIndices_CreatesNewAliasOnGivenIndex()
        {
            // GIVEN
            var sampleObjects = Enumerable
                .Range(1, 100)
                .Select(n => new SampleObject {Number = n});
            _elasticClient.IndexMany(sampleObjects, index: "SampleIndex");

            // TEST
            var aliasHelper = new AliasHelper(_elasticClient);
            aliasHelper.AddAliasOnIndices("SampleAlias", "SampleIndex");

            // VERIFY
            var indicesPointingToAlias = _elasticClient.GetIndicesPointingToAlias("SampleAlias");
            indicesPointingToAlias.Should().HaveCount(1);
            indicesPointingToAlias[0].Should().Be("SampleIndex");
        }

        class SampleObject
        {
            public int Number { get; set; }
        }
    }
}