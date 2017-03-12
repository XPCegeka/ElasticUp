using Elasticsearch.Net;
using ElasticUp.Alias;
using ElasticUp.Extension;
using ElasticUp.Tests.Sample;
using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Extension
{
    [TestFixture]
    public class ReadOnlyIndexContextTest : AbstractIntegrationTest
    {
        [Test]
        public void WhenUsing_ThrowsWhenIndexingDocument()
        {
            // GIVEN
            var indexName = TestIndex.IndexNameWithVersion();
            
            // TEST
            using (new ReadOnlyIndexContext(ElasticClient, indexName))
            {
                Assert.Throws<ElasticsearchClientException>(() =>
                    ElasticClient.Index(new SampleObject(), descriptor => descriptor.Index(indexName)));
            }
        }

        [Test]
        public void WhenUsing_SetsIndexToReadOnly()
        {
            var indexName = TestIndex.IndexNameWithVersion();

            using (new ReadOnlyIndexContext(ElasticClient, indexName))
            {
                var indexSettings = ElasticClient.GetIndexSettings(descriptor => descriptor.Index(indexName));
                indexSettings.Indices[indexName].Settings.BlocksReadOnly.Should().BeTrue();
            }
        }
    }
}
