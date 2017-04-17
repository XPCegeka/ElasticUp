using System.Collections.Generic;
using System.Linq;
using ElasticUp.Extension;
using ElasticUp.Tests.Sample;
using ElasticUp.Util;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Extension
{
    [TestFixture]
    public class ElasticClientExtensionsTest : AbstractIntegrationTest
    {
        [Test]
        public void DoScrollAsync_ReturnsDocuments()
        {
            // GIVEN
            const string index = "index";
            var documents = Enumerable.Range(0, 5000).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(documents, index);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var actualDocuments = new List<SampleObject>();
            ElasticClient.DoScrollAsync<SampleObject>(descriptor => descriptor.Index(index).MatchAll(), objects => actualDocuments.AddRange(objects)).Wait();

            // VERIFY
            actualDocuments.ShouldBeEquivalentTo(actualDocuments);
        }

        [Test]
        public void SetIndexBlocksReadOnly_SetsIndexAsReadonly()
        {
            var indexName = TestIndex.IndexNameWithVersion();

            ElasticClient.SetIndexBlocksReadOnly(indexName, true);

            var indexSettings = ElasticClient.GetIndexSettings(descriptor => descriptor.Index(indexName));
            indexSettings.Indices[indexName].Settings.BlocksReadOnly.Should().BeTrue();


            ElasticClient.SetIndexBlocksReadOnly(indexName, false);

            indexSettings = ElasticClient.GetIndexSettings(descriptor => descriptor.Index(indexName));
            indexSettings.Indices[indexName].Settings.BlocksReadOnly.Should().BeFalse();
        }

        [Test]
        public void SetIndexBlocksReadOnly_ThrowsWhenIndexNameNullOrEmpty()
        {
            Assert.Throws<ElasticUpException>(() => ElasticClient.SetIndexBlocksReadOnly(null, true));
            Assert.Throws<ElasticUpException>(() => ElasticClient.SetIndexBlocksReadOnly("", true));
        }
    }
}
