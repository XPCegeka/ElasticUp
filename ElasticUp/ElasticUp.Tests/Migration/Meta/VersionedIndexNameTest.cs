using ElasticUp.Migration.Meta;
using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Migration.Meta
{
    [TestFixture]
    public class VersionedIndexNameTest
    {
        [Test]
        public void CreateFromVersionedIndexName_ReturnsParsedVersionedIndexName()
        {
            var indexName = VersionedIndexName.CreateFromIndexName("test-v1");
            indexName.Name.Should().Be("test");
            indexName.Version.Should().Be(1);
        }

        [Test]
        public void CreateFromVersionedIndexName_ReturnsVersion0ForNonVersionedIndexName()
        {
            var indexName = VersionedIndexName.CreateFromIndexName("cegeka-is-great");
            indexName.Name.Should().Be("cegeka-is-great");
            indexName.Version.Should().Be(0);
        }

        [Test]
        public void ToString_ReturnsFormattedVersionedIndexName()
        {
            var indexName = new VersionedIndexName("test", 1);
            indexName.ToString().Should().Be("test-v1");
        }

        [Test]
        public void GetIncrementedVersion_ReturnsNextVersion()
        {
            var indexName = new VersionedIndexName("test", 1);
            var nextIndexName = indexName.GetIncrementedVersion();

            nextIndexName.Name.Should().Be("test");
            nextIndexName.Version.Should().Be(2);
            nextIndexName.ToString().Should().Be("test-v2");
        }
    }
}