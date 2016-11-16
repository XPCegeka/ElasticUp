using System.Linq;
using ElasticUp.History;
using ElasticUp.Migration.Meta;
using ElasticUp.Tests.Sample;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests
{
    [TestFixture]
    public class ElasticUpIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void ElasticUp_FullStackTest_FromIndexWithoutPriorMigrations()
        {
            // GIVEN
            const string aliasName = "sample-index";
            var oldIndex = new VersionedIndexName(aliasName, 1);
            var newIndex = oldIndex.GetIncrementedVersion();
            var oldIndexName = oldIndex.ToString();
            var newIndexName = newIndex.ToString();

            var sampleObjects = Enumerable.Range(1, 25000).Select(n => new SampleObject {Number = n}).ToList();
            ElasticClient.IndexMany(sampleObjects, oldIndexName);
            ElasticClient.PutAlias(oldIndexName, aliasName);
            ElasticClient.Refresh(Indices.All);

            // TEST
            new ElasticUp(ElasticClient)
                .Migration(new SampleMigrationWithCopyTypeOperation(0))
                .Run();

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var objCountInNewIndex = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(newIndexName)).Count;
            objCountInNewIndex.Should().Be(sampleObjects.Count);

            var migrationHistoryCountInNewIndex = ElasticClient.Count<MigrationHistory>(descriptor => descriptor.Index(newIndexName)).Count;
            migrationHistoryCountInNewIndex.Should().Be(1);
            
            var indicesPointingToAlias = ElasticClient.GetIndicesPointingToAlias(aliasName);
            indicesPointingToAlias.Should().HaveCount(1);
            indicesPointingToAlias[0].Should().Be(newIndexName);
        }

        [Test]
        public void ElasticUp_FullStackTest_FromIndexWithPriorMigrations()
        {
            // GIVEN
            const string aliasName = "sample-index";
            var oldIndex = new VersionedIndexName(aliasName, 1);
            var newIndex = oldIndex.GetIncrementedVersion();
            var oldIndexName = oldIndex.ToString();
            var newIndexName = newIndex.ToString();

            var sampleObjects = Enumerable.Range(1, 25000).Select(n => new SampleObject { Number = n }).ToList();
            ElasticClient.IndexMany(sampleObjects, oldIndexName);
            ElasticClient.PutAlias(oldIndexName, aliasName);

            ElasticClient.Index(new MigrationHistory {Id = "Sample"}, descriptor => descriptor.Index(oldIndexName));

            ElasticClient.Refresh(Indices.All);

            // TEST
            new ElasticUp(ElasticClient)
                .Migration(new SampleMigrationWithCopyTypeOperation(0))
                .Run();

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var objCountInNewIndex = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(newIndexName)).Count;
            objCountInNewIndex.Should().Be(sampleObjects.Count);

            var migrationHistoryCountInNewIndex = ElasticClient.Count<MigrationHistory>(descriptor => descriptor.Index(newIndexName)).Count;
            migrationHistoryCountInNewIndex.Should().Be(2);

            var indicesPointingToAlias = ElasticClient.GetIndicesPointingToAlias(aliasName);
            indicesPointingToAlias.Should().HaveCount(1);
            indicesPointingToAlias[0].Should().Be(newIndexName);
        }
    }
}