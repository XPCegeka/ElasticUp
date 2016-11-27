using System.Linq;
using ElasticUp.History;
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
            var sampleObjects = Enumerable.Range(1, 25000).Select(n => new SampleObject {Number = n}).ToList();
            ElasticClient.IndexMany(sampleObjects, TestIndex.AliasName);
            ElasticClient.PutAlias(TestIndex.IndexNameWithVersion(), TestIndex.AliasName);
            ElasticClient.Refresh(Indices.All);

            // TEST
            new ElasticUp(ElasticClient)
                .Migration(new SampleMigrationWithCopyTypeOperation(0))
                .Run();

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var objectCountInNewIndex = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion())).Count;
            objectCountInNewIndex.Should().Be(sampleObjects.Count);

            var migrationHistoryCountInNewIndex = ElasticClient.Count<ElasticUpMigrationHistory>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion())).Count;
            migrationHistoryCountInNewIndex.Should().Be(1);
            
            var indicesPointingToAlias = ElasticClient.GetIndicesPointingToAlias(TestIndex.AliasName);
            indicesPointingToAlias.Should().HaveCount(1);
            indicesPointingToAlias[0].Should().Be(TestIndex.NextIndexNameWithVersion());
        }

        [Test]
        public void ElasticUp_FullStackTest_FromIndexWithPriorMigrations()
        {
            // GIVEN
            var sampleObjects = Enumerable.Range(1, 25000).Select(n => new SampleObject { Number = n }).ToList();
            ElasticClient.IndexMany(sampleObjects, TestIndex.AliasName);
            ElasticClient.Index(new ElasticUpMigrationHistory {ElasticUpMigrationName = "Sample"}, descriptor => descriptor.Index(TestIndex.IndexNameWithVersion()));
            ElasticClient.Refresh(Indices.All);

            // TEST
            new ElasticUp(ElasticClient)
                .Migration(new SampleMigrationWithCopyTypeOperation(0))
                .Run();

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var objCountInNewIndex = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion())).Count;
            objCountInNewIndex.Should().Be(sampleObjects.Count);

            var migrationHistoryCountInNewIndex = ElasticClient.Count<ElasticUpMigrationHistory>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion())).Count;
            migrationHistoryCountInNewIndex.Should().Be(2);

            var indicesPointingToAlias = ElasticClient.GetIndicesPointingToAlias(TestIndex.AliasName);
            indicesPointingToAlias.Should().HaveCount(1);
            indicesPointingToAlias[0].Should().Be(TestIndex.NextIndexNameWithVersion());
        }
    }
}