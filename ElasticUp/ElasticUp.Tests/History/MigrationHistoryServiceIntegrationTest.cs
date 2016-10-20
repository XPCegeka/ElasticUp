using System;
using System.Linq;
using ElasticUp.History;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.History
{
    [TestFixture]
    public class MigrationHistoryServiceIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void CopyMigrationHistory_CopiesExistingMigrationHistoryFromOneIndexToAnother()
        {
            // GIVEN
            const string fromIndex = "from";
            const string toIndex = "to";

            var migrationHistory = Enumerable.Range(1, 10)
                .Select(n => new MigrationHistory
                {
                    Id = $"SampleMigration-{n}",
                    Applied = DateTime.UtcNow
                }).ToList();

            ElasticClient.IndexMany(migrationHistory, fromIndex);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var migrationHistoryService = new MigrationHistoryService(ElasticClient);
            migrationHistoryService.CopyMigrationHistory(fromIndex, toIndex);

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var actualMigrationHistory = ElasticClient.Search<MigrationHistory>(descriptor => descriptor.Index(toIndex));
            actualMigrationHistory.Documents.ShouldBeEquivalentTo(migrationHistory);
        }


        [Test]
        public void CopyMigrationHistory_DoesNotThrowWhenNoMigrationHistoryInFromIndex()
        {
            // GIVEN
            const string fromIndex = "from";
            const string toIndex = "to";
            
            ElasticClient.CreateIndex(fromIndex);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var migrationHistoryService = new MigrationHistoryService(ElasticClient);
            migrationHistoryService.CopyMigrationHistory(fromIndex, toIndex);

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var actualMigrationHistory = ElasticClient.Count<MigrationHistory>(descriptor => descriptor.Index(toIndex));
            actualMigrationHistory.Count.Should().Be(0);
        }
    }
}
