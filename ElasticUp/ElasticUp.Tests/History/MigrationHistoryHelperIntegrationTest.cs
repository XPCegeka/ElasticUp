using System;
using System.Linq;
using Elasticsearch.Net;
using ElasticUp.History;
using ElasticUp.Tests.Sample;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.History
{
    [TestFixture]
    public class MigrationHistoryHelperIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void CopyMigrationHistory_CopiesExistingMigrationHistoryFromOneIndexToAnother()
        {
            // GIVEN
            var migrationHistory = Enumerable
                .Range(1, 10)
                .Select(n => new ElasticUpMigrationHistory
                {
                    ElasticUpMigrationName = $"SampleMigration-{n}",
                    ElasticUpMigrationApplied = DateTime.UtcNow
                }).ToList();

            ElasticClient.IndexMany(migrationHistory, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            migrationHistoryHelper.CopyMigrationHistory(TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var actualMigrationHistory = ElasticClient.Search<ElasticUpMigrationHistory>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()));
            actualMigrationHistory.Documents.ShouldBeEquivalentTo(migrationHistory);
        }

        [Test]
        public void CopyMigrationHistory_DoesNotThrowWhenNoMigrationHistoryInFromIndex()
        {
            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            migrationHistoryHelper.CopyMigrationHistory(TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var actualMigrationHistory = ElasticClient.Count<ElasticUpMigrationHistory>(descriptor => descriptor.Index(TestIndex.IndexNameWithVersion()));
            actualMigrationHistory.Count.Should().Be(0);
            ElasticClient.IndexExists(TestIndex.NextIndexNameWithVersion()).Exists.Should().BeFalse();
        }

        [Test]
        public void AddMigrationHistory_AddsMigrationHistoryForExecutedOperation()
        {
            // GIVEN
            var migration = new SampleEmptyMigration(TestIndex.IndexNameWithVersion());

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            migrationHistoryHelper.AddMigrationToHistory(migration, TestIndex.NextIndexNameWithVersion());

            // VERIFY
            ElasticClient.Refresh(Indices.All);

            var historyFromElastic = ElasticClient.Search<ElasticUpMigrationHistory>(sd => sd.Index(TestIndex.NextIndexNameWithVersion()).Query(q => q.Term(t => t.ElasticUpMigrationName, migration.ToString()))).Documents.ToList();
            historyFromElastic.Should().HaveCount(1);
            historyFromElastic[0].ElasticUpMigrationName.Should().Be(migration.ToString());
        }

        [Test]
        public void AddMigrationHistory_WithException_AddsMigrationHistoryWithExceptionForExecutedOperation()
        {
            // GIVEN
            var migration = new SampleEmptyMigration(TestIndex.IndexNameWithVersion());
            var exception = new Exception("Sample");

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            migrationHistoryHelper.AddMigrationToHistory(migration, TestIndex.NextIndexNameWithVersion(), exception);

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            
            var historyFromElastic = ElasticClient.Search<ElasticUpMigrationHistory>(sd => sd.Index(TestIndex.NextIndexNameWithVersion()).Query(q => q.Term(t => t.ElasticUpMigrationName, migration.ToString()))).Documents.ToList();
            historyFromElastic.Should().HaveCount(1);
            historyFromElastic[0].ElasticUpMigrationName.Should().Be(migration.ToString());
            historyFromElastic[0].ElasticUpMigrationException.Message.Should().Be(exception.Message);
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ReturnsTrueIfMigrationAlreadyApplied()
        {
            // GIVEN
            var migration = new SampleEmptyMigration(TestIndex.IndexNameWithVersion());
            var migrationHistory = new ElasticUpMigrationHistory(migration.ToString());

            ElasticClient.Index(migrationHistory, descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()));
            ElasticClient.Refresh(Indices.All);

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            var hasMigrationAlreadyBeenApplied = migrationHistoryHelper.HasMigrationAlreadyBeenApplied(migration, TestIndex.NextIndexNameWithVersion());

            // VERIFY
            hasMigrationAlreadyBeenApplied.Should().BeTrue();
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ReturnsFalseIfMigrationNotApplied()
        {
            // GIVEN
            var migration = new SampleEmptyMigration(TestIndex.IndexNameWithVersion());
            
            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            var hasMigrationAlreadyBeenApplied = migrationHistoryHelper.HasMigrationAlreadyBeenApplied(migration, TestIndex.IndexNameWithVersion());

            // VERIFY
            hasMigrationAlreadyBeenApplied.Should().BeFalse();
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ReturnsFalseIfMigrationNotSucessfullyApplied()
        {
            // GIVEN
            var migration = new SampleEmptyMigration(TestIndex.IndexNameWithVersion());
            var migrationHistory = new ElasticUpMigrationHistory(migration.ToString(), new Exception());

            ElasticClient.Index(migrationHistory, descriptor => descriptor.Index(TestIndex.IndexNameWithVersion()));
            ElasticClient.Refresh(Indices.All);

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            var hasMigrationAlreadyBeenApplied = migrationHistoryHelper.HasMigrationAlreadyBeenApplied(migration, TestIndex.IndexNameWithVersion());

            // VERIFY
            hasMigrationAlreadyBeenApplied.Should().BeFalse();
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ThrowsIfIndexDoesNotExist()
        {
            // GIVEN
            var migration = new SampleEmptyMigration(TestIndex.IndexNameWithVersion());

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            Assert.Throws<ElasticsearchClientException>(() => migrationHistoryHelper.HasMigrationAlreadyBeenApplied(migration, "unknown-index"));
        }
    }
}
