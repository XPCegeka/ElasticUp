using System;
using System.Collections.Generic;
using System.Linq;
using ElasticUp.Elastic;
using ElasticUp.History;
using ElasticUp.Migration;
using ElasticUp.Operation;
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
            const string fromIndex = "from";
            const string toIndex = "to";

            var migrationHistory = Enumerable.Range(1, 10)
                .Select(n => new ElasticUpMigrationHistory
                {
                    ElasticUpMigrationName = $"SampleMigration-{n}",
                    ElasticUpMigrationApplied = DateTime.UtcNow
                }).ToList();

            ElasticClient.IndexMany(migrationHistory, fromIndex);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            migrationHistoryHelper.CopyMigrationHistory(fromIndex, toIndex);

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var actualMigrationHistory = ElasticClient.Search<ElasticUpMigrationHistory>(descriptor => descriptor.Index(toIndex));
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
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            migrationHistoryHelper.CopyMigrationHistory(fromIndex, toIndex);

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var actualMigrationHistory = ElasticClient.Count<ElasticUpMigrationHistory>(descriptor => descriptor.Index(toIndex));
            actualMigrationHistory.Count.Should().Be(0);
        }

        [Test]
        public void AddMigrationHistory_AddsMigrationHistoryForExecutedOperation()
        {
            // GIVEN
            const string toIndex = "to";

            var migration = new SampleEmptyMigration(0);

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            migrationHistoryHelper.AddMigrationToHistory(migration, toIndex);

            // VERIFY
            ElasticClient.Refresh(Indices.All);

            var historyFromElastic = ElasticClient.Search<ElasticUpMigrationHistory>(sd => sd.Index(toIndex).Query(q => q.Term(t => t.ElasticUpMigrationName, migration.ToString()))).Documents.ToList();
            historyFromElastic.Should().HaveCount(1);
            historyFromElastic[0].ElasticUpMigrationName.Should().Be(migration.ToString());
        }

        [Test]
        public void AddMigrationHistory_WithException_AddsMigrationHistoryWithExceptionForExecutedOperation()
        {
            // GIVEN
            const string toIndex = "to";

            var migration = new SampleEmptyMigration(0);
            var exception = new Exception("Sample");

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            migrationHistoryHelper.AddMigrationToHistory(migration, toIndex, exception);

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            
            var historyFromElastic = ElasticClient.Search<ElasticUpMigrationHistory>(sd => sd.Index(toIndex).Query(q => q.Term(t => t.ElasticUpMigrationName, migration.ToString()))).Documents.ToList();
            historyFromElastic.Should().HaveCount(1);
            historyFromElastic[0].ElasticUpMigrationName.Should().Be(migration.ToString());
            historyFromElastic[0].ElasticUpMigrationException.Message.Should().Be(exception.Message);
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ReturnsTrueIfMigrationAlreadyApplied()
        {
            // GIVEN
            const string toIndex = "to";

            var migration = new SampleEmptyMigration(0);
            var migrationHistory = new ElasticUpMigrationHistory(migration.ToString());

            ElasticClient.Index(migrationHistory, descriptor => descriptor.Index(toIndex));
            ElasticClient.Refresh(Indices.All);

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            var hasMigrationAlreadyBeenApplied = migrationHistoryHelper.HasMigrationAlreadyBeenApplied(migration, toIndex);

            // VERIFY
            hasMigrationAlreadyBeenApplied.Should().BeTrue();
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ReturnsFalseIfMigrationNotApplied()
        {
            // GIVEN
            const string toIndex = "to";

            var migration = new SampleEmptyMigration(0);

            ElasticClient.CreateIndex(toIndex);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            var hasMigrationAlreadyBeenApplied = migrationHistoryHelper.HasMigrationAlreadyBeenApplied(migration, toIndex);

            // VERIFY
            hasMigrationAlreadyBeenApplied.Should().BeFalse();
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ReturnsFalseIfMigrationNotSucessfullyApplied()
        {
            // GIVEN
            const string toIndex = "to";

            var migration = new SampleEmptyMigration(0);
            var migrationHistory = new ElasticUpMigrationHistory(migration.ToString(), new Exception());

            ElasticClient.Index(migrationHistory, descriptor => descriptor.Index(toIndex));
            ElasticClient.Refresh(Indices.All);

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            var hasMigrationAlreadyBeenApplied = migrationHistoryHelper.HasMigrationAlreadyBeenApplied(migration, toIndex);

            // VERIFY
            hasMigrationAlreadyBeenApplied.Should().BeFalse();
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ThrowsIfIndexDoesNotExist()
        {
            // GIVEN
            var migration = new SampleEmptyMigration(0);

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient);
            Assert.Throws<ElasticUpException>(() => migrationHistoryHelper.HasMigrationAlreadyBeenApplied(migration, "unknown-index"));
        }
    }
}
