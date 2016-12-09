using System;
using System.Linq;
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
        public void InitMigrationHistory_CreatesMigrationHistoryIndexWithVersionAndAlias()
        {
            ElasticClient.DeleteIndex(MigrationHistoryTestIndex.IndexNameWithVersion());
            ElasticClient.IndexExists(MigrationHistoryTestIndex.IndexNameWithVersion()).Exists.Should().BeFalse();
            ElasticClient.AliasExists(descriptor => descriptor.Name(MigrationHistoryTestIndex.AliasName)).Exists.Should().BeFalse();  

            new MigrationHistoryHelper(ElasticClient, MigrationHistoryTestIndex.AliasName).InitMigrationHistory();

            ElasticClient.IndexExists(MigrationHistoryTestIndex.IndexNameWithVersion()).Exists.Should().BeTrue();
            ElasticClient.AliasExists(descriptor => descriptor.Name(MigrationHistoryTestIndex.AliasName)).Exists.Should().BeTrue();
        }

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
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient, TestIndex.IndexNameWithVersion());
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
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient, TestIndex.IndexNameWithVersion());
            migrationHistoryHelper.CopyMigrationHistory(TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var actualMigrationHistory = ElasticClient.Count<ElasticUpMigrationHistory>(descriptor => descriptor.Index(TestIndex.IndexNameWithVersion()));
            actualMigrationHistory.Count.Should().Be(0);
            ElasticClient.IndexExists(TestIndex.NextIndexNameWithVersion()).Exists.Should().BeTrue();
        }

        [Test]
        public void AddMigrationHistory_AddsMigrationHistoryForExecutedOperation()
        {
            // GIVEN
            var migration = new SampleEmptyMigration(TestIndex.IndexNameWithVersion());

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient, MigrationHistoryTestIndex.AliasName);
            migrationHistoryHelper.AddMigrationToHistory(migration);

            // VERIFY
            ElasticClient.Refresh(Indices.All);

            var historyFromElastic = ElasticClient.Search<ElasticUpMigrationHistory>(sd => sd.Index(MigrationHistoryTestIndex.AliasName).Query(q => q.Term(t => t.ElasticUpMigrationName, migration.ToString()))).Documents.ToList();
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
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient, MigrationHistoryTestIndex);
            migrationHistoryHelper.AddMigrationToHistory(migration, exception);

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            
            var historyFromElastic = ElasticClient.Search<ElasticUpMigrationHistory>(sd => sd.Index(MigrationHistoryTestIndex.AliasName).Query(q => q.Term(t => t.ElasticUpMigrationName, migration.ToString()))).Documents.ToList();
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

            ElasticClient.Index(migrationHistory, descriptor => descriptor.Index(MigrationHistoryTestIndex.AliasName));
            ElasticClient.Refresh(Indices.All);

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient, MigrationHistoryTestIndex);
            var hasMigrationAlreadyBeenApplied = migrationHistoryHelper.HasMigrationAlreadyBeenApplied(migration);

            // VERIFY
            hasMigrationAlreadyBeenApplied.Should().BeTrue();
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ReturnsFalseIfMigrationNotApplied()
        {
            // GIVEN
            var migration = new SampleEmptyMigration(TestIndex.IndexNameWithVersion());
            
            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient, MigrationHistoryTestIndex);
            var hasMigrationAlreadyBeenApplied = migrationHistoryHelper.HasMigrationAlreadyBeenApplied(migration);

            // VERIFY
            hasMigrationAlreadyBeenApplied.Should().BeFalse();
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ReturnsFalseIfMigrationNotSucessfullyApplied()
        {
            // GIVEN
            var migration = new SampleEmptyMigration(TestIndex.IndexNameWithVersion());
            var migrationHistory = new ElasticUpMigrationHistory(migration.ToString(), new Exception());

            ElasticClient.Index(migrationHistory, descriptor => descriptor.Index(MigrationHistoryTestIndex.AliasName));
            ElasticClient.Refresh(Indices.All);

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient, MigrationHistoryTestIndex);
            var hasMigrationAlreadyBeenApplied = migrationHistoryHelper.HasMigrationAlreadyBeenApplied(migration);

            // VERIFY
            hasMigrationAlreadyBeenApplied.Should().BeFalse();
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_FalseIfMigrationIndexDoesNotExist()
        {
            // GIVEN
            ElasticClient.DeleteIndex(MigrationHistoryTestIndex.AliasName);
            ElasticClient.Refresh(Indices.All);

            var migration = new SampleEmptyMigration(TestIndex.IndexNameWithVersion());

            // TEST
            var migrationHistoryHelper = new MigrationHistoryHelper(ElasticClient, MigrationHistoryTestIndex);
            migrationHistoryHelper.HasMigrationAlreadyBeenApplied(migration).Should().BeFalse();
        }
    }
}
