using System;
using System.Linq;
using ElasticUp.Helper;
using ElasticUp.History;
using ElasticUp.Tests.Sample;
using ElasticUp.Util;
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
            ElasticClient.IndexExists(MigrationHistoryTestIndex.IndexNameWithVersion()).Exists.Should().BeFalse();
            
            new MigrationHistoryHelper(ElasticClient, MigrationHistoryTestIndex.AliasName).InitMigrationHistory();

            ElasticClient.IndexExists(MigrationHistoryTestIndex.IndexNameWithVersion()).Exists.Should().BeTrue();
            ElasticClient.AliasExists(descriptor => descriptor
                            .Index(MigrationHistoryTestIndex.IndexNameWithVersion())
                            .Name(MigrationHistoryTestIndex.AliasName))
                            .Exists.Should().BeTrue();
        }

        [Test]
        public void InitMigrationHistory_ThrowsExceptionIfAliasDoesNotExistButIndexAlreadyExists_TheyShouldEitherBothExistsOrNoneOfBotch()
        {
            new AliasHelper(ElasticClient).RemoveAliasFromIndex(MigrationHistoryTestIndex.AliasName, MigrationHistoryTestIndex.IndexNameWithVersion());
            ElasticClient.IndexExists(MigrationHistoryTestIndex.IndexNameWithVersion()).Exists.Should().BeTrue();
            ElasticClient.AliasExists(descriptor => descriptor.Name(MigrationHistoryTestIndex.AliasName)).Exists.Should().BeFalse();  

            Assert.Throws<ElasticUpException>(() => new MigrationHistoryHelper(ElasticClient, MigrationHistoryTestIndex.AliasName).InitMigrationHistory());
        }

        [Test]
        public void CopyMigrationHistory_CopiesExistingMigrationHistoryFromOneIndexToAnother()
        {
            var migrationHistory = Enumerable.Range(1, 10)
                .Select(n => new ElasticUpMigrationHistory
                {
                    ElasticUpMigrationName = $"SampleMigration-{n}",
                    ElasticUpMigrationApplied = DateTime.UtcNow
                }).ToList();

            ElasticClient.IndexMany(migrationHistory, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);

            new MigrationHistoryHelper(ElasticClient, TestIndex.IndexNameWithVersion())
                .CopyMigrationHistory(TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());

            ElasticClient.Refresh(Indices.All);
            ElasticClient
                .Search<ElasticUpMigrationHistory>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()))
                .Documents.ShouldBeEquivalentTo(migrationHistory);
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
            var migration = new SampleEmptyVersionedIndexMigration(TestIndex.IndexNameWithVersion());

            new MigrationHistoryHelper(ElasticClient, MigrationHistoryTestIndex.AliasName)
                .AddMigrationToHistory(migration);

            ElasticClient.Refresh(Indices.All);
            var historyFromElastic = ElasticClient.Search<ElasticUpMigrationHistory>(sd => sd.Index(MigrationHistoryTestIndex.AliasName).Query(q => q.Term(t => t.ElasticUpMigrationName, migration.ToString()))).Documents.ToList();
            historyFromElastic.Should().HaveCount(1);
            historyFromElastic[0].ElasticUpMigrationName.Should().Be(migration.ToString());
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ReturnsTrueIfMigrationAlreadyApplied()
        {
            var migration = new SampleEmptyVersionedIndexMigration(TestIndex.IndexNameWithVersion());
            var migrationHistory = new ElasticUpMigrationHistory { ElasticUpMigrationName = migration.ToString() };

            ElasticClient.Index(migrationHistory, descriptor => descriptor.Index(MigrationHistoryTestIndex.AliasName));
            ElasticClient.Refresh(Indices.All);

            new MigrationHistoryHelper(ElasticClient, MigrationHistoryTestIndex)
                .HasMigrationAlreadyBeenApplied(migration)
                .Should().BeTrue();
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ReturnsFalseIfMigrationNotApplied()
        {
            var migration = new SampleEmptyVersionedIndexMigration(TestIndex.IndexNameWithVersion());

            new MigrationHistoryHelper(ElasticClient, MigrationHistoryTestIndex)
                .HasMigrationAlreadyBeenApplied(migration)
                .Should().BeFalse();
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_FalseIfMigrationIndexDoesNotExist()
        {
            ElasticClient.DeleteIndex(MigrationHistoryTestIndex.AliasName);
            ElasticClient.Refresh(Indices.All);

            var migration = new SampleEmptyVersionedIndexMigration(TestIndex.IndexNameWithVersion());

            new MigrationHistoryHelper(ElasticClient, MigrationHistoryTestIndex)
                .HasMigrationAlreadyBeenApplied(migration)
                .Should().BeFalse();
        }
    }
}
