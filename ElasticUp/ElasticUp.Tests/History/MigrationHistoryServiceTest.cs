using System;
using ElasticUp.History;
using ElasticUp.Tests.Sample;
using Nest;
using NSubstitute;
using NUnit.Framework;

namespace ElasticUp.Tests.History
{
    [TestFixture]
    public class MigrationHistoryServiceTest
    {
        private IElasticClient _elasticClient;

        [SetUp]
        public void SetUp()
        {
            _elasticClient = Substitute.For<IElasticClient>();
        }

        [Test]
        public void CopyMigrationHistory_ThrowsWithInvalidParameters()
        {
            var elasticClient = Substitute.For<IElasticClient>();
            var migrationHistoryService = new MigrationHistoryService(_elasticClient);

            Assert.Throws<ArgumentNullException>(() => migrationHistoryService.CopyMigrationHistory(null, "x"));
            Assert.Throws<ArgumentNullException>(() => migrationHistoryService.CopyMigrationHistory(string.Empty, "x"));

            Assert.Throws<ArgumentNullException>(() => migrationHistoryService.CopyMigrationHistory("x", null));
            Assert.Throws<ArgumentNullException>(() => migrationHistoryService.CopyMigrationHistory("x", string.Empty));
        }

        [Test]
        public void AddMigrationHistory_ThrowsWithInvalidParameters()
        {
            var migrationHistoryService = new MigrationHistoryService(_elasticClient);
            Assert.Throws<ArgumentNullException>(() => migrationHistoryService.AddMigrationToHistory(null, "x"));
            Assert.Throws<ArgumentNullException>(() => migrationHistoryService.AddMigrationToHistory(new SampleEmptyMigration(0), null));
            Assert.Throws<ArgumentNullException>(() => migrationHistoryService.AddMigrationToHistory(new SampleEmptyMigration(0), string.Empty));
        }

        [Test]
        public void AddMigrationHistory_WithException_ThrowsWithInvalidParameters()
        {
            var migrationHistoryService = new MigrationHistoryService(_elasticClient);
            Assert.Throws<ArgumentNullException>(() => migrationHistoryService.AddMigrationToHistory(null, "x", null));
            Assert.Throws<ArgumentNullException>(() => migrationHistoryService.AddMigrationToHistory(new SampleEmptyMigration(0), null, null));
            Assert.Throws<ArgumentNullException>(() => migrationHistoryService.AddMigrationToHistory(new SampleEmptyMigration(0), string.Empty, null));
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ThrowsWithInvalidParameters()
        {
            var migrationHistoryService = new MigrationHistoryService(_elasticClient);
            Assert.Throws<ArgumentNullException>(() => migrationHistoryService.HasMigrationAlreadyBeenApplied(null, "x"));
            Assert.Throws<ArgumentNullException>(() => migrationHistoryService.HasMigrationAlreadyBeenApplied(new SampleEmptyMigration(0), null));
            Assert.Throws<ArgumentNullException>(() => migrationHistoryService.HasMigrationAlreadyBeenApplied(new SampleEmptyMigration(0), string.Empty));
        }
    }
}