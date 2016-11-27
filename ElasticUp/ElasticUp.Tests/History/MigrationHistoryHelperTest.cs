using System;
using ElasticUp.History;
using ElasticUp.Tests.Sample;
using Nest;
using NSubstitute;
using NUnit.Framework;

namespace ElasticUp.Tests.History
{
    [TestFixture]
    public class MigrationHistoryHelperTest
    {
        private IElasticClient _elasticClient;
        private MigrationHistoryHelper _migrationHistoryHelper;

        [SetUp]
        public void SetUp()
        {
            _elasticClient = Substitute.For<IElasticClient>();
            _migrationHistoryHelper = new MigrationHistoryHelper(_elasticClient);
        }

        [Test]
        public void CopyMigrationHistory_ThrowsWithInvalidParameters()
        {
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.CopyMigrationHistory(null, "x"));
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.CopyMigrationHistory(string.Empty, "x"));

            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.CopyMigrationHistory("x", null));
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.CopyMigrationHistory("x", string.Empty));
        }

        [Test]
        public void AddMigrationHistory_ThrowsWithInvalidParameters()
        {
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.AddMigrationToHistory(null, "x"));
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.AddMigrationToHistory(new SampleEmptyMigration("index"), null));
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.AddMigrationToHistory(new SampleEmptyMigration("index"), string.Empty));
        }

        [Test]
        public void AddMigrationHistory_WithException_ThrowsWithInvalidParameters()
        {
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.AddMigrationToHistory(null, "x", null));
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.AddMigrationToHistory(new SampleEmptyMigration("index"), null, null));
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.AddMigrationToHistory(new SampleEmptyMigration("index"), string.Empty, null));
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ThrowsWithInvalidParameters()
        {
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.HasMigrationAlreadyBeenApplied(null, "x"));
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.HasMigrationAlreadyBeenApplied(new SampleEmptyMigration("index"), null));
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.HasMigrationAlreadyBeenApplied(new SampleEmptyMigration("index"), string.Empty));
        }
    }
}