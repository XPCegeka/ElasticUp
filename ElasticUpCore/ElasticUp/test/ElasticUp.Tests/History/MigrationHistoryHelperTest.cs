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
            _migrationHistoryHelper = new MigrationHistoryHelper(_elasticClient, "elasticupmigrationhistory");
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
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.AddMigrationToHistory(null));
            Assert.Throws<ArgumentNullException>(() => new MigrationHistoryHelper(_elasticClient, null).AddMigrationToHistory(new SampleEmptyMigration("index")));
        }

        [Test]
        public void AddMigrationHistory_WithException_ThrowsWithInvalidParameters()
        {
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.AddMigrationToHistory(null, null));
            Assert.Throws<ArgumentNullException>(() => new MigrationHistoryHelper(_elasticClient, null).AddMigrationToHistory(new SampleEmptyMigration("index")));
        }

        [Test]
        public void HasMigrationAlreadyBeenApplied_ThrowsWithInvalidParameters()
        {
            Assert.Throws<ArgumentNullException>(() => _migrationHistoryHelper.HasMigrationAlreadyBeenApplied(null));
            Assert.Throws<ArgumentNullException>(() => new MigrationHistoryHelper(_elasticClient, null).HasMigrationAlreadyBeenApplied(new SampleEmptyMigration("index")));
        }
    }
}