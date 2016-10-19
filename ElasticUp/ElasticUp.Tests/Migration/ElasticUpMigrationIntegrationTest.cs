using System;
using ElasticUp.Migration;
using ElasticUp.Migration.Meta;
using ElasticUp.Operation;
using ElasticUp.Tests.Operation;
using ElasticUp.Tests.Sample;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;

namespace ElasticUp.Tests.Migration
{
    [TestFixture]
    public class ElasticUpMigrationIntegrationTest : AbstractIntegrationTest
    {
        private ElasticUpMigration _elasticUpMigration;

        public ElasticUpMigrationIntegrationTest() : base(ElasticServiceStartupType.StartupForEach) {}
        
        [SetUp]
        public void Setup()
        {
            _elasticUpMigration = new SampleEmptyMigration(0);
            _elasticUpMigration.OnIndexAlias("test");
        }

        [Test]
        public void Operation_AddsOperationToOperations()
        {
            // GIVEN
            var operation = new SampleEmptyOperation(0);

            // WHEN
            _elasticUpMigration.Operation(operation);

            // THEN
            _elasticUpMigration.Operations.Should().HaveCount(1);
        }

        [Test]
        public void Operation_ThrowsWhenAddingOperationWithSameIndex()
        {
            // GIVEN
            var operation1 = new SampleEmptyOperation(0);
            var operation2 = new SampleEmptyOperation(0);

            // WHEN / THEN
            _elasticUpMigration.Operation(operation1);
            Assert.Throws<ArgumentException>(() => _elasticUpMigration.Operation(operation2), "Duplicate operation number.");
        }

        [Ignore("No alias available for type 'test'")]
        [Test]
        public void Execute_ExecutesEachOperation()
        {
            // GIVEN
            var operation1 = Substitute.For<ElasticUpOperation>(0);
            var operation2 = Substitute.For<ElasticUpOperation>(1);

            _elasticUpMigration.Operation(operation1);
            _elasticUpMigration.Operation(operation2);

            var index0 = new VersionedIndexName("test", 0);
            var index1 = index0.GetIncrementedVersion();

            // WHEN
            _elasticUpMigration.Execute(ElasticClient, index0, index1);

            // THEN
            operation1.Received().Execute(ElasticClient, index0.Name, index1.Name);
            operation2.Received().Execute(ElasticClient, index0.Name, index1.Name);
        }

        [Test]
        public void ToString_ReturnsMigrationNumberPlusClassName()
        {
            new SampleEmptyMigration(5).ToString().Should().Be("005_SampleEmptyMigration");
            new SampleEmptyMigration(14).ToString().Should().Be("014_SampleEmptyMigration");
        }
    }
}
