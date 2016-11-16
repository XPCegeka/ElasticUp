using System;
using ElasticUp.Migration;
using ElasticUp.Migration.Meta;
using ElasticUp.Operation;
using ElasticUp.Tests.Sample;
using FluentAssertions;
using Nest;
using NSubstitute;
using NUnit.Framework;

namespace ElasticUp.Tests.Migration
{
    [TestFixture]
    public class AbstractElasticUpMigrationTest
    {
        private AbstractElasticUpMigration _elasticUpMigration;
        
        [SetUp]
        public void Setup()
        {
            _elasticUpMigration = new SampleEmptyMigration(0, "test");
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

        [Test]
        public void ToString_ReturnsMigrationNumberPlusClassName()
        {
            new SampleEmptyMigration(5).ToString().Should().Be("005_SampleEmptyMigration");
            new SampleEmptyMigration(14).ToString().Should().Be("014_SampleEmptyMigration");
        }
    }
}
