using System;
using ElasticUp.Migration;
using ElasticUp.Operation;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;

namespace ElasticUp.Tests.Migration
{
    [TestFixture]
    public class ElasticUpMigrationTest
    {
        private ElasticUpMigration _elasticUpMigration;

        [SetUp]
        public void Setup()
        {
            _elasticUpMigration = new TestMigration(0);
        }

        [Test]
        public void Operation_AddsOperationToOperations()
        {
            // GIVEN
            var operation = new ElasticUpOperation(0);

            // WHEN
            _elasticUpMigration.Operation(operation);

            // THEN
            _elasticUpMigration.Operations.Should().HaveCount(1);
        }

        [Test]
        public void Operation_ThrowsWhenAddingOperationWithSameIndex()
        {
            // GIVEN
            var operation1 = new ElasticUpOperation(0);
            var operation2 = new ElasticUpOperation(0);

            // WHEN / THEN
            _elasticUpMigration.Operation(operation1);
            Assert.Throws<ArgumentException>(() => _elasticUpMigration.Operation(operation2), "Duplicate operation number.");
        }

        [Test]
        public void Execute_ExecutesEachOperation()
        {
            // GIVEN
            var operation1 = Substitute.For<ElasticUpOperation>(0);
            var operation2 = Substitute.For<ElasticUpOperation>(1);

            _elasticUpMigration.Operation(operation1);
            _elasticUpMigration.Operation(operation2);

            // WHEN
            _elasticUpMigration.Execute();

            // THEN
            operation1.Received().Execute();
            operation2.Received().Execute();
        }
    }
}
