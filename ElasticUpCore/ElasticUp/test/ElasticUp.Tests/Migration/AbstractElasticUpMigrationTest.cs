using System;
using ElasticUp.Migration;
using ElasticUp.Tests.Sample;
using FluentAssertions;
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
            _elasticUpMigration = new SampleEmptyMigration("test");
        }

        [Test]
        public void Operation_AddsOperationToOperations()
        {
            // GIVEN
            var operation = new SampleEmptyOperation();

            // WHEN
            _elasticUpMigration.Operation(operation);

            // THEN
            _elasticUpMigration.Operations.Should().HaveCount(1);
        }

        [Test]
        public void ToString_ReturnsMigrationNumberPlusClassName()
        {
            new SampleEmptyMigration("index").ToString().Should().Be("sampleemptymigration");
            new SampleEmptyMigration("index").ToString().Should().Be("sampleemptymigration");
        }
    }
}
