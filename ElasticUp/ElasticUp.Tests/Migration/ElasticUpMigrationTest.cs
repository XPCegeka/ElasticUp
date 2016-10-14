using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Migration
{
    [TestFixture]
    public class ElasticUpMigrationTest
    {
        [Test]
        public void ToString_ReturnsMigrationNumberPlusClassName()
        {
            new TestMigration(5).ToString().Should().Be("005_TestMigration");
            new TestMigration(14).ToString().Should().Be("014_TestMigration");
        }
    }
}
