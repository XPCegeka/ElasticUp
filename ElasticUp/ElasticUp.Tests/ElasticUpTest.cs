using System;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests
{
    [TestFixture]
    public class ElasticUpTest
    {
        [Test]
        public void WhenAddingAMigration_MakeSureAllMigrationNumbersAreUnique()
        {
            Assert.Throws<ArgumentException>(() =>
                new ElasticUp(new ElasticClient())
                    .Migration(new Sample.SampleEmptyMigration(1))
                    .Migration(new Sample.SampleEmptyMigration(1)));
        }

        [Test]
        public void WhenAddingAMigration_MakeSureAllMigrationNumbersAreAscending()
        {
            Assert.Throws<ArgumentException>(() =>
                new ElasticUp(new ElasticClient())
                    .Migration(new Sample.SampleEmptyMigration(2))
                    .Migration(new Sample.SampleEmptyMigration(1)));
        }
    }
}
