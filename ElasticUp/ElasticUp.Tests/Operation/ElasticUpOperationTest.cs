using System;
using ElasticUp.Migration;
using ElasticUp.Migration.Meta;
using ElasticUp.Operation;
using ElasticUp.Tests.Infrastructure;
using ElasticUp.Tests.Migration;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation
{
    [TestFixture]
    public class ElasticUpOperationTest : AbstractIntegrationTest
    {
        private ElasticSearchContainer _elasticContainer;
        private TestOperation _elasticUpOperation;

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            //_elasticContainer = SetupElasticSearchService();
        }

        [SetUp]
        public void Setup()
        {
            _elasticUpOperation = new TestOperation(0);
        }

        [OneTimeTearDown]
        public void Teardown()
        {
            //_elasticContainer.Dispose();
        }

        [Test]
        public void FromIndex_SetsFromIndexForOperation()
        {
            // GIVEN
            var index0 = new VersionedIndexName("test", 0);
            var index1 = index0.GetIncrementedVersion();

            // WHEN
            _elasticUpOperation.From(index0);

            // THEN
            _elasticUpOperation.FromIndex.Should().Be(index0);
        }

        [Test]
        public void ToIndex_SetsToIndexForOperation()
        {
            // GIVEN
            var index0 = new VersionedIndexName("test", 0);
            var index1 = index0.GetIncrementedVersion();

            // WHEN
            _elasticUpOperation.To(index1);

            // THEN
            _elasticUpOperation.ToIndex.Should().Be(index1);
        }
    }
}