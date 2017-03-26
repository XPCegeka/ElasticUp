using System;
using ElasticUp.Operation.Reindex;
using ElasticUp.Tests.Sample;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Reindex
{
    [TestFixture]
    public class BatchUpdateDescriptorTest
    {
        private BatchUpdateDescriptor<SampleObject, SampleObject> _descriptor;

        [SetUp]
        public void Setup()
        {
            _descriptor = new BatchUpdateDescriptor<SampleObject, SampleObject>();
        }

        [Test]
        public void WithBatchSize_ThrowsWithInvalidParameters()
        {
            Assert.Throws<ArgumentException>(() => _descriptor.BatchSize(0));
            Assert.Throws<ArgumentException>(() => _descriptor.BatchSize(-1));
        }

        [Test]
        public void WithScrollTimeout_ThrowsWithInvalidParameters()
        {
            Assert.Throws<ArgumentException>(() => _descriptor.ScrollTimeoutInSeconds(0));
            Assert.Throws<ArgumentException>(() => _descriptor.ScrollTimeoutInSeconds(-1));
        }

        [Test]
        public void WithDegreeOfParallellism_ThrowsWithInvalidParameters()
        {
            Assert.Throws<ArgumentException>(() => _descriptor.DegreeOfParallellism(0));
            Assert.Throws<ArgumentException>(() => _descriptor.DegreeOfParallellism(-1));
        }

        [Test]
        public void WithBatchTransformation_ThrowsWithInvalidParameters()
        {
            Assert.Throws<ArgumentNullException>(() => _descriptor.Transformation(null));
        }

        [Test]
        public void WithSearchDescriptor_ThrowsWithInvalidParameters()
        {
            Assert.Throws<ArgumentNullException>(() => _descriptor.SearchDescriptor(null));
        }

        [Test]
        public void WithOnBatchProcessed_ThrowsWithInvalidParameters()
        {
            Assert.Throws<ArgumentNullException>(() => _descriptor.OnDocumentProcessed(null));
        }
    }
}