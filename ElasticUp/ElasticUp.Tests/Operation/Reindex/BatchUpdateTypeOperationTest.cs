using System;
using ElasticUp.Operation.Reindex;
using ElasticUp.Tests.Sample;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Reindex
{
    [TestFixture]
    public class BatchUpdateTypeOperationTest
    {
        [Test]
        public void WithBatchSize_ThrowsWithInvalidParameters()
        {
            var operation = new BatchUpdateTypeOperation<SampleObject>();

            Assert.Throws<ArgumentException>(() => operation.WithBatchSize(0));
            Assert.Throws<ArgumentException>(() => operation.WithBatchSize(-1));
        }

        [Test]
        public void WithScrollTimeout_ThrowsWithInvalidParameters()
        {
            var operation = new BatchUpdateTypeOperation<SampleObject>();

            Assert.Throws<ArgumentException>(() => operation.WithScrollTimeout(0));
            Assert.Throws<ArgumentException>(() => operation.WithScrollTimeout(-1));
        }

        [Test]
        public void WithBatchTransformation_ThrowsWithInvalidParameters()
        {
            var operation = new BatchUpdateTypeOperation<SampleObject>();

            Assert.Throws<ArgumentNullException>(() => operation.WithDocumentTransformation(null));
        }

        [Test]
        public void WithSearchDescriptor_ThrowsWithInvalidParameters()
        {
            var operation = new BatchUpdateTypeOperation<SampleObject>();

            Assert.Throws<ArgumentNullException>(() => operation.WithSearchDescriptor(null));
        }

        [Test]
        public void WithOnBatchProcessed_ThrowsWithInvalidParameters()
        {
            var operation = new BatchUpdateTypeOperation<SampleObject>();

            Assert.Throws<ArgumentNullException>(() => operation.WithOnDocumentProcessed(null));
        }
    }
}