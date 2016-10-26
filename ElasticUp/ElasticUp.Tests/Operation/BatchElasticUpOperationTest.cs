using System;
using ElasticUp.Operation;
using Nest;
using NSubstitute;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation
{
    public class BatchElasticUpOperationTest
    {
        [Test]
        public void WithBatchSize_ThrowsWithInvalidParameters()
        {
            var operation = new BatchedElasticUpOperation<SampleObject>(0);

            Assert.Throws<ArgumentException>(() => operation.WithBatchSize(0));
            Assert.Throws<ArgumentException>(() => operation.WithBatchSize(-1));
        }

        [Test]
        public void WithScrollTimeout_ThrowsWithInvalidParameters()
        {
            var operation = new BatchedElasticUpOperation<SampleObject>(0);

            Assert.Throws<ArgumentException>(() => operation.WithScrollTimeout(0));
            Assert.Throws<ArgumentException>(() => operation.WithScrollTimeout(-1));
        }

        [Test]
        public void WithBatchTransformation_ThrowsWithInvalidParameters()
        {
            var operation = new BatchedElasticUpOperation<SampleObject>(0);

            Assert.Throws<ArgumentNullException>(() => operation.WithBatchTransformation(null));
        }

        [Test]
        public void WithSearchDescriptor_ThrowsWithInvalidParameters()
        {
            var operation = new BatchedElasticUpOperation<SampleObject>(0);

            Assert.Throws<ArgumentNullException>(() => operation.WithSearchDescriptor(null));
        }

        [Test]
        public void WithOnBatchProcessed_ThrowsWithInvalidParameters()
        {
            var operation = new BatchedElasticUpOperation<SampleObject>(0);

            Assert.Throws<ArgumentNullException>(() => operation.WithOnBatchProcessed(null));
        }
    }
}