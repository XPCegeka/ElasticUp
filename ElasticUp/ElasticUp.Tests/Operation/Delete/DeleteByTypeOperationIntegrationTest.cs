using System;
using System.Linq;
using ElasticUp.Elastic;
using ElasticUp.Operation.Delete;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Delete
{
    [TestFixture]
    public class DeleteByTypeOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void Execute_ThrowsWhenIndexNameNull()
        {
            var operation = new DeleteByTypeOperation()
                .WithIndexName(null)
                .WithTypeName("Type");

            Assert.Throws<ElasticUpException>(() => operation.Execute(ElasticClient));
        }

        [Test]
        public void Execute_ThrowsWhenIndexNameEmpty()
        {
            var operation = new DeleteByTypeOperation()
                .WithIndexName("")
                .WithTypeName("Type");

            Assert.Throws<ElasticUpException>(() => operation.Execute(ElasticClient));
        }

        [Test]
        public void Execute_ThrowsWhenTypeNameNull()
        {
            var operation = new DeleteByTypeOperation()
                .WithIndexName("Index")
                .WithTypeName(null);

            Assert.Throws<ElasticUpException>(() => operation.Execute(ElasticClient));
        }

        [Test]
        public void Execute_ThrowsWhenTypeNameEmpty()
        {
            var operation = new DeleteByTypeOperation()
                .WithIndexName("Index")
                .WithTypeName("");

            Assert.Throws<ElasticUpException>(() => operation.Execute(ElasticClient));
        }

        [Test]
        public void GenericWithTypeName_SetsTypeName()
        {
            var operation = new DeleteByTypeOperation();
            operation.WithTypeName<SampleObject>();

            operation.TypeName.Should().Be("sampleobject");
        }

        [Test]
        public void WithTypeName_SetsTypeName()
        {
            const string typeName = "typename";

            var operation = new DeleteByTypeOperation();
            operation.WithTypeName(typeName);

            operation.TypeName.Should().Be(typeName);
        }

        [Test]
        public void WithIndexName_SetsIndexName()
        {
            const string indexName = "indexname";

            var operation = new DeleteByTypeOperation();
            operation.WithIndexName(indexName);

            operation.IndexName.Should().Be(indexName);
        }

        [Test]
        public void WithScrollTimeout_SetsScrollTimeout()
        {
            var scrollTimeout = TimeSpan.FromDays(1);

            var operation = new DeleteByTypeOperation();
            operation.WithScrollTimeout(scrollTimeout);

            operation.ScrollTimeout.Should().Be(scrollTimeout);
        }

        [Test]
        public void WithBatchSize_SetsBatchSize()
        {
            const int batchSize = 5000;

            var operation = new DeleteByTypeOperation();
            operation.WithBatchSize(batchSize);

            operation.BatchSize.Should().Be(batchSize);
        }

        [Test]
        public void WithBatchSize_ThrowsWithZeroOrNegativeBatchSize()
        {
            var operation = new DeleteByTypeOperation();
            Assert.Throws<ElasticUpException>(() => operation.WithBatchSize(0));
            Assert.Throws<ElasticUpException>(() => operation.WithBatchSize(-1));
        }

        [Test]
        public void Execute_DeletesDocuments()
        {
            var documents = Enumerable.Range(1, 10000).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(documents);
            ElasticClient.Refresh(Indices.All);

            var operation = new DeleteByTypeOperation()
                .WithIndexName(TestIndex)
                .WithTypeName<SampleObject>();

            operation.Execute(ElasticClient);

            ElasticClient.Refresh(Indices.All);
            var actualDocumentCount = ElasticClient.Count<SampleObject>(descr => descr.Index(Indices.Parse(TestIndex))).Count;
            actualDocumentCount.Should().Be(0);
        }

        [Test]
        public void Execute_DeletesNoDocumentsWhenNothingToDelete()
        {
            const int documentCount = 10000;
            var documents = Enumerable.Range(1, documentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(documents);
            ElasticClient.Refresh(Indices.All);

            var operation = new DeleteByTypeOperation()
                .WithIndexName(TestIndex)
                .WithTypeName("othertypename");

            operation.Execute(ElasticClient);

            ElasticClient.Refresh(Indices.All);
            var actualDocumentCount = ElasticClient.Count<SampleObject>(descr => descr.Index(Indices.Parse(TestIndex))).Count;
            actualDocumentCount.Should().Be(documentCount);
        }
    }
}