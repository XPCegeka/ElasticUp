using System;
using System.Collections.Generic;
using System.Linq;
using ElasticUp.Operation;
using FluentAssertions;
using Nest;
using NSubstitute;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation
{
    public class BatchElasticUpOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void Execute_ProcessesDocumentsInBatchesAndInsertsInNewIndex()
        {
            // GIVEN
            const string fromIndex = "from";
            const string toIndex = "to";
            const int expectedDocumentCount = 50000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, fromIndex);
            ElasticClient.Refresh(Indices.All);


            // TEST
            var processedDocumentCount = 0;
            var processedBatchCount = 0;
            var operation = new BatchedElasticUpOperation<SampleObject>(0)
                .WithBatchTransformation(docs =>
                {
                    processedBatchCount++;
                    var transformedDocs = new List<SampleObject>();
                    foreach (var doc in docs)
                    {
                        processedDocumentCount++;
                        transformedDocs.Add(doc);
                    }

                    return transformedDocs;
                });

            operation.Execute(ElasticClient, fromIndex, toIndex);

            // VERIFY
            processedDocumentCount.Should().Be(expectedDocumentCount);
            processedBatchCount.Should().Be(10);

            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(toIndex));
            countResponse.Count.Should().Be(expectedDocumentCount);
        }

        [Test]
        public void Execute_WithOnBatchProcessed_ProcessesDocumentsInBatchesAndInvokesEventHandler()
        {
            // GIVEN
            const string fromIndex = "from";
            const string toIndex = "to";
            const int expectedDocumentCount = 50000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, fromIndex);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var numberOfTimesInvoked = 0;
            var operation = new BatchedElasticUpOperation<SampleObject>(0)
                .WithBatchTransformation(docs => new List<SampleObject>(docs))
                .WithOnBatchProcessed((originalDocuments, transformedDocuments) =>
                {
                    numberOfTimesInvoked++;
                    originalDocuments.ShouldBeEquivalentTo(transformedDocuments);
                });

            operation.Execute(ElasticClient, fromIndex, toIndex);

            // VERIFY
            numberOfTimesInvoked.Should().Be(10);
        }

        [Test]
        public void Execute_WithSearchDescriptor_ProcessesFilteredDocumentsInBatchesAndInsertsInNewIndex()
        {
            // GIVEN
            const string fromIndex = "from";
            const string toIndex = "to";
            const int expectedDocumentCount = 50000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, fromIndex);
            ElasticClient.Refresh(Indices.All);


            // TEST
            var processedDocumentCount = 0;
            var operation = new BatchedElasticUpOperation<SampleObject>(0)
                .WithSearchDescriptor(descriptor =>
                    descriptor.Query(query =>
                        query.Range(rangeQuery => rangeQuery.Field(x => x.Number).LessThan(10000))))
                .WithBatchTransformation(docs =>
                {
                    var transformedDocs = new List<SampleObject>();
                    foreach (var doc in docs)
                    {
                        processedDocumentCount++;
                        transformedDocs.Add(doc);
                    }

                    return transformedDocs;
                });

            operation.Execute(ElasticClient, fromIndex, toIndex);

            // VERIFY
            processedDocumentCount.Should().Be(10000);

            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(toIndex));
            countResponse.Count.Should().Be(10000);
        }

        [Test]
        public void Execute_WithBatchTransformationWithFilters_ProcessesAllDocumentsInBatchesAndInsertsFilteredInNewIndex()
        {
            // GIVEN
            const string fromIndex = "from";
            const string toIndex = "to";
            const int expectedDocumentCount = 50000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, fromIndex);
            ElasticClient.Refresh(Indices.All);


            // TEST
            var processedDocumentCount = 0;
            var documentCountWithEvenNumber = 0;
            var operation = new BatchedElasticUpOperation<SampleObject>(0)
                .WithBatchTransformation(docs =>
                {
                    var transformedDocs = new List<SampleObject>();
                    foreach (var doc in docs)
                    {
                        processedDocumentCount++;

                        if (doc.Number%2 == 0)
                        {
                            documentCountWithEvenNumber++;
                            transformedDocs.Add(doc);
                        }
                    }

                    return transformedDocs;
                });

            operation.Execute(ElasticClient, fromIndex, toIndex);

            // VERIFY
            processedDocumentCount.Should().Be(expectedDocumentCount);
            documentCountWithEvenNumber.Should().Be(25000);

            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(toIndex));
            countResponse.Count.Should().Be(25000);
        }

        [Test]
        public void Execute_ThrowsOnServerError()
        {
            // GIVEN
            const string fromIndex = "does not exist";
            const string toIndex = "to";

            // TEST
            var operation = new BatchedElasticUpOperation<SampleObject>(0)
                .WithBatchTransformation(docs => docs.ToList());
            
            Assert.Throws<Exception>(() => operation.Execute(ElasticClient, fromIndex, toIndex));
        }
    }
}
