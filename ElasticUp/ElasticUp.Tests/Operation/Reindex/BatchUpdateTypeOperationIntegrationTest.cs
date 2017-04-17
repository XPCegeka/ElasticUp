using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ElasticUp.Operation.Reindex;
using ElasticUp.Tests.Sample;
using ElasticUp.Util;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Reindex
{
    [Obsolete("Use BatchUpdateOperation")]
    [TestFixture]
    public class BatchUpdateTypeOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void Execute_ProcessTypeAndInsertInNewIndex()
        {
            // GIVEN
            const int expectedDocumentCount = 15000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);
            
            // TEST
            var processedRecordCount = 0;
            var operation = new BatchUpdateTypeOperation<SampleObject>()
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .WithDocumentTransformation(doc =>
                {
                    processedRecordCount++;
                    return doc;
                });

            operation.Execute(ElasticClient);

            // VERIFY
            processedRecordCount.Should().Be(expectedDocumentCount);

            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()));
            countResponse.Count.Should().Be(expectedDocumentCount);
        }

        [Test]
        public void Execute_WithOnDocumentProcessed_InvokesEventHandler()
        {
            // GIVEN
            const int expectedDocumentCount = 15000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n }).ToList();
            ElasticClient.IndexMany(sampleDocuments, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);

            // TEST
            var processedDocuments = new List<SampleObject>();
            var operation = new BatchUpdateTypeOperation<SampleObject>()
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .WithOnDocumentProcessed(doc =>
                {
                    processedDocuments.Add(doc);
                });

            operation.Execute(ElasticClient);

            // VERIFY
            processedDocuments.OrderBy(doc => doc.Number).ShouldAllBeEquivalentTo(sampleDocuments.OrderBy(doc => doc.Number));
        }

        [Test]
        public void Execute_WithSearchDescriptor_ProcessesFilteredDocumentsAndInsertsInNewIndex()
        {
            // GIVEN
            const int expectedDocumentCount = 15000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);
            
            // TEST
            var operation = new BatchUpdateTypeOperation<SampleObject>()
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .WithSearchDescriptor(descriptor =>
                    descriptor.Query(query =>
                        query.Range(rangeQuery => rangeQuery.Field(x => x.Number).LessThan(10000))));

            operation.Execute(ElasticClient);

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()));
            countResponse.Count.Should().Be(10000);
        }

        [Test]
        public void Execute_WithDocumentTransformationWithFilters_ProcessesAllDocumentsInBatchesAndInsertsFilteredInNewIndex()
        {
            // GIVEN
            const int expectedDocumentCount = 15000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);
            
            // TEST
            var processedDocumentCount = 0;
            var documentCountWithEvenNumber = 0;
            var operation = new BatchUpdateTypeOperation<SampleObject>()
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .WithDocumentTransformation(doc =>
                {
                    processedDocumentCount++;

                    if (doc.Number%2 == 0)
                    {
                        documentCountWithEvenNumber++;
                        return doc;
                    }
                    return null;
                });

            operation.Execute(ElasticClient);

            // VERIFY
            processedDocumentCount.Should().Be(expectedDocumentCount);
            documentCountWithEvenNumber.Should().Be(7500);

            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()));
            countResponse.Count.Should().Be(7500);
        }

        [Test]
        public void Execute_ValidatesFromAndToIndex()
        {
            Assert.Throws<ElasticUpException>(() => new BatchUpdateTypeOperation<SampleObject>().FromIndex(null).ToIndex(TestIndex.NextIndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateTypeOperation<SampleObject>().FromIndex(" ").ToIndex(TestIndex.NextIndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateTypeOperation<SampleObject>().FromIndex(TestIndex.IndexNameWithVersion()).ToIndex(null).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateTypeOperation<SampleObject>().FromIndex(TestIndex.IndexNameWithVersion()).ToIndex("").Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateTypeOperation<SampleObject>().FromIndex("does not exist").ToIndex(TestIndex.NextIndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateTypeOperation<SampleObject>().FromIndex(TestIndex.IndexNameWithVersion()).ToIndex("does not exist").Execute(ElasticClient));
        }
    }
}

