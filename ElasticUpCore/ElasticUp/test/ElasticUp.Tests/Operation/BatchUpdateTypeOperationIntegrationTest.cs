using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;
using ElasticUp.Operation;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation
{
    [TestFixture]
    public class BatchUpdateTypeOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void BatchUpdateJObjectOperation_ProcessTypeAndInsertInNewIndex()
        {
            // GIVEN
            const int expectedDocumentCount = 15000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);
            
            // TEST
            var processedRecordCount = 0;
            var operation = new BatchUpdateTypeOperation<SampleObject>()
                .WithDocumentTransformation(doc =>
                {
                    processedRecordCount++;
                    return doc;
                });

            operation.Execute(ElasticClient, TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());

            // VERIFY
            processedRecordCount.Should().Be(expectedDocumentCount);

            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()));
            countResponse.Count.Should().Be(expectedDocumentCount);
        }

        [Test]
        public void BatchUpdateJObjectOperation_WithOnDocumentProcessed_InvokesEventHandler()
        {
            // GIVEN
            const int expectedDocumentCount = 15000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n }).ToList();
            ElasticClient.IndexMany(sampleDocuments, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);

            // TEST
            var processedDocuments = new List<SampleObject>();
            var operation = new BatchUpdateTypeOperation<SampleObject>()
                .WithOnDocumentProcessed(doc =>
                {
                    processedDocuments.Add(doc);
                });

            operation.Execute(ElasticClient, TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());

            // VERIFY
            processedDocuments.OrderBy(doc => doc.Number).ShouldAllBeEquivalentTo(sampleDocuments.OrderBy(doc => doc.Number));
        }

        [Test]
        public void BatchUpdateJObjectOperation_WithSearchDescriptor_ProcessesFilteredDocumentsAndInsertsInNewIndex()
        {
            // GIVEN
            const int expectedDocumentCount = 15000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);
            
            // TEST
            var operation = new BatchUpdateTypeOperation<SampleObject>()
                .WithSearchDescriptor(descriptor =>
                    descriptor.Query(query =>
                        query.Range(rangeQuery => rangeQuery.Field(x => x.Number).LessThan(10000))));

            operation.Execute(ElasticClient, TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()));
            countResponse.Count.Should().Be(10000);
        }

        [Test]
        public void BatchUpdateJObjectOperation_WithDocumentTransformationWithFilters_ProcessesAllDocumentsInBatchesAndInsertsFilteredInNewIndex()
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

            operation.Execute(ElasticClient, TestIndex.IndexNameWithVersion(), TestIndex.NextIndexNameWithVersion());

            // VERIFY
            processedDocumentCount.Should().Be(expectedDocumentCount);
            documentCountWithEvenNumber.Should().Be(7500);

            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion()));
            countResponse.Count.Should().Be(7500);
        }

        [Test]
        public void BatchUpdateJObjectOperation_ThrowsOnServerError()
        {
            // TEST
            var operation = new BatchUpdateTypeOperation<SampleObject>();
            
            Assert.Throws<ElasticsearchClientException>(() => operation.Execute(ElasticClient, "does not exist", TestIndex.NextIndexNameWithVersion()));
        }

        [Test]
        public void BatchUpdateJObjectOperation_ParallelTest()
        {
            // GIVEN
            const int documentCount = 25000;
            var documents = Enumerable.Range(0, documentCount).Select(n => new SampleObject {Number = n}).ToList();
            ElasticClient.IndexMany(documents, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);

            // TEST
            var actualDocuments = new List<SampleObject>(documentCount);
            var scrollTimeout = new Time(60*1000);
            var searchResponse = ElasticClient.Search<SampleObject>(descriptor => descriptor.Scroll(scrollTimeout).Size(5000).Index(TestIndex.IndexNameWithVersion()));
            actualDocuments.AddRange(searchResponse.Documents);

            var scrollId = searchResponse.ScrollId;
            DoScroll<SampleObject>(scrollId, docs => { actualDocuments.AddRange(docs); }).Wait();
        }

        private Task<ISearchResponse<TDocument>> DoScroll<TDocument>(string scrollId, Action<IEnumerable<TDocument>> action) where TDocument : class
        {
            Console.WriteLine(@"Retrieving data");
            var scrollTimeout = new Time(60 * 1000);
            var bar = ElasticClient.ScrollAsync<TDocument>(scrollTimeout, scrollId)
                .ContinueWith(responseTask =>
                {
                    Console.WriteLine(@"Data retrieved");

                    var response = responseTask.Result;
                    if (!response.Documents.Any())
                        return responseTask;

                    var doScrollTask = DoScroll(response.ScrollId, action);
                    Console.WriteLine(@"Processing data");
                    action(response.Documents);
                    Console.WriteLine(@"Data processed");

                    return doScrollTask;
                });

            return bar.Unwrap();
        }
    }
}

