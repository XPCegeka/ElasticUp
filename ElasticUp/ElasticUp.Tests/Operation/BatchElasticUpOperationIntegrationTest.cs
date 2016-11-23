﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ElasticUp.Elastic;
using ElasticUp.Operation;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation
{
    [TestFixture]
    public class BatchElasticUpOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void Execute_ProcessesDocumentsAndInsertsInNewIndex()
        {
            // GIVEN
            const string fromIndex = "from";
            const string toIndex = "to";
            const int expectedDocumentCount = 50000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, fromIndex);
            ElasticClient.Refresh(Indices.All);


            // TEST
            var processedRecordCount = 0;
            var operation = new BatchedElasticUpOperation<SampleObject>(0)
                .WithDocumentTransformation(doc =>
                {
                    processedRecordCount++;
                    return doc;
                });

            operation.Execute(ElasticClient, fromIndex, toIndex);

            // VERIFY
            processedRecordCount.Should().Be(expectedDocumentCount);

            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(toIndex));
            countResponse.Count.Should().Be(expectedDocumentCount);
        }

        [Test]
        public void Execute_WithOnDocumentProcessed_InvokesEventHandler()
        {
            // GIVEN
            const string fromIndex = "from";
            const string toIndex = "to";
            const int expectedDocumentCount = 50000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n }).ToList();
            ElasticClient.IndexMany(sampleDocuments, fromIndex);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var processedDocuments = new List<SampleObject>();
            var operation = new BatchedElasticUpOperation<SampleObject>(0)
                .WithOnDocumentProcessed(doc =>
                {
                    processedDocuments.Add(doc);
                });

            operation.Execute(ElasticClient, fromIndex, toIndex);

            // VERIFY
            processedDocuments.OrderBy(doc => doc.Number).ShouldAllBeEquivalentTo(sampleDocuments.OrderBy(doc => doc.Number));
        }

        [Test]
        public void Execute_WithSearchDescriptor_ProcessesFilteredDocumentsAndInsertsInNewIndex()
        {
            // GIVEN
            const string fromIndex = "from";
            const string toIndex = "to";
            const int expectedDocumentCount = 50000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, fromIndex);
            ElasticClient.Refresh(Indices.All);


            // TEST
            var operation = new BatchedElasticUpOperation<SampleObject>(0)
                .WithSearchDescriptor(descriptor =>
                    descriptor.Query(query =>
                        query.Range(rangeQuery => rangeQuery.Field(x => x.Number).LessThan(10000))));

            operation.Execute(ElasticClient, fromIndex, toIndex);

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(toIndex));
            countResponse.Count.Should().Be(10000);
        }

        [Test]
        public void Execute_WithDocumentTransformationWithFilters_ProcessesAllDocumentsInBatchesAndInsertsFilteredInNewIndex()
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
            var operation = new BatchedElasticUpOperation<SampleObject>(0);
            
            Assert.Throws<ElasticUpException>(() => operation.Execute(ElasticClient, fromIndex, toIndex));
        }

        [Test]
        public void ParallelTest()
        {
            // GIVEN
            const string index = "from";
            const int documentCount = 25000;
            var documents = Enumerable.Range(0, documentCount).Select(n => new SampleObject {Number = n}).ToList();
            ElasticClient.IndexMany(documents, index);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var actualDocuments = new List<SampleObject>(documentCount);
            var scrollTimeout = new Time(60*1000);
            var searchResponse = ElasticClient.Search<SampleObject>(descriptor => descriptor.Scroll(scrollTimeout).Size(5000).Index(index));
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

