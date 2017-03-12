using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ElasticUp.Elastic;
using ElasticUp.Operation.Reindex;
using ElasticUp.Tests.Sample;
using ElasticUp.Util;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Reindex
{
    [TestFixture]
    [Ignore("Not yet operational")]
    public class BatchUpdateOperationPerformanceIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void BatchUpdateTypeOperationIsFasterWhenUsingParallellism()
        {
            // GIVEN
            const int expectedDocumentCount = 1000;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);
            Thread.Sleep(TimeSpan.FromSeconds(5)); //wait for elastic background stuff

            // TEST
            var processedDocumentCount = 0;

            var operation = new BatchUpdateOperation<SampleObject, SampleObject>(descriptor => descriptor
                                    .FromIndex(TestIndex.IndexNameWithVersion())
                                    .ToIndex(TestIndex.NextIndexNameWithVersion())
                                    .Transformation(doc =>
                                    {
                                        processedDocumentCount++;
                                        return doc;
                                    }));

            var timer1 = new ElasticUpTimer("WithoutParallellism");
            using (timer1)
            {
                operation.Execute(ElasticClient);
            }
            var elapsedWithoutParallellism = timer1.GetElapsedMilliseconds();

            // VERIFY
            processedDocumentCount.Should().Be(expectedDocumentCount);
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion())).Count.Should().Be(expectedDocumentCount);

            CreateIndex(TestIndex.NextVersion().NextIndexNameWithVersion());
            processedDocumentCount = 0;

            var operation2 = new BatchUpdateOperation<SampleObject, SampleObject>(descriptor => descriptor
                .FromIndex(TestIndex.NextVersion().IndexNameWithVersion())
                .ToIndex(TestIndex.NextVersion().NextIndexNameWithVersion())
                .DegreeOfParallellism(5)
                .Transformation(doc =>
                {
                    processedDocumentCount++;
                    return doc;
                }));

            var t2 = new ElasticUpTimer("WithoutParallellism");
            using (t2)
            {
                operation2.Execute(ElasticClient);

            }
            var elapsedWithParallellism = t2.StopAndGetElapsedMilliseconds();

            processedDocumentCount.Should().Be(expectedDocumentCount);
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextVersion().NextIndexNameWithVersion())).Count.Should().Be(expectedDocumentCount);

            elapsedWithoutParallellism.Should().BeGreaterThan(elapsedWithParallellism);
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

        [Test]
        public void ParallelTest()
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

