using System.Linq;
using ElasticUp.Operation.Reindex;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Reindex
{
    [TestFixture]
    public class BatchUpdateFromTypeToTypeUsingJObjectOperationIntegrationTest : AbstractIntegrationTest
    {
        [TearDown]
        public void TearDownAfterTest()
        {
            TryDeleteIndex(TestIndex.NextIndexNameWithVersion());
            TryDeleteIndex(MigrationHistoryTestIndex.IndexNameWithVersion());
        }

        [Test]
        public void Execute_ProcessesTypeAsJObjectAndInsertsInNewIndex()
        {
            // GIVEN
            const int expectedDocumentCount = 150;
            var sampleDocuments = Enumerable.Range(0, expectedDocumentCount).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(sampleDocuments, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);
            
            // TEST
            var processedRecordCount = 0;
            var operation = new BatchUpdateFromTypeToTypeUsingJObjectOperation("sampleobject", "sampleobject")
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .WithDocumentTransformation(doc =>
                {
                    doc["number"] = 666;
                    processedRecordCount++;
                    return doc;
                })
                .WithSearchDescriptor(sd => sd.MatchAll())
                .WithBatchSize(50);

            operation.Execute(ElasticClient);

            // VERIFY
            processedRecordCount.Should().Be(expectedDocumentCount);

            ElasticClient.Refresh(Indices.All);
            var countResponse = ElasticClient.Count<SampleObject>(descriptor => descriptor
                                                                                    .Index(TestIndex.NextIndexNameWithVersion())
                                                                                    .Query(q => q.Term(t => t.Number, 666)));
            countResponse.Count.Should().Be(expectedDocumentCount);
        }

        [Test]
        public void Execute_ProcessesTypeAsJObjectAndInsertsInNewIndex_WithKeepingTheId()
        {
            // GIVEN
            var sampleObjectWithId1 = new SampleObjectWithId {Id = new ObjectId {Type = "TestId", Sequence = 0}};
            var sampleObjectWithId2 = new SampleObjectWithId {Id = new ObjectId {Type = "TestId", Sequence = 1}};
            ElasticClient.Index(sampleObjectWithId1, idx => idx.Index(TestIndex.IndexNameWithVersion()));
            ElasticClient.Index(sampleObjectWithId2, idx => idx.Index(TestIndex.IndexNameWithVersion()));
            ElasticClient.Refresh(Indices.All);

            // TEST
            var processedRecordCount = 0;
            var operation = new BatchUpdateFromTypeToTypeUsingJObjectOperation("sampleobjectwithid", "sampleobjectwithid")
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .WithSameId()
                .WithDocumentTransformation(doc =>
                {
                    doc["number"] = 666;
                    processedRecordCount++;
                    return doc;
                })
                .WithSearchDescriptor(sd => sd.MatchAll())
                .WithBatchSize(50);

            operation.Execute(ElasticClient);

            // VERIFY
            processedRecordCount.Should().Be(2);

            ElasticClient.Refresh(Indices.All);
            ElasticClient.Get<SampleObjectWithId>("TestId-0", desc => desc.Index(TestIndex.NextIndexNameWithVersion())).Should().NotBeNull();
            ElasticClient.Get<SampleObjectWithId>("TestId-1", desc => desc.Index(TestIndex.NextIndexNameWithVersion())).Should().NotBeNull();
        }

    }
}

