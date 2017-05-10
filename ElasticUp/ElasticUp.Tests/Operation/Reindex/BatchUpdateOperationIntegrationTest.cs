using System;
using System.Collections.Generic;
using System.Threading;
using ElasticUp.Operation.Reindex;
using ElasticUp.Tests.Sample;
using ElasticUp.Util;
using FluentAssertions;
using Nest;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Reindex
{
    [TestFixture]

    public class BatchUpdateOperationPerformanceIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void BatchUpdateTypeOperationIsFasterWhenUsingParallellism()
        {
            CreateIndex(TestIndex.NextVersion().NextIndexNameWithVersion());

            const int expectedDocumentCount = 1000;
            BulkIndexSampleObjects(expectedDocumentCount, expectedDocumentCount);
            Thread.Sleep(TimeSpan.FromSeconds(5)); //wait for elastic to do it's thing

            // GIVEN
            var processedDocumentCount = 0;

            // WHEN BATCHUPDATE WITHOUT PARALLEL
            var timerWithoutParallel = new ElasticUpTimer("WithoutParallellism");
            using (timerWithoutParallel)
            {
                new BatchUpdateOperation<SampleObject, SampleObject>(descriptor => descriptor
                    .FromIndex(TestIndex.IndexNameWithVersion())
                    .ToIndex(TestIndex.NextIndexNameWithVersion())
                    .DegreeOfParallellism(1)
                    .Transformation(doc =>
                    {
                        processedDocumentCount++;
                        return doc;
                    })).Execute(ElasticClient);
            }

            // VERIFY BATCH UPDATE
            processedDocumentCount.Should().Be(expectedDocumentCount);
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion())).Count.Should().Be(expectedDocumentCount);


            //WHEN BATCHUPDATE WITH PARALLEL
            processedDocumentCount = 0;
            var timerWithParallel = new ElasticUpTimer("WithoutParallellism");
            using (timerWithParallel)
            {
                new BatchUpdateOperation<SampleObject, SampleObject>(descriptor => descriptor
                        .FromIndex(TestIndex.NextVersion().IndexNameWithVersion())
                        .ToIndex(TestIndex.NextVersion().NextIndexNameWithVersion())
                        .DegreeOfParallellism(5)
                        .Transformation(doc =>
                        {
                            processedDocumentCount++;
                            return doc;
                        })
                ).Execute(ElasticClient);
            }

            //VERIFY
            processedDocumentCount.Should().Be(expectedDocumentCount);
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextVersion().NextIndexNameWithVersion())).Count.Should().Be(expectedDocumentCount);

            //VERIFY parallel was faster
            timerWithoutParallel.GetElapsedMilliseconds().Should().BeGreaterThan(timerWithParallel.GetElapsedMilliseconds());
        }

        [Test]
        public void OperationValidatesThatFromAndToIndexAreDefinedAndExistInElasticSearch()
        {
            Assert.Throws<ElasticUpException>(() => new BatchUpdateOperation<SampleObject,SampleObject>(s => s.FromIndex(null).ToIndex(TestIndex.NextIndexNameWithVersion())).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateOperation<SampleObject,SampleObject>(s => s.FromIndex(" ").ToIndex(TestIndex.NextIndexNameWithVersion())).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateOperation<SampleObject,SampleObject>(s => s.FromIndex(TestIndex.IndexNameWithVersion()).ToIndex(null)).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateOperation<SampleObject,SampleObject>(s => s.FromIndex(TestIndex.IndexNameWithVersion()).ToIndex("")).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateOperation<SampleObject,SampleObject>(s => s.FromIndex("does not exist").ToIndex(TestIndex.NextIndexNameWithVersion())).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateOperation<SampleObject,SampleObject>(s => s.FromIndex(TestIndex.IndexNameWithVersion()).ToIndex("does not exist")).Validate(ElasticClient));
        }

        [Test]
        public void TransformAsJObjects_ObjectsAreTransformedAndIndexedInNewIndex()
        {
            // GIVEN
            const int expectedDocumentCount = 150;
            BulkIndexSampleObjects(expectedDocumentCount);

            // TEST
            var processedRecordCount = 0;

            var operation = new BatchUpdateOperation<JObject,JObject>(descriptor => descriptor
                .BatchSize(50)
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .FromType<SampleObject>()
                .ToType<SampleObject>()
                .SearchDescriptor(sd => sd.MatchAll())
                .Transformation(doc =>
                {
                    doc["number"] = 666;
                    processedRecordCount++;
                    return doc;
                }));

            operation.Execute(ElasticClient);

            // VERIFY
            ElasticClient.Refresh(Indices.All);

            processedRecordCount.Should().Be(expectedDocumentCount);

            ElasticClient.Count<SampleObject>(descriptor => descriptor
                .Index(TestIndex.NextIndexNameWithVersion())
                .Query(q => q.Term(t => t.Number, 666))).Count.Should().Be(expectedDocumentCount);
        }

        [Test]
        public void TransformAsJObjects_ObjectsAreTransformedAndIndexedInNewIndex_AndKeepTheirId()
        {
            // GIVEN
            var sampleObjectWithId1 = new SampleObjectWithId {Id = new ObjectId {Type = "TestId", Sequence = 0}};
            var sampleObjectWithId2 = new SampleObjectWithId {Id = new ObjectId {Type = "TestId", Sequence = 1}};
            ElasticClient.Index(sampleObjectWithId1, idx => idx.Index(TestIndex.IndexNameWithVersion()));
            ElasticClient.Index(sampleObjectWithId2, idx => idx.Index(TestIndex.IndexNameWithVersion()));
            ElasticClient.Refresh(Indices.All);

            // TEST
            var operation = new BatchUpdateOperation<JObject,JObject>(descriptor => descriptor
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .FromType<SampleObjectWithId>()
                .ToType<SampleObjectWithId>()
                .Transformation(doc => doc)
                .SearchDescriptor(sd => sd.MatchAll()));

            operation.Execute(ElasticClient);

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Get<SampleObjectWithId>("TestId-0", desc => desc.Index(TestIndex.NextIndexNameWithVersion())).Should().NotBeNull();
            ElasticClient.Get<SampleObjectWithId>("TestId-1", desc => desc.Index(TestIndex.NextIndexNameWithVersion())).Should().NotBeNull();
        }

        [Test]
        public void TransformAsJObjects_ObjectsAreTransformedAndIndexedInNewIndex_AndKeepTheirVersion()
        {
            // GIVEN
            var sampleObject1V1 = new SampleObjectWithId {Id = new ObjectId {Type = "TestId", Sequence = 1}, Number = 1};
            var sampleObject1V2 = new SampleObjectWithId {Id = new ObjectId {Type = "TestId", Sequence = 1}, Number = 2};
            var sampleObject1V3 = new SampleObjectWithId {Id = new ObjectId {Type = "TestId", Sequence = 1}, Number = 3};

            var sampleObject2V1 = new SampleObjectWithId { Id = new ObjectId { Type = "TestId", Sequence = 2 }, Number = 1 };
            var sampleObject2V2 = new SampleObjectWithId { Id = new ObjectId { Type = "TestId", Sequence = 2 }, Number = 2 };

            var sampleObject3V1 = new SampleObjectWithId { Id = new ObjectId { Type = "TestId", Sequence = 3 }, Number = 1 };

            var sampleObject4V1 = new SampleObjectWithId { Id = new ObjectId { Type = "TestId", Sequence = 4 }, Number = 1 };
            var sampleObject4V2 = new SampleObjectWithId { Id = new ObjectId { Type = "TestId", Sequence = 4 }, Number = 2 };
            var sampleObject4V3 = new SampleObjectWithId { Id = new ObjectId { Type = "TestId", Sequence = 4 }, Number = 3 };
            var sampleObject4V4 = new SampleObjectWithId { Id = new ObjectId { Type = "TestId", Sequence = 4 }, Number = 4 };

            ElasticClient.IndexMany(new List<SampleObjectWithId>
            {
                sampleObject1V1, sampleObject1V2, sampleObject1V3,
                sampleObject2V1, sampleObject2V2,
                sampleObject3V1,
                sampleObject4V1, sampleObject4V2, sampleObject4V3, sampleObject4V4
            }, TestIndex.IndexNameWithVersion());
            ElasticClient.Refresh(Indices.All);

            // TEST
            var operation = new BatchUpdateOperation<JObject, JObject>(descriptor => descriptor
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .FromType<SampleObjectWithId>()
                .ToType<SampleObjectWithId>()
                .SearchDescriptor(sd => sd.MatchAll()));

            operation.Execute(ElasticClient);

            // VERIFY
            ElasticClient.Refresh(Indices.All);

            var sampleObject1Version = ElasticClient.Get<SampleObjectWithId>($"TestId-1", desc => desc.Index(TestIndex.NextIndexNameWithVersion())).Version;
            sampleObject1Version.Should().Be(3);

            var sampleObject2Version = ElasticClient.Get<SampleObjectWithId>($"TestId-2", desc => desc.Index(TestIndex.NextIndexNameWithVersion())).Version;
            sampleObject2Version.Should().Be(2);

            var sampleObject3Version = ElasticClient.Get<SampleObjectWithId>($"TestId-3", desc => desc.Index(TestIndex.NextIndexNameWithVersion())).Version;
            sampleObject3Version.Should().Be(1);

            var sampleObject4Version = ElasticClient.Get<SampleObjectWithId>($"TestId-4", desc => desc.Index(TestIndex.NextIndexNameWithVersion())).Version;
            sampleObject4Version.Should().Be(4);
        }

        [Test]
        public void TransformAsJObjects_TransformToNullIsNotIndexed()
        {
            // GIVEN
            var sampleObjectWithId1 = new SampleObjectWithId {Id = new ObjectId {Type = "TestId", Sequence = 0}};
            var sampleObjectWithId2 = new SampleObjectWithId {Id = new ObjectId {Type = "TestId", Sequence = 1}};
            ElasticClient.Index(sampleObjectWithId1, idx => idx.Index(TestIndex.IndexNameWithVersion()));
            ElasticClient.Index(sampleObjectWithId2, idx => idx.Index(TestIndex.IndexNameWithVersion()));
            ElasticClient.Refresh(Indices.All);

            // TEST
            var operation = new BatchUpdateOperation<JObject,JObject>(descriptor => descriptor
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .FromType<SampleObjectWithId>()
                .ToType<SampleObjectWithId>()
                .SearchDescriptor(sd => sd.MatchAll())
                .Transformation(doc =>
                {
                    if (doc["id"].ToObject<ObjectId>().ToString() == "TestId-0") return null;
                    return doc;
                }));

            operation.Execute(ElasticClient);

            // VERIFY
            ElasticClient.Refresh(Indices.All);

            var response0 = ElasticClient.Get<SampleObjectWithId>("TestId-0", desc => desc.Index(TestIndex.NextIndexNameWithVersion()));
            response0.IsValid.Should().BeTrue();
            response0.Found.Should().BeFalse();
            response0.Source.Should().BeNull();

            var response1 = ElasticClient.Get<SampleObjectWithId>("TestId-1", desc => desc.Index(TestIndex.NextIndexNameWithVersion()));
            response1.IsValid.Should().BeTrue();
            response1.Found.Should().BeTrue();
            response1.Source.Should().NotBeNull();
        }

        [Test]
        public void WithOnDocumentProcessed_InvokesEventHandler()
        {
            // GIVEN
            const int expectedDocumentCount = 15000;
            BulkIndexSampleObjects(expectedDocumentCount);

            // TEST
            var processedDocuments = new List<SampleObject>();
            var operation = new BatchUpdateOperation<SampleObject, SampleObject>( s => s
                .FromIndex(TestIndex.IndexNameWithVersion())
                .ToIndex(TestIndex.NextIndexNameWithVersion())
                .OnDocumentProcessed(doc =>
                {
                    processedDocuments.Add(doc);
                }));

            operation.Execute(ElasticClient);

            // VERIFY
            processedDocuments.Count.Should().Be(expectedDocumentCount);
        }
    }
}

