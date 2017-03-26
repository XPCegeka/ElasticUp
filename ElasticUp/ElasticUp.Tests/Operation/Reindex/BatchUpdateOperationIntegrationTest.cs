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
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Reindex
{
    [TestFixture]

    public class BatchUpdateOperationPerformanceIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        [Ignore("Not yet operational")]
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
        public void Execute_ValidatesThatFromAndToIndexAreDefinedAndExistInElasticSearch()
        {
            Assert.Throws<ElasticUpException>(() => new BatchUpdateTypeOperation<SampleObject>().FromIndex(null).ToIndex(TestIndex.NextIndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateTypeOperation<SampleObject>().FromIndex(" ").ToIndex(TestIndex.NextIndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateTypeOperation<SampleObject>().FromIndex(TestIndex.IndexNameWithVersion()).ToIndex(null).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateTypeOperation<SampleObject>().FromIndex(TestIndex.IndexNameWithVersion()).ToIndex("").Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateTypeOperation<SampleObject>().FromIndex("does not exist").ToIndex(TestIndex.NextIndexNameWithVersion()).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new BatchUpdateTypeOperation<SampleObject>().FromIndex(TestIndex.IndexNameWithVersion()).ToIndex("does not exist").Execute(ElasticClient));
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
                    if (doc["Id"].Value<ObjectId>().ToString() == "TestId-0") return null;
                    return doc;
                }));

            operation.Execute(ElasticClient);

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Get<SampleObjectWithId>("TestId-0", desc => desc.Index(TestIndex.NextIndexNameWithVersion())).Should().NotBeNull();
            ElasticClient.Get<SampleObjectWithId>("TestId-1", desc => desc.Index(TestIndex.NextIndexNameWithVersion())).Should().NotBeNull();
        }
    }
}

