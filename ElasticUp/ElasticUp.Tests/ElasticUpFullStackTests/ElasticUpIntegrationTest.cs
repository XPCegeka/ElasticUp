using System;
using System.Linq;
using ElasticUp.History;
using ElasticUp.Migration;
using ElasticUp.Operation.Reindex;
using ElasticUp.Tests.Sample;
using ElasticUp.Util;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.ElasticUpFullStackTests
{
    [TestFixture]
    public class ElasticUpIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void ElasticUp_FullStackTest_MigrateObjectsToNewVersionedIndexWithoutPriorMigrationHistory()
        {
            // GIVEN
            var sampleObjects = Enumerable.Range(1, 25000).Select(n => new SampleObject {Number = n}).ToList();
            ElasticClient.IndexMany(sampleObjects, TestIndex.AliasName);
            ElasticClient.PutAlias(TestIndex.IndexNameWithVersion(), TestIndex.AliasName);
            ElasticClient.Refresh(Indices.All);

            // TEST
            new ElasticUp(ElasticClient)
                .WithMigrationHistoryIndexAliasName(MigrationHistoryTestIndex.AliasName)
                .Migration(new SampleVersionedIndexMigrationWithReindexTypeOperation())
                .Run();

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var objectCountInNewIndex = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion())).Count;
            objectCountInNewIndex.Should().Be(sampleObjects.Count);

            var migrationHistoryCountInNewIndex = ElasticClient.Count<ElasticUpMigrationHistory>(descriptor => descriptor.Index(MigrationHistoryTestIndex.AliasName)).Count;
            migrationHistoryCountInNewIndex.Should().Be(1);
            
            var indicesPointingToAlias = ElasticClient.GetIndicesPointingToAlias(TestIndex.AliasName);
            indicesPointingToAlias.Should().HaveCount(1);
            indicesPointingToAlias[0].Should().Be(TestIndex.NextIndexNameWithVersion());
        }

        [Test]
        public void ElasticUp_FullStackTest_MigrateObjectsToNewVersionedIndexWithPriorMigrationHistory()
        {
            // GIVEN
            var sampleObjects = Enumerable.Range(1, 25000).Select(n => new SampleObject { Number = n }).ToList();
            ElasticClient.IndexMany(sampleObjects, TestIndex.AliasName);
            ElasticClient.Index(new ElasticUpMigrationHistory {ElasticUpMigrationName = "Sample"}, descriptor => descriptor.Index(MigrationHistoryTestIndex.AliasName));
            ElasticClient.Refresh(Indices.All);

            // TEST
            new ElasticUp(ElasticClient)
                .WithMigrationHistoryIndexAliasName(MigrationHistoryTestIndex.AliasName)
                .Migration(new SampleVersionedIndexMigrationWithReindexTypeOperation())
                .Run();

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var objCountInNewIndex = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(TestIndex.NextIndexNameWithVersion())).Count;
            objCountInNewIndex.Should().Be(sampleObjects.Count);

            var migrationHistoryCountInNewIndex = ElasticClient.Count<ElasticUpMigrationHistory>(descriptor => descriptor.Index(MigrationHistoryTestIndex.AliasName)).Count;
            migrationHistoryCountInNewIndex.Should().Be(2);

            var indicesPointingToAlias = ElasticClient.GetIndicesPointingToAlias(TestIndex.AliasName);
            indicesPointingToAlias.Should().HaveCount(1);
            indicesPointingToAlias[0].Should().Be(TestIndex.NextIndexNameWithVersion());
        }

        [Test]
        public void ElasticUp_FullStackTest_FromAndToIndexIdentical_Exception()
        {
            ElasticClient.Index(new SampleDocument { Id = "1", Name = "Jabba/The/Hut" }, id => id.Index(TestIndex.AliasName));
            ElasticClient.Refresh(Indices.All);

            // Run migration: reindex and update document
            try
            {
                new ElasticUp(ElasticClient)
                    .WithMigrationHistoryIndexAliasName(MigrationHistoryTestIndex.AliasName)
                    .Migration(new MigrationToIdenticalIndex(TestIndex))
                    .Run();

                throw new AssertionException("The above code should throw an error");
            }
            catch (Exception e)
            {
                //all is well here on the dark side
            }
        }

        [Test]
        public void WhenAddingAMigration_MakeSureAllMigrationNamesAreUnique()
        {
            Assert.Throws<ArgumentException>(() =>
                new ElasticUp(ElasticClient)
                    .Migration(new SampleEmptyVersionedIndexMigration(TestIndex.IndexNameWithVersion()))
                    .Migration(new SampleEmptyVersionedIndexMigration(TestIndex.NextIndexNameWithVersion())));
        }


    }

    class MigrationToIdenticalIndex : ElasticUpCustomMigration
    {
        private readonly VersionedIndexName _testIndex;

        public MigrationToIdenticalIndex(VersionedIndexName testIndex)
        {
            _testIndex = testIndex;
        }

        protected override void DefineOperations()
        {
            Operations.Add(new BatchUpdateOperation<SampleDocument, SampleDocument>(descriptor => descriptor
                .FromIndex(_testIndex.IndexNameWithVersion())
                .ToIndex(_testIndex.IndexNameWithVersion())
                .FromType<SampleDocument>()
                .ToType<SampleDocument>()
                .Transformation(model =>
                {
                    model.Name = model.Name + " from Star Wars";
                    return model;
                })
                .SearchDescriptor(sd => sd.MatchAll())));
        }
    }
}