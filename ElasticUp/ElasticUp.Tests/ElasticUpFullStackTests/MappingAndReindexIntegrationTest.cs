using System.Linq;
using System.Threading;
using ElasticUp.Migration;
using ElasticUp.Operation.Mapping;
using ElasticUp.Operation.Reindex;
using ElasticUp.Tests.Infrastructure;
using ElasticUp.Tests.Sample;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.ElasticUpFullStackTests
{
    [TestFixture]
    public class MappingAndReindexIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void ElasticUp_FullStackTest_GivenADocumentWithAnalyzedTextThatIsNotFoundWithTermQuery_AfterSettingMappingAsNotAnalyzedAndReindexing_ItCanBeFoundWithTermQuery()
        {
            const string jabbaTheHut = "Jabba/The/Hut";

            //Index sampledocument with name field that will be analyzed, and because of that it can't be searched using a Term query
            ElasticClient.Index(new SampleDocument {Id = "1", Name = jabbaTheHut}, id => id.Index(TestIndex.AliasName));
            ElasticClient.Refresh(Indices.All);
            ElasticClient.Get<SampleDocument>("1", g => g.Index(TestIndex.AliasName)).Should().NotBeNull();
            ElasticClient.Search<SampleDocument>(s => s.Index(TestIndex.AliasName).Query(q => q.Term(t => t.Name, jabbaTheHut))).Documents.Should().HaveCount(0);

            // Run migration: post mapping in new index, reindex sampledocuments from old index to new index
            new ElasticUp(ElasticClient)
                .WithMigrationHistoryIndexAliasName(MigrationHistoryTestIndex.AliasName)
                .Migration(new SampleMigrationWithMappingAndReindex(TestIndex.AliasName))
                .Run();

            // After ElasticUpMigration, the Alias will have been moved to the new versioned index. Since this index contains the updated mapping the sampledocument should now be found using the Term query
            ElasticClient.Get<SampleDocument>("1", g => g.Index(TestIndex.AliasName)).Should().NotBeNull();
            SpinWait.SpinUntil(() => ElasticClient.Search<SampleDocument>(s => s.Index(TestIndex.AliasName).Query(q => q.Term(t => t.Name, jabbaTheHut))).Documents.Count() == 1);
        }
    }

    internal class SampleMigrationWithMappingAndReindex : ElasticUpVersionedIndexMigration
    {
        public SampleMigrationWithMappingAndReindex(string alias) : base(alias) { }

        protected override void DefineOperations()
        {
            Operation(new PutTypeMappingOperation<SampleDocument>().OnIndex(ToIndexName).WithMapping(ResourceUtilities.FromResourceFileToString("mapping_v0_sampledocument.json")));
            Operation(new ReindexTypeOperation<SampleDocument>().FromIndex(FromIndexName).ToIndex(ToIndexName));
        }
    }
}