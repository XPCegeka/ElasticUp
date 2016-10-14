using System.Linq;
using ElasticUp.Migration;
using ElasticUp.Migration.Meta;
using ElasticUp.Operation;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Runner
{
    [TestFixture]
    public class ElasticUpIntegrationTest : AbstractIntegrationTest
    {
        public ElasticUpIntegrationTest()
            : base(ElasticServiceStartup.StartupForEach)
        {
            
        }

        [Test]
        public void ElasticUp_FullStackTest()
        {
            // GIVEN
            const string aliasName = "sample-index";
            var oldIndex = new VersionedIndexName(aliasName, 1);
            var newIndex = oldIndex.GetIncrementedVersion();
            var oldIndexName = oldIndex.ToString();
            var newIndexName = newIndex.ToString();

            var sampleObjects = Enumerable.Range(1, 10).Select(n => new SampleObject {Number = n}).ToList();
            ElasticClient.IndexMany(sampleObjects, oldIndexName);
            ElasticClient.PutAlias(oldIndexName, aliasName);
            ElasticClient.Refresh(Indices.All);

            // TEST
            ElasticUp.Runner.ElasticUp
                .ConfigureElasticUp(ElasticClient)
                .Migration(new SampleMigration())
                .Run();

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var objCountInNewIndex = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(newIndexName)).Count;
            objCountInNewIndex.Should().Be(sampleObjects.Count);

            //TODO introduce alias migrationss
            //var indicesPointingToAlias = ElasticClient.GetIndicesPointingToAlias(aliasName);
            //indicesPointingToAlias.Should().HaveCount(1);
            //indicesPointingToAlias[0].Should().Be(newIndexName);
        }
    }

    public class SampleMigration : ElasticUpMigration
    {
        public SampleMigration() : base(0)
        {
            OnIndexAlias("sample-index")
                .Operation(new CopyTypeOperation<SampleObject>(0));
        }
    }
}