using System.Linq;
using ElasticUp.Migration.Meta;
using ElasticUp.Tests.Sample;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Runner
{
    [TestFixture]
    public class ElasticUpIntegrationTest : AbstractIntegrationTest
    {
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
                .Migration(new SampleMigrationWithCopyTypeOperation(0))
                .Run();

            // VERIFY
            ElasticClient.Refresh(Indices.All);
            var objCountInNewIndex = ElasticClient.Count<SampleObject>(descriptor => descriptor.Index(newIndexName)).Count;
            objCountInNewIndex.Should().Be(sampleObjects.Count);
            
            var indicesPointingToAlias = ElasticClient.GetIndicesPointingToAlias(aliasName);
            indicesPointingToAlias.Should().HaveCount(1);
            indicesPointingToAlias[0].Should().Be(newIndexName);
        }
    }
}