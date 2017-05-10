using System.Linq;
using Elasticsearch.Net;
using ElasticUp.Operation.Mapping;
using ElasticUp.Tests.Infrastructure;
using ElasticUp.Tests.Sample;
using ElasticUp.Util;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Mapping
{
    [TestFixture]
    public class PutTypeMappingOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void PutMapping_SetsCorrectMapping()
        {
            var mapping = ResourceUtilities.FromResourceFileToString("mapping_v0_sampledocument.json");

            var type = typeof(SampleDocument).Name.ToLowerInvariant();

            new PutTypeMappingOperation(type)
                    .OnIndex(TestIndex.IndexNameWithVersion())
                    .WithMapping(mapping)
                    .Execute(ElasticClient);

            var response = ElasticClient.GetMapping(new GetMappingRequest(Indices.Parse(TestIndex.IndexNameWithVersion())));
            response.Mappings.ToList()[0].Key.Should().Be(TestIndex.IndexNameWithVersion());
            ((StringProperty)response.Mappings.ToList()[0].Value[0].Properties.ToList()[1].Value).Index.Should().Be(FieldIndexOption.NotAnalyzed);
        }

        [Test]
        public void PutMapping_AddAFieldToTheMappingUpdatesTheMapping()
        {
            var mapping = ResourceUtilities.FromResourceFileToString("mapping_v0_sampledocument.json");
            var mapping2 = ResourceUtilities.FromResourceFileToString("mapping_v1_sampledocument.json");

            var type = typeof(SampleDocument).Name.ToLowerInvariant();

            new PutTypeMappingOperation(type).OnIndex(TestIndex.IndexNameWithVersion()).WithMapping(mapping).Execute(ElasticClient);
            new PutTypeMappingOperation(type).OnIndex(TestIndex.IndexNameWithVersion()).WithMapping(mapping2).Execute(ElasticClient);

            var response = ElasticClient.GetMapping(new GetMappingRequest(Indices.Parse(TestIndex.IndexNameWithVersion())));
            response.Mappings.ToList()[0].Key.Should().Be(TestIndex.IndexNameWithVersion());
            response.Mappings.ToList()[0].Value[0].Properties.ToList()[1].Key.Name.Should().Be("name");
            response.Mappings.ToList()[0].Value[0].Properties.ToList()[2].Key.Name.Should().Be("test");
        }

        [Test]
        public void PutMapping_ChangeAnalyzerOfAField_ThrowsExceptionBecauseCannotUpdateThis()
        {
            var mapping = ResourceUtilities.FromResourceFileToString("mapping_v1_sampledocument.json");
            var mapping2 = ResourceUtilities.FromResourceFileToString("mapping_v2_sampledocument.json");

            var type = typeof(SampleDocument).Name.ToLowerInvariant();

            new PutTypeMappingOperation(type).OnIndex(TestIndex.IndexNameWithVersion()).WithMapping(mapping).Execute(ElasticClient);
            Assert.Throws<ElasticsearchClientException>(() => new PutTypeMappingOperation(type).OnIndex(TestIndex.IndexNameWithVersion()).WithMapping(mapping2).Execute(ElasticClient));
        }

        [Test]
        public void PutMapping_ValidatesSettings()
        {
            Assert.Throws<ElasticUpException>(() => new PutTypeMappingOperation(null).OnIndex(TestIndex.IndexNameWithVersion()).WithMapping("{}").Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new PutTypeMappingOperation(" ").OnIndex(TestIndex.IndexNameWithVersion()).WithMapping("{}").Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new PutTypeMappingOperation(typeof(SampleDocument).Name).OnIndex(null).WithMapping("{}").Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new PutTypeMappingOperation(typeof(SampleDocument).Name).OnIndex(" ").WithMapping("{}").Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new PutTypeMappingOperation(typeof(SampleDocument).Name).OnIndex(TestIndex.IndexNameWithVersion()).WithMapping(null).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new PutTypeMappingOperation(typeof(SampleDocument).Name).OnIndex(TestIndex.IndexNameWithVersion()).WithMapping(" ").Validate(ElasticClient));
        }
    }
}