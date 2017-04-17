using System.Linq;
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
    public class CopyTypeMappingOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void CopyMapping_CopiesMappingToNewIndex()
        {
            //Given
            var mapping = ResourceUtilities.FromResourceFileToString("mapping_v0_sampledocument.json");
            var type = typeof(SampleDocument).Name.ToLowerInvariant();

            new PutTypeMappingOperation(type)
                    .OnIndex(TestIndex.IndexNameWithVersion())
                    .WithMapping(mapping)
                    .Execute(ElasticClient);

            var responseTestIndex = ElasticClient.GetMapping(new GetMappingRequest(Indices.Parse(TestIndex.IndexNameWithVersion())));
            responseTestIndex.Mappings.ToList()[0].Key.Should().Be(TestIndex.IndexNameWithVersion());
            ((StringProperty)responseTestIndex.Mappings.ToList()[0].Value[0].Properties.ToList()[1].Value).Index.Should().Be(FieldIndexOption.NotAnalyzed);

            //When
            new CopyTypeMappingOperation(type)
                    .FromIndex(TestIndex.IndexNameWithVersion())
                    .ToIndex(TestIndex.NextIndexNameWithVersion())
                    .Execute(ElasticClient);

            //Then
            var responseNextTestIndex = ElasticClient.GetMapping(new GetMappingRequest(Indices.Parse(TestIndex.NextIndexNameWithVersion())));
            responseNextTestIndex.Mappings.ToList()[0].Key.Should().Be(TestIndex.NextIndexNameWithVersion());
            ((StringProperty)responseNextTestIndex.Mappings.ToList()[0].Value[0].Properties.ToList()[1].Value).Index.Should().Be(FieldIndexOption.NotAnalyzed);
        }

        [Test]
        public void CopyMapping_ValidatesSettings()
        {
            Assert.Throws<ElasticUpException>(() => new CopyTypeMappingOperation(null).FromIndex(TestIndex.IndexNameWithVersion()).ToIndex(TestIndex.NextIndexNameWithVersion()).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CopyTypeMappingOperation(" ").FromIndex(TestIndex.IndexNameWithVersion()).ToIndex(TestIndex.NextIndexNameWithVersion()).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CopyTypeMappingOperation(typeof(SampleDocument).Name).FromIndex(null).ToIndex(TestIndex.NextIndexNameWithVersion()).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CopyTypeMappingOperation(typeof(SampleDocument).Name).FromIndex(TestIndex.IndexNameWithVersion()).ToIndex(null).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CopyTypeMappingOperation(typeof(SampleDocument).Name).FromIndex(" ").ToIndex(TestIndex.NextIndexNameWithVersion()).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CopyTypeMappingOperation(typeof(SampleDocument).Name).FromIndex(TestIndex.IndexNameWithVersion()).ToIndex(" ").Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CopyTypeMappingOperation(typeof(SampleDocument).Name).FromIndex("does-not-exist").ToIndex(TestIndex.NextIndexNameWithVersion()).Validate(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new CopyTypeMappingOperation(typeof(SampleDocument).Name).FromIndex(TestIndex.IndexNameWithVersion()).ToIndex("does-not-exist").Validate(ElasticClient));
        }
    }
}