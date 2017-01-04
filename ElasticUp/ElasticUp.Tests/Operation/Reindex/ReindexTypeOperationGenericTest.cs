using ElasticUp.Operation.Reindex;
using ElasticUp.Tests.Sample;
using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Reindex
{
    [TestFixture]
    public class ReindexTypeOperationGenericTest
    {
        [Test]
        public void Generic_UsesNameOfTypeForTypeName()
        {
            // WHEN
            var operation = new ReindexTypeOperation<SampleDocument>();
            operation.TypeName.Should().Be("sampledocument");
        }
    }
}