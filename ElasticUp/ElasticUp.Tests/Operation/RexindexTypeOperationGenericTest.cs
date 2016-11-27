using ElasticUp.Operation;
using ElasticUp.Tests.Sample;
using FluentAssertions;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation
{
    [TestFixture]
    public class RexindexTypeOperationGenericTest
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