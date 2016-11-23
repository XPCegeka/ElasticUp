using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ElasticUp.Elastic;
using ElasticUp.History;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Elastic
{
    [TestFixture]
    public class ElasticClientHelperIntegrationTest : AbstractIntegrationTest
    {

        [Test]
        public void GivenAnIResponseFromElasticClient_WhenValidating_ThrowExceptionContainingInformationAboutError()
        {
            var response = ElasticClient.Get<ElasticUpMigrationHistory>("migration01", descriptor => descriptor.Index("not-existing-index"));

            try
            {
                ElasticClientHelper.ValidateElasticResponse(response);
                Assert.Fail("expected exception");
            }
            catch (ElasticUpException expectedException)
            {
                expectedException.ServerError.Status.Should().Be(404);
                expectedException.ServerError.Error.Reason.Should().Be("no such index");
                expectedException.DebugInformation.StartsWith("Invalid NEST response built from a unsuccessful low level call on GET: /not-existing-index/elasticupmigrationhistory/migration01").Should().BeTrue();
                expectedException.Message.Should().Be("Exception when calling elasticsearch");
            }
        }
    }
}
