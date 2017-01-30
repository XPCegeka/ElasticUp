using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using ElasticUp.Elastic;
using ElasticUp.Operation.Index;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Operation.Index
{
    [TestFixture]
    public class DeleteIndexOperationIntegrationTest : AbstractIntegrationTest
    {
        [Test]
        public void DeletesIndex()
        {
            var customIndexName = TestIndex.IndexNameWithVersion() + "-custom";
            ElasticClient.CreateIndex(customIndexName);
            ElasticClient.IndexExists(customIndexName).Exists.Should().BeTrue();

            new DeleteIndexOperation(customIndexName).Execute(ElasticClient);
            
            ElasticClient.IndexExists(customIndexName).Exists.Should().BeFalse();
        }

        [Test]
        public void DeleteIndex_ValidatesSettings()
        {
            var customIndexName = TestIndex.IndexNameWithVersion() + "-custom";
            Assert.Throws<ElasticUpException>(() => new DeleteIndexOperation(null).Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new DeleteIndexOperation(" ").Execute(ElasticClient));
            Assert.Throws<ElasticUpException>(() => new DeleteIndexOperation(customIndexName).Execute(ElasticClient));
        }
    }
}