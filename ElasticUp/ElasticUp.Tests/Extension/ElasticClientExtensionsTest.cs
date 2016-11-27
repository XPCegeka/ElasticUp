using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using ElasticUp.Extension;
using ElasticUp.Tests.Sample;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Extension
{
    [Parallelizable]
    [TestFixture]
    public class ElasticClientExtensionsTest : AbstractIntegrationTest
    {
        [Test]
        public void DoScrollAsync_ReturnsDocuments()
        {
            // GIVEN
            const string index = "index";
            var documents = Enumerable.Range(0, 5000).Select(n => new SampleObject { Number = n });
            ElasticClient.IndexMany(documents, index);
            ElasticClient.Refresh(Indices.All);

            // TEST
            var actualDocuments = new List<SampleObject>();
            ElasticClient.DoScrollAsync<SampleObject>(descriptor => descriptor.Index(index).MatchAll(), objects => actualDocuments.AddRange(objects)).Wait();

            // VERIFY
            actualDocuments.ShouldBeEquivalentTo(actualDocuments);
        }
    }
}
