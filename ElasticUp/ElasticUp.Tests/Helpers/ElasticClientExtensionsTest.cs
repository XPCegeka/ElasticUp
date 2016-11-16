using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using ElasticUp.Helpers;
using ElasticUp.Tests.Sample;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Helpers
{
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

        [Test]
        public void DoScrollAsync_IsFasterThanDoScroll()
        {
            // GIVEN
            const string index = "from";
            var filler = string.Concat(Enumerable.Repeat("X", 512));
            var documents = Enumerable.Range(0, 150000).Select(n => new SampleDocument { Name = $"{n}-{filler}"}).ToList();
            ElasticClient.IndexMany(documents, index);
            ElasticClient.Refresh(Indices.All);

            // TEST
            Action<IEnumerable<SampleDocument>> simulatedActivity = _ => Thread.Sleep(500);

            var elapsedTimeAsync = Stopwatch.StartNew();
            ElasticClient.DoScrollAsync(descriptor => descriptor.Index(index).MatchAll(), simulatedActivity).Wait();
            elapsedTimeAsync.Stop();

            var elapsedTimeSync = Stopwatch.StartNew();
            ElasticClient.DoScroll(descriptor => descriptor.Index(index).MatchAll(), simulatedActivity);
            elapsedTimeSync.Stop();

            // VERIFY
            elapsedTimeAsync.Elapsed.Should().BeLessThan(elapsedTimeSync.Elapsed);
        }
    }
}
