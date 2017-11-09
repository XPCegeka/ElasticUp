using System;
using ElasticUp.Helper;
using FluentAssertions;
using Nest;
using NUnit.Framework;

namespace ElasticUp.Tests.Helper
{
    public class IndexSettingsForBulkHelperIntegrationTest : AbstractIntegrationTest
    {
       
        public void SetupIndex(Func<IndexSettingsDescriptor, IndexSettingsDescriptor> indexSettings)
        {
            ElasticClient.DeleteIndex(TestIndex.IndexNameWithVersion());
            
            ElasticClient.CreateIndex(
                TestIndex.IndexNameWithVersion(),
                indexDescriptor => indexDescriptor
                    .Settings(indexSettings));
        }

        [Test]
        public void IndexSettingsForBulkHelper_SetsRefreshIntervalAndNumberOfReplicas_AndRevertsSettingsToDefaultsIfTheyDidntExistYetAfterwards()
        {
            SetupIndex(indexSettings => indexSettings);

            var settingsBefore = GetIndexSettings();
            settingsBefore.RefreshInterval.Should().BeNull();
            settingsBefore.NumberOfReplicas.Should().Be(1); //because 1 is default appearantly

            using (new IndexSettingsForBulkHelper(ElasticClient, TestIndex.IndexNameWithVersion()))
            {
                var settingsDuring = GetIndexSettings();
                settingsDuring.RefreshInterval.Should().Be(new Time(-1));
                settingsDuring.NumberOfReplicas.Should().Be(0);
            }

            var settingsAfter = GetIndexSettings();
            settingsAfter.RefreshInterval.Should().Be(IndexSettingsForBulkHelper.DefaultRefreshInterval);
            settingsAfter.NumberOfReplicas.Should().Be(IndexSettingsForBulkHelper.DefaultNumberOfReplicas);
        }

        [Test]
        public void IndexSettingsForBulkHelper_SetsRefreshIntervalAndNumberOfReplicas_AndRevertsSettingsAfterwards()
        {
            var refreshInterval = new Time(TimeSpan.FromSeconds(2));
            var numberOfReplicas = 2;

            SetupIndex(indexSettings => indexSettings.RefreshInterval(refreshInterval).NumberOfReplicas(numberOfReplicas));

            var settingsBefore = GetIndexSettings();
            settingsBefore.RefreshInterval.Should().Be(refreshInterval);
            settingsBefore.NumberOfReplicas.Should().Be(numberOfReplicas);

            using (new IndexSettingsForBulkHelper(ElasticClient, TestIndex.IndexNameWithVersion()))
            {
                var settingsDuring = GetIndexSettings();
                settingsDuring.RefreshInterval.Should().Be(new Time(-1));
                settingsDuring.NumberOfReplicas.Should().Be(0);
            }

            var settingsAfter = GetIndexSettings();
            settingsAfter.RefreshInterval.Should().Be(refreshInterval);
            settingsAfter.NumberOfReplicas.Should().Be(numberOfReplicas);
        }

        [Test]
        public void IndexSettingsForBulkHelper_DoesNotChangeSettingsIfDisabled()
        {
            var refreshInterval = new Time(TimeSpan.FromSeconds(2));
            var numberOfReplicas = 2;

            SetupIndex(indexSettings => indexSettings.RefreshInterval(refreshInterval).NumberOfReplicas(numberOfReplicas));

            var settingsBefore = GetIndexSettings();
            settingsBefore.RefreshInterval.Should().Be(refreshInterval);
            settingsBefore.NumberOfReplicas.Should().Be(numberOfReplicas);

            using (new IndexSettingsForBulkHelper(ElasticClient, TestIndex.IndexNameWithVersion(), false))
            {
                var settingsDuring = GetIndexSettings();
                settingsDuring.RefreshInterval.Should().Be(refreshInterval);
                settingsDuring.NumberOfReplicas.Should().Be(numberOfReplicas);
            }

            var settingsAfter = GetIndexSettings();
            settingsAfter.RefreshInterval.Should().Be(refreshInterval);
            settingsAfter.NumberOfReplicas.Should().Be(numberOfReplicas);
        }

        private IIndexSettings GetIndexSettings()
        {
            var response = ElasticClient.GetIndexSettings(s => s.Index(TestIndex.IndexNameWithVersion()));
            return response.Indices[TestIndex.IndexNameWithVersion()].Settings;
        }
    }
}
