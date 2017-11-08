using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nest;

namespace ElasticUp.Helper
{
    public class IndexSettingsForBulkHelper : IDisposable
    {
        private readonly IElasticClient _elasticClient;
        private readonly string _index;
        private Time _settingsRefreshInterval;
        private int? _settingsNumberOfReplicas;

        public IndexSettingsForBulkHelper(IElasticClient elasticClient, string index)
        {
            _elasticClient = elasticClient;
            _index = index;

            PrepareForBulkIndex();
        }

        private void PrepareForBulkIndex()
        {
            var getIndexSettingsResponse = _elasticClient.GetIndexSettings(s => s.Index(_index));
            _settingsRefreshInterval = getIndexSettingsResponse.Indices[_index].Settings.RefreshInterval;
            _settingsNumberOfReplicas = getIndexSettingsResponse.Indices[_index].Settings.NumberOfReplicas;
            _elasticClient.UpdateIndexSettings(_index, s => s.IndexSettings(p => p.NumberOfReplicas(0).RefreshInterval(new Time(-1))));
        }

        public void Dispose()
        {
            _elasticClient.UpdateIndexSettings(_index, s => s.IndexSettings(p => p.NumberOfReplicas(_settingsNumberOfReplicas).RefreshInterval(_settingsRefreshInterval)));
        }
    }
}
