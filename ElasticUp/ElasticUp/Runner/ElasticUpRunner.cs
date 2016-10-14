using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ElasticUp.Migration;
using Nest;

namespace ElasticUp.Runner
{
    public class ElasticUpRunner
    {
        private IElasticClient _elasticClient;
        public readonly IList<ElasticUpMigration> Migrations = new List<ElasticUpMigration>();
        
        private ElasticUpRunner(IElasticClient elasticClient)
        {
            this._elasticClient = elasticClient;
        }

        public static ElasticUpRunner CreateElasticUpRunner(IElasticClient elasticClient)
        {
            return new ElasticUpRunner(elasticClient);
        }

        public ElasticUpRunner Migration(ElasticUpMigration migration)
        {

            Migrations.Add(migration);
            return this;
        }

        public void Run()
        {
            //TODO 
        }

    }
}
