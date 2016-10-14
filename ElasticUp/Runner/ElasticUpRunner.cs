using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ElasticUp.Migration;

namespace ElasticUp.Runner
{
    public class ElasticUpRunner
    {
        public readonly IList<ElasticUpMigration> Migrations = new List<ElasticUpMigration>();

        private ElasticUpRunner() {}

        public static ElasticUpRunner Create()
        {
            return new ElasticUpRunner();
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
