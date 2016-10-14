namespace ElasticUp.Migration
{
    public abstract class ElasticUpMigration
    {
        public int MigrationNumber { get; }

        protected ElasticUpMigration(int migrationNumber)
        {
            MigrationNumber = migrationNumber;
        }

       

    }
}
