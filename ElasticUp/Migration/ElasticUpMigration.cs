namespace ElasticUp.Migration
{
    public abstract class ElasticUpMigration
    {
        private readonly int _migrationNumber;

        protected ElasticUpMigration(int migrationNumber)
        {
            _migrationNumber = migrationNumber;
        }

       

    }
}
