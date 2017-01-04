namespace ElasticUp.Migration
{
    public abstract class ElasticUpCustomMigration : AbstractElasticUpMigration
    {
        protected sealed override void BeforeMigration() {}

        protected sealed override void AfterMigration()
        {
            AddMigrationToHistory(this);
        }
    }
}
