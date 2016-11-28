namespace ElasticUp.Migration
{
    public abstract class CustomElasticUpMigration : AbstractElasticUpMigration
    {
        private readonly string _sourceIndex;
        private readonly string _targetIndex;

        protected CustomElasticUpMigration(string sourceIndex, string targetIndex)
        {
            _sourceIndex = sourceIndex;
            _targetIndex = targetIndex;
        }

        protected override void InitIndices()
        {
            SourceIndex = _sourceIndex;
            TargetIndex = _targetIndex;
        }

        protected override void PostMigrationTasks()
        {
            AddMigrationToHistory(this);
        }
    }
}
