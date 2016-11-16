namespace ElasticUp.Migration
{
    public abstract class CustomElasticUpMigration : AbstractElasticUpMigration
    {
        private readonly string _sourceIndex;
        private readonly string _targetIndex;

        protected CustomElasticUpMigration(int migrationNumber, string sourceIndex, string targetIndex) : base(migrationNumber)
        {
            _sourceIndex = sourceIndex;
            _targetIndex = targetIndex;
        }

        protected override void InitIndices()
        {
            SourceIndex = _sourceIndex;
            TargetIndex = _targetIndex;
        }

        protected override void PreMigrationTasks()
        {
            CopyHistory(SourceIndex, TargetIndex);
        }

        protected override void PostMigrationTasks()
        {
            AddMigrationToHistory(this, TargetIndex);
        }
    }
}
