using System.Text.RegularExpressions;

namespace ElasticUp.Util
{
    public class VersionedIndexName
    {
        public string AliasName { get; protected set; }
        public int Version { get; protected set; }

        public VersionedIndexName(string aliasName, int version)
        {
            AliasName = aliasName;
            Version = version;
        }

        public string IndexNameWithVersion()
        {
            return $"{AliasName}-v{Version}";
        }
        public VersionedIndexName NextVersion()
        {
            return new VersionedIndexName(AliasName, Version + 1);
        }

        public string NextIndexNameWithVersion()
        {
            return NextVersion().IndexNameWithVersion();
        }

        public static VersionedIndexName CreateFromIndexName(string indexName)
        {
            var pattern = new Regex(@"^(?<aliasName>.+)-v(?<version>\d+)$", RegexOptions.IgnoreCase);
            if (!pattern.IsMatch(indexName))
                return new VersionedIndexName(indexName, 0);

            var match = pattern.Match(indexName);
            var name = match.Groups["aliasName"].Value;
            var version = int.Parse(match.Groups["version"].Value);
            return new VersionedIndexName(name, version);
        }

        public static implicit operator string(VersionedIndexName indexName)
        {
            return indexName.IndexNameWithVersion();
        }
    }
}