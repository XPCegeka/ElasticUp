using System.Text.RegularExpressions;

namespace ElasticUp.Migration.Meta
{
    internal class VersionedIndexName
    {
        public string Name { get; protected set; }
        public int Version { get; protected set; }

        public VersionedIndexName(string name, int version)
        {
            Name = name;
            Version = version;
        }

        public VersionedIndexName GetIncrementedVersion()
        {
            return new VersionedIndexName(Name, Version + 1);
        }

        public override string ToString()
        {
            return $"{Name}-v{Version}";
        }

        public static VersionedIndexName CreateFromIndexName(string indexName)
        {
            var pattern = new Regex(@"^(?<name>.+)-v(?<version>\d+)$", RegexOptions.IgnoreCase);
            if (!pattern.IsMatch(indexName))
                return new VersionedIndexName(indexName, 0);

            var match = pattern.Match(indexName);
            var name = match.Groups["name"].Value;
            var version = int.Parse(match.Groups["version"].Value);
            return new VersionedIndexName(name, version);
        }
    }
}