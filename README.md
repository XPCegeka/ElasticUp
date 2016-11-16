# ElasticUp
Easy ElasticSearch data migrations for continuous delivery! Inspired by tools like DbUp for Sql.

# Why ElasticUp

# How it works

# How to use
Add a nuget dependency in your project: https://www.nuget.org/packages/ElasticUp.ElasticUp.ElasticUp/

new ElasticUp(elasticClient)
 .Migration(new YourFirstMigration())
 .Migration(new YourSecondtMigration())
 .Migration(new YourThirdMigration())
 .Run();
