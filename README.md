# ElasticUp for C#
Easy ElasticSearch data migrations for continuous delivery! Inspired by tools like DbUp for Sql.

# Why ElasticUp
When developing new features it is possible your datamodel changes. This may require an update of your existing data in ElasticSearch. We were looking for a tool similar to DbUp for sql, but didn't find any that matched our needs. So we decided to develop our own tool.

The requirements for ElasticUp are:
- automate the migration and updates of data in your ElasticSearch
- track history of executed migrations so they run once and only once
- offer sensible base classes, and helpers to implement your own migrations
- make it possible to create a console application that uses ElasticUp, so you can call your MigrationProject from Octopus Deploy before deploying a new version of your application

# How it works





# How to use
Add a nuget dependency in your project: https://www.nuget.org/packages/ElasticUp.ElasticUp.ElasticUp/

```
new ElasticUp(elasticClient)
 .Migration(new YourFirstMigration())
 .Migration(new YourSecondtMigration())
 .Migration(new YourThirdMigration())
 .Run();
```

# Do's and don'ts

- Don't rename your migration classes or change the migrationnumber. The migrationhistory uses a combination of the migrationnumber and the class name to track which migrations have already run.
