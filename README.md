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

Create an ElasticUp instance and add Migrations to it. Each migration can have multiple operations.

## The default case
The default use case is that your ElasticSearch has an index name with a version, and an alias without version. (Your application will talk with ElasticSearch through the alias.) 

E.g.

**Alias**:  myindex

**IndexName**: myindex-v3


When writing a migration (by extending DefaultElasticUpMigration) the following will happen:

1. find out the IndexName and version based on the given alias. ElasticUp now understands it will migrate something from myindex-v3 to myindex-v4

2. check if the migration has already run by checking the MigrationHistory documents in the source index

3. copy the MigrationHistory documents from the source index to the target index

4. do the actual migration work by running all operations in the migration (this is your actual code)

5. add this migration to the MigrationHistory in the target index

6. move the alias from the source index to the target index

Because your application talks to ElasticSearch by using the alias, your migration will have no impact on the application. It just migrates data from myindex-v3 to myindex-v4 and moves the alias to myindex-v4.

## The custom case
In some cases you may want to do something different than migrating from vX to vY. In this case your migration can extend CustomElasticUpMigration. You can override and implement almost any step of a migration, so you are free to do as you want.

**With great power comes great responsibility**: remember, if you add custom migrations, make sure you don't forget to add the MigrationHistory either your source index or target index. And implement your check if the migration already ran accordingly.


# How to use
Add a nuget dependency in your project: https://www.nuget.org/packages/ElasticUp.ElasticUp.ElasticUp/

```
new ElasticUp(elasticClient)
 .Migration(new MyFirstMigration("myindexalias"))
 .Migration(new MySecondtMigration("myindexalias"))
 .Migration(new MyThirdMigration("myindexalias"))
 .Run();
 

public class YourFirstMigration : DefaultElasticUpMigration 
{
  public YourFirstMigration(string alias) : base(1, alias)
  {
      Operation(new CopyTypeOperation<SampleObject>(1)); //this operation is offered by ElasticUp to just copy the documents from/to
      Operation(new CopyTypeOperation<OtherSampleObject>(2)); //this operation is offered by ElasticUp to just copy the documents from/to
  }
} 
```




# Do's and don'ts

- DON'T rename your migration classes or change the migrationnumber. The migrationhistory uses a combination of the migrationnumber and the class name to track which migrations have already run.
- DON'T 
