# Database Sampler

Copy a sample of your database into another database, respecting foreign keys. 

This may be useful for example to create a staging database with realistic data from your production database.

## Usage

```
require 'database_sampler'

source_url = "postgres://user:pass@host:5432/prod"

target_url = "postgres://user:pass@host:5432/staging"


exclude_tables = ['audits']

anonymise_cols = {
  'email' => %Q{'bla' || RANDOM()::text || 'bla' || RANDOM()::text || '@example.com'},
  'last_name' => %Q{'Jones'},
  'guid' => %Q{md5(random()::text)}
}

samples = [
  { 'table' => 'members', 'size' => '20000' },
  { 'table' => 'mailings', 'size' => '300' },
  { 'table' => 'actions', 'size' => '5000' },
  { 'table' => 'area_zips', 'size' => '50000' },
  { 'table' => 'dataset_rows', 'size' => '100000' },
]

manual_links = [{
  'table_name' => 'member_mailings',
  'parent_table' => 'mailings',
  'column' => 'mailing_id',
  'parent_column' => 'id'
  }]

cj = DatabaseSampler::CopyJob.new(source_url, 
                      target_url, 
                      exclude_tables: exclude_tables, 
                      anonymise_cols: anonymise_cols, 
                      samples: samples, 
                      schema_name: 'sample_export', 
                      drop_schema: false, 
                      use_fks_from_target: true)
cj.run

```

### Caching the sample

The sampled data gets written into a separate schema in the source database. If you wish to rebuild a staging database more quickly you can just run CopyJob#copy_to_target to recopy the cached data, which could save a large amount of time depending on the size of your source database. 

### Use Foreign Keys from the Target database

You may have a schema in the target database which is stricter than the source database. You can use the foreign keys from the target database instead of the source database by passing `use_fks_from_target: true` to the new CopyJob. 

## Limitations

- Only supports foreign keys that reference the `id` field of the parent table.
- If the schemas of the tables differ at all you'll get errors - you can use the post_copy_sql parameter to add SQL that fixes this, but it's still a manual process.
- Self relations aren't properly supported - so you need to make sure there aren't any self-relations using conditions

## Recommendations

Run with a database user that has read-only access to your production schema, for additional peace of mind.
