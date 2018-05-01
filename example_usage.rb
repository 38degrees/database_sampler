require 'database_sampler'

source_url = "databaseurl"
target_url = "databaseurl"

exclude_tables = ['list_members', 'mailing_logs', 'api_logs']

anonymise_cols = {
  'email' => %Q{'bla' || RANDOM()::text || 'bla' || RANDOM()::text || '@example.com'},
  'last_name' => %Q{'Jones'},
  'guid' => %Q{md5(random()::text)}
}

samples = [
  { 'table' => 'members', 'size' => '10000' },
  { 'table' => 'actions', 'size' => '5000' },
  { 'table' => 'area_zips', 'size' => '50000' },
  { 'table' => 'dataset_rows', 'size' => '10000' },
]

conditions = [
  { 'table' => 'member_mailings', 'last' => 5000000 },
  { 'table' => 'mailings', 'condition' => 'parent_mailing_id IS NULL', 'last' => 1000 },
  { 'table' => 'audits', 'condition' => 'member_id is not null' },
  { 'table' => 'clicks', 'last' => 500000 },
  { 'table' => 'opens', 'last' => 1500000 },
  { 'table' => 'smses', 'condition' => 'member_id is not null' }
]

pre_copy_sql = %Q{
  ALTER TABLE spider_export.members DROP COLUMN name;
  ALTER TABLE spider_export.actions DROP COLUMN member_actions_count;
  ALTER TABLE spider_export.actions DROP COLUMN image;
  ALTER TABLE spider_export.campaigns ADD COLUMN url TEXT;
}

cj = DatabaseSampler::CopyJob.new(source_url, 
                      target_url, 
                      exclude_tables: exclude_tables, 
                      anonymise_cols: anonymise_cols, 
                      samples: samples, 
                      conditions: conditions, 
                      schema_name: 'spider_export', 
                      drop_schema: false, 
                      pre_copy_sql: pre_copy_sql,
                      use_fks_from_target: true)

# cj.detect_schema_differences
cj.run
# cj.copy_to_target