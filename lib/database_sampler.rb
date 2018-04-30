require "database_sampler/version"
require 'active_support/all'
require 'pg'

module DatabaseSampler
  class CopyJob
    def initialize(source_database_url, target_database_url, exclude_tables: [], anonymise_cols: [], samples: [], conditions: [], schema_name: 'spider_export', drop_schema: false, manual_links: [], pre_copy_sql: nil, use_fks_from_target: false)
      @source_database_url = source_database_url
      @source_conn = PG.connect(source_database_url)
      @source_conn.set_notice_receiver { |r| } # Stop all the NOTICE truncation messages

      @target_database_url = target_database_url
      @target_conn = PG.connect(target_database_url)
      @target_conn.set_notice_receiver { |r| } # Stop all the NOTICE truncation messages

      @exclude_tables = exclude_tables
      @anonymise_cols = anonymise_cols
      @samples = samples
      @conditions = conditions
      @schema_name = schema_name
      @drop_schema = drop_schema
      @manual_links = manual_links
      @pre_copy_sql = pre_copy_sql
      @use_fks_from_target = use_fks_from_target
    end

    def copy_to_target
      puts @pre_copy_sql
      @source_conn.exec(@pre_copy_sql) if @pre_copy_sql
      tables = get_tables_in_order
      tables.each do |table, _|
        column_string = get_columns_for_table(table).map { |c| %Q{\\"#{c}\\"} }.join(', ')
        puts table
        `psql #{@source_database_url} --command "\\copy (SELECT #{column_string} FROM #{@schema_name}.#{table}) TO tmp_copy.csv WITH CSV"`
        result = @target_conn.exec("TRUNCATE #{table} CASCADE")
        puts "Truncated #{result.cmd_tuples}" if result.cmd_tuples > 0
        `psql #{@target_database_url} --command "\\copy #{table} (#{column_string}) FROM tmp_copy.csv WITH CSV"`
        `rm tmp_copy.csv`
      end
    end

    def run
      get_tables_in_order # Ensure that we can get a successful ordering before we continue
      puts "Possible copy order found"

      if @drop_schema
        puts "Dropping..."
        drop_schema 
      end

      puts "Setting up..."
      setup_schema
      puts "Making samples..."
      make_sample_tables

      copy_all
      anonymise

      puts "Copying to target..."
      copy_to_target

      puts "Dropping schema..."
      drop_schema if @drop_schema
      puts "Done"
    end

    def detect_schema_differences 
      # Columns
      column_sql = "SELECT table_name, column_name, column_default, is_nullable, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name not ilike 'list_members_part_%'"
      source_cols = @source_conn.exec(column_sql).to_a
      target_cols = @target_conn.exec(column_sql).to_a
      both_cols = target_cols.map { |c| c.slice('table_name', 'column_name') } & source_cols.map { |c| c.slice('table_name', 'column_name') } 
      source_only_cols = source_cols.map { |c| c.slice('table_name', 'column_name') } - both_cols
      target_only_cols = target_cols.map { |c| c.slice('table_name', 'column_name') } - both_cols

      different_definition_cols = []
      both_cols.map do |col|
        source_col = source_cols.select{|scol| scol['table_name'] == col['table_name'] && scol['column_name'] == col['column_name'] }.first
        target_col = source_cols.select{|tcol| tcol['table_name'] == col['table_name'] && tcol['column_name'] == col['column_name'] }.first
        unless source_col == target_col
          different_definition_cols.push([source_col, target_col])
        end
      end

      # Foreign Keys
      source_fks = get_foreign_keys
      target_fks = get_foreign_keys(false)
      both_fks = source_fks & target_fks
      source_only_fks = source_fks - both_fks
      target_only_fks = target_fks - both_fks

      return {source_only_cols: source_only_cols, target_only_cols: target_only_cols, different_definition_cols: different_definition_cols, source_only_fks: source_only_fks, target_only_fks: target_only_fks}

    end

    # private

    def get_foreign_keys(source=true)
      sql = %Q{SELECT
        t.table_name,
        c.column,
        c.parent_table,
        c.parent_column
      FROM information_schema.tables t
      LEFT JOIN (
        SELECT 
          REPLACE(conrelid::regclass::text, 'public.','') AS table_name,
          conname as constraint_name,
          split_part(split_part(pg_get_constraintdef(c.oid),'(',2),')',1) as column,
          REPLACE(split_part(split_part(pg_get_constraintdef(c.oid),' ',5),'(',1), 'public.','') AS parent_table,
          TRIM(split_part(split_part(pg_get_constraintdef(c.oid),' ',5),'(',2), ')') AS parent_column  
        FROM pg_constraint c 
        JOIN pg_namespace n 
          ON n.oid = c.connamespace
        WHERE contype IN ('f') 
          AND n.nspname = 'public'
      ) c
        ON (t.table_name = c.table_name)
      WHERE t.table_schema = 'public'
        AND t.table_name NOT ilike 'list_members_part_list_ids_%'
        AND (c.parent_table IS NULL OR t.table_name != c.parent_table)
      ORDER BY t.table_name} # To avoid loops we remove self-references for now
      
      if source
        @foreign_keys_source ||= (@source_conn.exec(sql).to_a + @manual_links).reject{ |fk| @exclude_tables.include?(fk['table_name']) || @exclude_tables.include?(fk['parent_table'])}
        return @foreign_keys_source
      else
        @foreign_keys_target ||= (@target_conn.exec(sql).to_a + @manual_links).reject{ |fk| @exclude_tables.include?(fk['table_name']) || @exclude_tables.include?(fk['parent_table'])}
        return @foreign_keys_target
      end
    end

    def get_children(table_name)
      foreign_keys = get_foreign_keys(!@use_fks_from_target)
      foreign_keys.select{ |r| r['parent_table'] == table_name }.map{ |r| r['table_name'] }
    end


    # Build a map of the database - each item in the network hash corresponds to a table, and contains parents and children keys, detailing its links
    def get_network
      foreign_keys = get_foreign_keys(!@use_fks_from_target)

      network = {}
      foreign_keys.each do |row|
        data = network[row['table_name']] || {parents_count: 0, parents_remaining: 0, parents: {}, children: []}
        if row['parent_table']
          data[:parents_count] += 1
          data[:parents_remaining] += 1
          data[:parents][row['parent_table']] = { source_column: row['column'], target_column: row['parent_column'] }
        end

        data[:children] = get_children(row['table_name']) || []

        network[row['table_name']] = data
      end

      # If a table has no parents or children, reject it
      network.reject!{ |k, v| v[:parents_count] == 0 && v[:children].count == 0 }

      return network
    end

    def parent_conditions(table_name)
      network = get_network
      network[table_name][:parents].map do |parent, cols|
        "(#{cols[:source_column]} IS NULL OR #{cols[:source_column]} IN (SELECT #{cols[:target_column]} FROM #{@schema_name}.#{parent}))"
      end
    end

    def copy_table(table_name)
      target = "#{@schema_name}.#{table_name}"
      @source_conn.exec("CREATE TABLE IF NOT EXISTS #{target} (LIKE #{table_name} INCLUDING INDEXES)") unless table_exists?(table_name)
      @source_conn.exec("TRUNCATE #{target}")

      conditions = parent_conditions(table_name)
      if sample_tables.include?(table_name)
        conditions.push("id IN (SELECT id FROM #{@schema_name}.sample_#{table_name})")
      end
      if condition_tables.include?(table_name)
        conditions.push(get_condition_for_table(table_name))
      end

      where_string = if conditions.count > 0
        "WHERE " + conditions.join(' AND ')
      else
        ""
      end

      insert_sql = "INSERT INTO #{target} 
        SELECT * 
        FROM #{table_name} 
        #{where_string}"

      print "#{table_name}... "
      result = @source_conn.exec(insert_sql)
      puts result.cmd_tuples
    end

    def get_tables_in_order
      network = get_network
      copy_order = []

      while true
        tables = network.select { |k,v| v[:parents_remaining] == 0 }
        break if tables.count == 0
        tables.each do |table, data|
          data[:children].each do |child|
            network[child][:parents_remaining] -= 1
          end
          copy_order.push(table)
          network.delete table
        end
      end

      if network.count > 0
        raise "Couldn't find order to copy all tables. #{network} still remaining."
      end

      return copy_order    
    end

    # This method searches all tables with no parents remaining, and then decrements the parents_remaining count on their child tables
    def copy_all
      tables = get_tables_in_order
      puts "Copying.."
      puts tables.join(', ')
      
      tables.each do |table, data|
        copy_table(table)
      end
    end

    def drop_schema
      @source_conn.exec("DROP SCHEMA IF EXISTS #{@schema_name} CASCADE")
    end

    def setup_schema
      @source_conn.exec("CREATE SCHEMA IF NOT EXISTS #{@schema_name}")
    end

    # List of all table names that have samples
    def sample_tables
      @samples.map { |s| s['table'] } 
    end

    def condition_tables
      @conditions.map { |s| s['table'] } 
    end

    def get_condition_for_table(table)
      condition = @conditions.select{ |c| c['table'] == table }.first
      condition_strings = []
      if condition['last'].present?
        max_id = get_max_id_for_table(table)
        min_id = [0, max_id - condition['last']].max
        condition_strings.push("id >= #{min_id}")
      end
      if condition['condition'].present?
        condition_strings.push(condition['condition'])
      end

      condition_strings.join(' AND ').presence
    end

    def get_max_id_for_table(table)
      @source_conn.exec("SELECT MAX(id) as max FROM #{table}").to_a.first['max'].to_i
    end

    def make_sample_tables
      @samples.each do |sample|
        # Sampling a large table with ORDER BY RANDOM() is slow. So we use a faster method: generating a set of numbers in the range 1..max(id) for the table, and selecting those. 
        # To calculate the range and number of IDs we need, we need to know two things: the max_id and what proportion of the IDs between 1 and max(id) actually exist (the 'density')
        # We could work out density by dividing count(id) / max(id), but counting is slow too, so we estimate density by sampling 10000 random ids from the table and see how many actually exist.
        puts "Creating sample for #{sample['table']}"
        max_id = @source_conn.exec("SELECT MAX(id) as max FROM #{sample['table']}").to_a.first['max'].to_i
        id_density = @source_conn.exec("SELECT count(1)::float / 10000 as density FROM #{sample['table']} WHERE id IN (SELECT ROUND(random()*#{max_id})::int FROM generate_series(1,10000));").to_a.first['density'].to_f
    
        condition_string = if condition_tables.include?(sample['table'])
          " AND " + get_condition_for_table(sample['table'])
        end
        sql = "CREATE TABLE #{@schema_name}.sample_#{sample['table']} AS 
          SELECT id 
          FROM #{sample['table']}
          WHERE id in (
            select round(random() * #{max_id})::integer as id
            from generate_series(1, ROUND(1.1 * #{sample['size']} / #{id_density})::int)
            group by id
          )
            #{condition_string} 
          LIMIT #{sample['size']}"
        @source_conn.exec(sql)
        @source_conn.exec("CREATE INDEX sample_#{sample['table']}_id ON #{@schema_name}.sample_#{sample['table']} (id)")
      end
    end

    def table_exists?(table_name)
      @source_conn.exec(%Q{SELECT EXISTS (
         SELECT 1
         FROM   information_schema.tables 
         WHERE  table_schema = '#{@schema_name}'
         AND    table_name = '#{table_name}'
         )}).first['exists'] == 't'
    end

    def get_columns_for_table(table_name) 
      sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = '#{@schema_name}' AND table_name = '#{table_name}'"
      @source_conn.exec(sql).to_a.map { |r| r['column_name'] }
    end

    def anonymise
      sql = "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '#{@schema_name}'"

      cols = @source_conn.exec(sql).to_a
      @anonymise_cols.each do |anonymise_col, expression|
        cols.select{ |c| c['column_name'] == anonymise_col }.each do |col|
          sql = "UPDATE #{@schema_name}.#{col['table_name']} SET #{anonymise_col} = #{expression}"
          puts sql
          @source_conn.exec(sql)
        end
      end
    end
  end
end
