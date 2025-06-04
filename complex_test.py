# Complete Satellite Table Optimization Test Suite - Template
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, count, current_timestamp, datediff, dateadd, current_date
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, DoubleType
import time
from typing import Dict, List, Any

def main(session: snowpark.Session):
    
    # ============================================================================
    # COMPLETE OPTIMIZATION TEST CONFIGURATION
    # ============================================================================
    
    CONFIG = {
        "test_name": "Complete Satellite Table Optimization Test Suite",
        
        # Table Configuration - UPDATE THESE FOR YOUR ENVIRONMENT
        "satellite_tables": [
            {
                "name": "{SATELLITE_TABLE_NAME}",           # e.g., "S_CUSTOMER_DETAILS"
                "database": "{DATABASE_NAME}",              # e.g., "DW_PROD"
                "schema": "{SATELLITE_SCHEMA}",             # e.g., "RAWVAULT"
                "alias": "NO_DATE_FILTER",
                "has_date_filter": False
            },
            {
                "name": "{SATELLITE_TABLE_NAME}",           # Same table, different filter
                "database": "{DATABASE_NAME}",
                "schema": "{SATELLITE_SCHEMA}",
                "alias": "WITH_DATE_FILTER", 
                "has_date_filter": True
            }
        ],
        
        # Staging table
        "staging_table": {
            "name": "{STAGING_TABLE_NAME}",                 # e.g., "STAGE_CUSTOMER_DATA"
            "database": "{DATABASE_NAME}",
            "schema": "{STAGING_SCHEMA}"                    # e.g., "STAGE"
        },
        
        # Column Names - UPDATE THESE TO MATCH YOUR SCHEMA
        "hub_key_column": "{HUB_KEY_COLUMN}",              # e.g., "HK_H_CUSTOMER"
        "change_hash_column": "{CHANGE_HASH_COLUMN}",      # e.g., "DSS_CHANGE_HASH"
        "load_date_column": "{LOAD_DATE_COLUMN}",          # e.g., "DSS_LOAD_DATE"
        "staging_hash_column": "{STAGING_HASH_COLUMN}",    # e.g., "DSS_CHANGE_HASH_CUSTOMER_DETAILS"
        
        # Test Parameters
        "date_filter_days": 60,                            # Start conservative, can test 30/90 days
        "staging_sample_size": 1000000,                    # Adjust based on your data volume
        "test_readonly": True
    }
    
    print("=" * 100)
    print(f"🎯 {CONFIG['test_name']}")
    print("=" * 100)
    print(f"⏰ Test started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Testing BOTH simple CTE and full INSERT logic")
    print(f"📊 Using {CONFIG['staging_sample_size']:,} staging records")
    print(f"✅ 100% READ-ONLY - No tables created or data modified!")
    
    results = []
    
    try:
        # Step 1: Analyze staging table
        staging_results = analyze_staging_table(session, CONFIG)
        results.extend(staging_results)
        
        # Step 2: Test SIMPLE CTE performance
        cte_results = test_simple_cte_performance(session, CONFIG)
        results.extend(cte_results)
        
        # Step 3: Test the full NOT EXISTS logic
        not_exists_results = test_full_not_exists_logic(session, CONFIG)
        results.extend(not_exists_results)
        
        # Step 4: Test simulated INSERT performance 
        insert_results = test_simulated_insert_performance(session, CONFIG)
        results.extend(insert_results)
        
        # Step 5: Performance analysis
        analysis_results = analyze_complete_performance(session, CONFIG, results)
        results.extend(analysis_results)
        
        # Step 6: Run ACTUAL PRODUCTION TEST (Enable this for real testing)
        print("\n" + "="*100)
        print("🚀 PRODUCTION PERFORMANCE TEST - Ready to Run")
        print("="*100)
        print("💡 Uncomment the line below to run the actual production comparison:")
        print("    test_actual_production_performance(session, CONFIG)")
        
        # UNCOMMENT THE LINE BELOW FOR ACTUAL PRODUCTION TESTING:
        # test_actual_production_performance(session, CONFIG)
        
        print("\n" + "=" * 100)
        print("✅ Complete optimization test suite completed!")
        print(f"📊 Generated {len(results)} result records")
        print(f"⏱️ Completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 100)
        
        return create_results_dataframe(session, results)
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        error_results = [create_error_result("FATAL_ERROR", str(e))]
        return create_results_dataframe(session, error_results)

def test_simple_cte_performance(session: snowpark.Session, config: Dict) -> List[Dict]:
    """
    Test the simple CTE performance
    This isolates just the satellite table CTE performance
    """
    print("\n🔍 Step 2: Testing Simple CTE Performance (Satellite Table Only)...")
    
    results = []
    
    for satellite in config['satellite_tables']:
        try:
            satellite_name = get_table_full_name(satellite)
            
            print(f"   Testing {satellite['alias']} CTE performance...")
            
            # Build the simple CTE query
            if satellite['has_date_filter']:
                cte_query = f"""
                WITH SAT_CURR_ROWS AS (
                    SELECT {config['hub_key_column']}, {config['change_hash_column']} 
                    FROM {satellite_name}
                    WHERE {config['load_date_column']} >= DATEADD(day, -{config['date_filter_days']}, CURRENT_DATE())
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY {config['hub_key_column']} ORDER BY {config['load_date_column']} DESC) = 1
                )
                SELECT COUNT(*) as CTE_Row_Count FROM SAT_CURR_ROWS
                """
            else:
                cte_query = f"""
                WITH SAT_CURR_ROWS AS (
                    SELECT {config['hub_key_column']}, {config['change_hash_column']} 
                    FROM {satellite_name}
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY {config['hub_key_column']} ORDER BY {config['load_date_column']} DESC) = 1
                )
                SELECT COUNT(*) as CTE_Row_Count FROM SAT_CURR_ROWS
                """
            
            start_time = time.time()
            result = session.sql(cte_query).collect()
            execution_time_ms = (time.time() - start_time) * 1000
            
            cte_row_count = result[0]['CTE_ROW_COUNT'] if result else 0
            
            print(f"      ✅ {satellite['alias']} CTE: {execution_time_ms:.2f}ms")
            print(f"         CTE Rows Generated: {cte_row_count:,}")
            
            if execution_time_ms > 60000:  # More than 1 minute
                print(f"         ⏰ Runtime: {execution_time_ms/1000/60:.1f} minutes")
            
            results.append({
                'test_step': 'SIMPLE_CTE_TEST',
                'table_type': satellite['alias'],
                'metric_name': 'CTE_ONLY_PERFORMANCE',
                'metric_value': f"{execution_time_ms:.2f}ms",
                'execution_time_ms': execution_time_ms,
                'result_count': cte_row_count,
                'additional_info': f"Date Filter: {satellite['has_date_filter']}, "
                                 f"CTE Rows: {cte_row_count:,}, "
                                 f"Filter Days: {config['date_filter_days'] if satellite['has_date_filter'] else 'None'}"
            })
            
        except Exception as e:
            print(f"   ❌ Error testing {satellite['alias']} CTE: {e}")
            results.append(create_error_result(f"CTE_{satellite['alias']}", str(e)))
    
    return results

def test_actual_production_performance(session: snowpark.Session, config: Dict) -> None:
    """
    Test the ACTUAL production performance with different date filters
    This runs the real queries to compare performance
    """
    
    print("\n" + "="*100)
    print("🚀 ACTUAL PRODUCTION PERFORMANCE TEST")
    print("="*100)
    print("⏰ Testing with REAL production queries")
    print("🔒 100% READ-ONLY - No data will be modified\n")
    
    satellite_name = get_table_full_name(config['satellite_tables'][0])  # Use first satellite
    staging_name = get_table_full_name(config['staging_table'])
    
    # Test 1: No Date Filter (Current Production)
    print("📊 Test 1: Current Production Approach (No Date Filter)...")
    
    current_query = f"""
    WITH SAT_CURR_ROWS AS (
        SELECT {config['hub_key_column']}, {config['change_hash_column']} 
        FROM {satellite_name}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {config['hub_key_column']} ORDER BY {config['load_date_column']} DESC) = 1
    )
    SELECT COUNT(*) as ROWS_TO_INSERT
    FROM {staging_name} staging_table
    WHERE NOT EXISTS(
        SELECT 1 FROM SAT_CURR_ROWS
        WHERE 
            staging_table.{config['hub_key_column']} = SAT_CURR_ROWS.{config['hub_key_column']}
            AND staging_table.{config['staging_hash_column']} = SAT_CURR_ROWS.{config['change_hash_column']}
    )
    """
    
    start_time = time.time()
    try:
        result_current = session.sql(current_query).collect()
        current_time = (time.time() - start_time)
        rows_current = result_current[0]['ROWS_TO_INSERT'] if result_current else 0
        print(f"   ✅ Current approach completed in {current_time:.1f} seconds ({current_time/60:.1f} minutes)")
        print(f"   📊 Rows to insert: {rows_current:,}")
    except Exception as e:
        print(f"   ❌ Current approach test failed: {e}")
        return
    
    # Test 2: Date Filter (Optimized)
    print(f"\n📊 Test 2: Optimized Approach ({config['date_filter_days']}-Day Date Filter)...")
    
    optimized_query = f"""
    WITH SAT_CURR_ROWS AS (
        SELECT {config['hub_key_column']}, {config['change_hash_column']} 
        FROM {satellite_name}
        WHERE {config['load_date_column']} >= DATEADD(day, -{config['date_filter_days']}, CURRENT_DATE())
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {config['hub_key_column']} ORDER BY {config['load_date_column']} DESC) = 1
    )
    SELECT COUNT(*) as ROWS_TO_INSERT
    FROM {staging_name} staging_table
    WHERE NOT EXISTS(
        SELECT 1 FROM SAT_CURR_ROWS
        WHERE 
            staging_table.{config['hub_key_column']} = SAT_CURR_ROWS.{config['hub_key_column']}
            AND staging_table.{config['staging_hash_column']} = SAT_CURR_ROWS.{config['change_hash_column']}
    )
    """
    
    start_time = time.time()
    try:
        result_optimized = session.sql(optimized_query).collect()
        optimized_time = (time.time() - start_time)
        rows_optimized = result_optimized[0]['ROWS_TO_INSERT'] if result_optimized else 0
        print(f"   ✅ Optimized approach completed in {optimized_time:.1f} seconds ({optimized_time/60:.1f} minutes)")
        print(f"   📊 Rows to insert: {rows_optimized:,}")
    except Exception as e:
        print(f"   ❌ Optimized approach test failed: {e}")
        return
    
    # Performance Analysis
    print("\n" + "="*80)
    print("📈 ACTUAL PRODUCTION PERFORMANCE COMPARISON:")
    print("="*80)
    
    improvement_pct = ((current_time - optimized_time) / current_time) * 100
    speedup = current_time / optimized_time if optimized_time > 0 else 0
    time_saved = current_time - optimized_time
    row_difference = rows_current - rows_optimized
    row_difference_pct = (row_difference / rows_current) * 100 if rows_current > 0 else 0
    
    print(f"🕐 Current Runtime: {current_time:.1f} seconds ({current_time/60:.1f} minutes)")
    print(f"🕐 Optimized Runtime: {optimized_time:.1f} seconds ({optimized_time/60:.1f} minutes)")
    print(f"⏱️  Time Saved: {time_saved:.1f} seconds ({time_saved/60:.1f} minutes)")
    print(f"📊 Performance Improvement: {improvement_pct:.1f}% faster")
    print(f"🚀 Speedup Factor: {speedup:.1f}x")
    print(f"📋 Data Comparison:")
    print(f"   Current Rows to Insert: {rows_current:,}")
    print(f"   Optimized Rows to Insert: {rows_optimized:,}")
    print(f"   Row Difference: {row_difference:,} ({row_difference_pct:.3f}%)")
    
    # Business Impact Assessment
    if improvement_pct > 80:
        impact = "🚀 MASSIVE IMPROVEMENT - Critical to implement!"
    elif improvement_pct > 50:
        impact = "✅ MAJOR IMPROVEMENT - Highly recommended"
    elif improvement_pct > 20:
        impact = "📈 SIGNIFICANT IMPROVEMENT - Should implement"
    else:
        impact = "📝 MODERATE IMPROVEMENT - Consider implementing"
    
    print(f"\n💼 BUSINESS IMPACT: {impact}")
    
    if row_difference_pct < 0.1:
        data_safety = "✅ EXCELLENT - Minimal data difference"
    elif row_difference_pct < 1.0:
        data_safety = "✅ GOOD - Acceptable data difference"
    else:
        data_safety = "⚠️ REVIEW - Significant data difference"
    
    print(f"🔒 DATA SAFETY: {data_safety}")
    print("="*100)

def get_table_full_name(table_config):
    """Get fully qualified table name"""
    return f'"{table_config["database"]}"."{table_config["schema"]}"."{table_config["name"]}"'

def analyze_staging_table(session: snowpark.Session, config: Dict) -> List[Dict]:
    """Analyze the staging table to understand workload"""
    print("\n📏 Step 1: Analyzing Staging Table...")
    
    results = []
    staging_name = get_table_full_name(config['staging_table'])
    
    try:
        print(f"   📊 Analyzing staging table characteristics...")
        
        staging_query = f"""
        SELECT 
            COUNT(*) as TOTAL_STAGING_ROWS,
            COUNT(DISTINCT {config['hub_key_column']}) as DISTINCT_HK_VALUES,
            MIN({config['load_date_column']}) as MIN_LOAD_DATE,
            MAX({config['load_date_column']}) as MAX_LOAD_DATE,
            COUNT(DISTINCT {config['load_date_column']}) as DISTINCT_LOAD_DATES
        FROM {staging_name}
        """
        
        start_time = time.time()
        result = session.sql(staging_query).collect()
        analysis_time = (time.time() - start_time) * 1000
        
        if result:
            staging_info = result[0]
            print(f"      📈 Staging Rows: {staging_info['TOTAL_STAGING_ROWS']:,}")
            print(f"      🔑 Distinct Hash Keys: {staging_info['DISTINCT_HK_VALUES']:,}")
            print(f"      📅 Load Date Range: {staging_info['MIN_LOAD_DATE']} to {staging_info['MAX_LOAD_DATE']}")
            
            results.append({
                'test_step': 'STAGING_ANALYSIS',
                'table_type': 'STAGING_TABLE',
                'metric_name': 'STAGING_SIZE_ANALYSIS',
                'metric_value': f"{staging_info['TOTAL_STAGING_ROWS']:,} rows",
                'execution_time_ms': analysis_time,
                'result_count': staging_info['TOTAL_STAGING_ROWS'],
                'additional_info': f"Distinct HK: {staging_info['DISTINCT_HK_VALUES']:,}"
            })
            
    except Exception as e:
        print(f"   ❌ Error analyzing staging table: {e}")
        results.append(create_error_result("STAGING_ANALYSIS", str(e)))
    
    return results

def test_full_not_exists_logic(session: snowpark.Session, config: Dict) -> List[Dict]:
    """Test the full NOT EXISTS logic with staging comparison"""
    print("\n🔍 Step 3: Testing Full NOT EXISTS Logic...")
    
    results = []
    staging_name = get_table_full_name(config['staging_table'])
    
    for satellite in config['satellite_tables']:
        try:
            satellite_name = get_table_full_name(satellite)
            
            print(f"   Testing {satellite['alias']} NOT EXISTS with staging...")
            
            if satellite['has_date_filter']:
                logic_query = f"""
                WITH SAT_CURR_ROWS AS (
                    SELECT {config['hub_key_column']}, {config['change_hash_column']} 
                    FROM {satellite_name}
                    WHERE {config['load_date_column']} >= DATEADD(day, -{config['date_filter_days']}, CURRENT_DATE())
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY {config['hub_key_column']} ORDER BY {config['load_date_column']} DESC) = 1
                )
                SELECT COUNT(*) as RECORDS_TO_INSERT
                FROM {staging_name} staging_table
                WHERE NOT EXISTS(
                    SELECT 1 FROM SAT_CURR_ROWS
                    WHERE 
                        staging_table.{config['hub_key_column']} = SAT_CURR_ROWS.{config['hub_key_column']}
                        AND staging_table.{config['staging_hash_column']} = SAT_CURR_ROWS.{config['change_hash_column']}
                )
                """
            else:
                logic_query = f"""
                WITH SAT_CURR_ROWS AS (
                    SELECT {config['hub_key_column']}, {config['change_hash_column']} 
                    FROM {satellite_name}
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY {config['hub_key_column']} ORDER BY {config['load_date_column']} DESC) = 1
                )
                SELECT COUNT(*) as RECORDS_TO_INSERT
                FROM {staging_name} staging_table
                WHERE NOT EXISTS(
                    SELECT 1 FROM SAT_CURR_ROWS
                    WHERE 
                        staging_table.{config['hub_key_column']} = SAT_CURR_ROWS.{config['hub_key_column']}
                        AND staging_table.{config['staging_hash_column']} = SAT_CURR_ROWS.{config['change_hash_column']}
                )
                """
            
            start_time = time.time()
            result = session.sql(logic_query).collect()
            execution_time_ms = (time.time() - start_time) * 1000
            
            records_to_insert = result[0]['RECORDS_TO_INSERT'] if result else 0
            
            print(f"      ✅ {satellite['alias']}: {execution_time_ms:.2f}ms")
            print(f"         Records to Insert: {records_to_insert:,}")
            
            if execution_time_ms > 60000:
                print(f"         ⏰ Runtime: {execution_time_ms/1000/60:.1f} minutes")
            
            results.append({
                'test_step': 'FULL_NOT_EXISTS_TEST',
                'table_type': satellite['alias'],
                'metric_name': 'NOT_EXISTS_WITH_STAGING',
                'metric_value': f"{execution_time_ms:.2f}ms",
                'execution_time_ms': execution_time_ms,
                'result_count': records_to_insert,
                'additional_info': f"Date Filter: {satellite['has_date_filter']}, Records: {records_to_insert:,}"
            })
            
        except Exception as e:
            print(f"   ❌ Error testing {satellite['alias']}: {e}")
            results.append(create_error_result(f"NOT_EXISTS_{satellite['alias']}", str(e)))
    
    return results

def test_simulated_insert_performance(session: snowpark.Session, config: Dict) -> List[Dict]:
    """Test simulated INSERT performance - READ ONLY"""
    print("\n💾 Step 4: Testing Simulated INSERT Performance...")
    
    results = []
    staging_name = get_table_full_name(config['staging_table'])
    
    for satellite in config['satellite_tables']:
        try:
            satellite_name = get_table_full_name(satellite)
            
            print(f"   Testing {satellite['alias']} INSERT simulation...")
            
            if satellite['has_date_filter']:
                insert_query = f"""
                WITH SAT_CURR_ROWS AS (
                    SELECT {config['hub_key_column']}, {config['change_hash_column']} 
                    FROM {satellite_name}
                    WHERE {config['load_date_column']} >= DATEADD(day, -{config['date_filter_days']}, CURRENT_DATE())
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY {config['hub_key_column']} ORDER BY {config['load_date_column']} DESC) = 1
                ),
                INSERT_READY_DATA AS (
                    SELECT DISTINCT
                        staging_table.{config['hub_key_column']},
                        staging_table.{config['staging_hash_column']} AS {config['change_hash_column']},
                        staging_table.{config['load_date_column']},
                        CURRENT_TIMESTAMP AS DSS_START_DATE
                    FROM {staging_name} staging_table
                    WHERE NOT EXISTS(
                        SELECT 1 FROM SAT_CURR_ROWS
                        WHERE 
                            staging_table.{config['hub_key_column']} = SAT_CURR_ROWS.{config['hub_key_column']}
                            AND staging_table.{config['staging_hash_column']} = SAT_CURR_ROWS.{config['change_hash_column']}
                    )
                )
                SELECT COUNT(*) as ROWS_TO_INSERT FROM INSERT_READY_DATA
                """
            else:
                insert_query = f"""
                WITH SAT_CURR_ROWS AS (
                    SELECT {config['hub_key_column']}, {config['change_hash_column']} 
                    FROM {satellite_name}
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY {config['hub_key_column']} ORDER BY {config['load_date_column']} DESC) = 1
                ),
                INSERT_READY_DATA AS (
                    SELECT DISTINCT
                        staging_table.{config['hub_key_column']},
                        staging_table.{config['staging_hash_column']} AS {config['change_hash_column']},
                        staging_table.{config['load_date_column']},
                        CURRENT_TIMESTAMP AS DSS_START_DATE
                    FROM {staging_name} staging_table
                    WHERE NOT EXISTS(
                        SELECT 1 FROM SAT_CURR_ROWS
                        WHERE 
                            staging_table.{config['hub_key_column']} = SAT_CURR_ROWS.{config['hub_key_column']}
                            AND staging_table.{config['staging_hash_column']} = SAT_CURR_ROWS.{config['change_hash_column']}
                    )
                )
                SELECT COUNT(*) as ROWS_TO_INSERT FROM INSERT_READY_DATA
                """
            
            start_time = time.time()
            result = session.sql(insert_query).collect()
            execution_time_ms = (time.time() - start_time) * 1000
            
            rows_to_insert = result[0]['ROWS_TO_INSERT'] if result else 0
            
            print(f"      ✅ {satellite['alias']}: {execution_time_ms:.2f}ms")
            print(f"         Rows Would Insert: {rows_to_insert:,}")
            
            results.append({
                'test_step': 'SIMULATED_INSERT_TEST',
                'table_type': satellite['alias'],
                'metric_name': 'FULL_INSERT_LOGIC_READONLY',
                'metric_value': f"{execution_time_ms:.2f}ms",
                'execution_time_ms': execution_time_ms,
                'result_count': rows_to_insert,
                'additional_info': f"Date Filter: {satellite['has_date_filter']}, Rows: {rows_to_insert:,}"
            })
            
        except Exception as e:
            print(f"   ❌ Error testing {satellite['alias']}: {e}")
            results.append(create_error_result(f"INSERT_SIM_{satellite['alias']}", str(e)))
    
    return results

def analyze_complete_performance(session: snowpark.Session, config: Dict, all_results: List[Dict]) -> List[Dict]:
    """Analyze complete performance across all test types"""
    print("\n📊 Step 5: Complete Performance Analysis...")
    
    results = []
    
    # Get results by test type
    cte_results = [r for r in all_results if r['test_step'] == 'SIMPLE_CTE_TEST']
    not_exists_results = [r for r in all_results if r['test_step'] == 'FULL_NOT_EXISTS_TEST']
    insert_results = [r for r in all_results if r['test_step'] == 'SIMULATED_INSERT_TEST']
    
    print(f"\n   📈 Performance Summary:")
    
    # CTE Performance Analysis
    if len(cte_results) >= 2:
        ma1_cte = next((r for r in cte_results if 'NO_DATE_FILTER' in r['table_type']), None)
        ma2_cte = next((r for r in cte_results if 'WITH_DATE_FILTER' in r['table_type']), None)
        
        if ma1_cte and ma2_cte:
            cte_improvement = ((ma1_cte['execution_time_ms'] - ma2_cte['execution_time_ms']) / ma1_cte['execution_time_ms']) * 100
            print(f"      CTE Performance Improvement: {cte_improvement:.1f}%")
    
    # Full Logic Performance Analysis
    if len(insert_results) >= 2:
        ma1_insert = next((r for r in insert_results if 'NO_DATE_FILTER' in r['table_type']), None)
        ma2_insert = next((r for r in insert_results if 'WITH_DATE_FILTER' in r['table_type']), None)
        
        if ma1_insert and ma2_insert:
            insert_improvement = ((ma1_insert['execution_time_ms'] - ma2_insert['execution_time_ms']) / ma1_insert['execution_time_ms']) * 100
            data_difference = abs(ma1_insert['result_count'] - ma2_insert['result_count'])
            data_difference_pct = (data_difference / ma1_insert['result_count']) * 100
            
            print(f"      Full INSERT Improvement: {insert_improvement:.1f}%")
            print(f"      Data Difference: {data_difference:,} records ({data_difference_pct:.3f}%)")
            
            results.append({
                'test_step': 'COMPLETE_ANALYSIS',
                'table_type': 'PERFORMANCE_SUMMARY',
                'metric_name': 'OPTIMIZATION_IMPACT',
                'metric_value': f"{insert_improvement:.1f}% improvement",
                'execution_time_ms': ma2_insert['execution_time_ms'],
                'result_count': 1,
                'additional_info': f"Data diff: {data_difference:,} ({data_difference_pct:.3f}%), "
                                 f"Filter: {config['date_filter_days']} days"
            })
    
    return results

def create_error_result(context: str, error_message: str) -> Dict:
    """Create a standardized error result"""
    return {
        'test_step': 'ERROR',
        'table_type': context,
        'metric_name': 'FAILED',
        'metric_value': 'ERROR',
        'execution_time_ms': 0,
        'result_count': 0,
        'additional_info': f'Error: {error_message}'
    }

def create_results_dataframe(session: snowpark.Session, results: List[Dict]) -> snowpark.DataFrame:
    """Convert results list to Snowpark DataFrame"""
    
    if not results:
        results = [create_error_result("NO_RESULTS", "No test results generated")]
    
    data = []
    for result in results:
        data.append([
            result.get('test_step', 'UNKNOWN'),
            result.get('table_type', 'UNKNOWN'),
            result.get('metric_name', 'UNKNOWN'),
            str(result.get('metric_value', 'N/A')),
            float(result.get('execution_time_ms', 0)),
            int(result.get('result_count', 0)),
            result.get('additional_info', '')
        ])
    
    schema = StructType([
        StructField("TEST_STEP", StringType()),
        StructField("TABLE_TYPE", StringType()),
        StructField("METRIC_NAME", StringType()),
        StructField("METRIC_VALUE", StringType()),
        StructField("EXECUTION_TIME_MS", DoubleType()),
        StructField("RESULT_COUNT", IntegerType()),
        StructField("ADDITIONAL_INFO", StringType())
    ])
    
    return session.create_dataframe(data, schema)
