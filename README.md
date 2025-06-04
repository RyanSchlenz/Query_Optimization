# Query_Optimization Test Suite

## 🎯 Overview

A comprehensive Python test suite for validating and measuring performance optimizations in Data Vault satellite table processing. This suite provides two complementary approaches to test the impact of adding date filters to satellite table CTEs, potentially reducing query execution time from hours to minutes.

## 📊 Project Components

### **Two Testing Approaches**

1. **🚀 Simple Test (`simple_test.py`)** 
   - **Runtime**: 5-15 minutes
   - **Use Case**: Quick validation and initial assessment
   - **Sampling**: Uses 50K staging records for testing
   - **Best For**: Daily development, CI/CD integration

2. **🔬 Comprehensive Test (`complex_test.py`)**
   - **Runtime**: 30-120 minutes 
   - **Use Case**: Deep analysis and production validation
   - **Comprehensive**: Full staging data volume testing
   - **Best For**: Final validation, detailed analysis

## 🏗️ Architecture

### **The Optimization Strategy**

**Current Problem:**

-- SLOW: Processes entire satellite history
WITH SAT_CURRENT AS (
    SELECT hub_key, change_hash
    FROM satellite_table
    QUALIFY ROW_NUMBER() OVER (...) = 1
)


**Proposed Solution:**
-- FAST: Only processes recent data
WITH SAT_CURRENT AS (
    SELECT hub_key, change_hash
    FROM satellite_table
    WHERE load_date >= DATEADD(day, -60, CURRENT_DATE())
    QUALIFY ROW_NUMBER() OVER (...) = 1
)


### **Testing Framework**

Both scripts validate:
- **Performance Impact**: How much faster the optimization runs
- **Data Quality**: How many records differ between approaches
- **Business Risk**: Whether the optimization is safe to implement
- **Production Scaling**: Extrapolated impact on full datasets

## 🚀 Quick Start

### Prerequisites

pip install snowflake-snowpark-python


### Environment Setup

1. **Copy and customize configuration:**

# Database Configuration
DATABASE_NAME = "your_database"
SATELLITE_SCHEMA = "your_satellite_schema"  # e.g., "RAWVAULT"
STAGING_SCHEMA = "your_staging_schema"      # e.g., "STAGE"

# Table Names
SATELLITE_TABLE_NAME = "your_satellite_table"  # e.g., "S_CUSTOMER_DETAILS"
STAGING_TABLE_NAME = "your_staging_table"      # e.g., "STAGE_CUSTOMER_DATA"

# Column Names (Update to match your schema)
HUB_KEY_COLUMN = "your_hub_key"            # e.g., "HK_H_CUSTOMER"
CHANGE_HASH_COLUMN = "your_change_hash"    # e.g., "DSS_CHANGE_HASH"
LOAD_DATE_COLUMN = "your_load_date"        # e.g., "DSS_LOAD_DATE"
STAGING_HASH_COLUMN = "your_staging_hash"  # e.g., "DSS_CHANGE_HASH_CUSTOMER"

2. **Initialize Snowpark session:**

import snowflake.snowpark as snowpark

session = snowpark.Session.builder.configs({
    "account": "your_account",
    "user": "your_username",
    "password": "your_password",
    "role": "your_role",
    "warehouse": "your_warehouse",
    "database": "your_database"
}).create()

### Choose Your Testing Approach

#### **Option 1: Quick Validation (Recommended First)**

from simple_satellite_test import main, quick_validation

# Ultra-fast validation (2 minutes)
quick_validation(session)

# Full simple test (5-15 minutes)
results = main(session)
print(f"Performance improvement: {results['staging_improvement']:.1f}%")

#### **Option 2: Comprehensive Analysis**

from complete_satellite_test import main

# Full comprehensive test (30-120 minutes)
results_df = main(session)
results_df.show()


## 📋 Detailed Usage

### **Simple Test Script**

**Perfect for:**
- Initial feasibility assessment
- Development validation
- CI/CD integration
- Quick performance checks

**Features:**
- Smart sampling (50K records)
- Production extrapolation
- Clear business recommendations
- 15-minute target runtime

**Sample Output:**

🎯 FINAL RESULTS & RECOMMENDATIONS
═══════════════════════════════════
📊 Performance Improvements:
   CTE Performance: 75.2% faster
   Full Logic Performance: 68.9% faster
   Estimated Production Time Savings: 22.4 minutes

🔍 Data Impact Analysis:
   CTE Row Difference: 2.3%
   Insert Record Difference: 1.8%

💼 Business Recommendation:
   🚀 STRONGLY RECOMMEND - High performance gain, low data risk


### **Comprehensive Test Script**

**Perfect for:**
- Final production validation
- Detailed performance analysis
- Executive reporting
- Risk assessment

**Features:**
- 6-phase testing methodology
- Full staging data testing
- Detailed error handling
- Comprehensive result tracking

**Testing Phases:**
1. **📏 Staging Analysis**: Data volume analysis
2. **🔍 CTE Performance**: Isolated satellite testing
3. **🔗 NOT EXISTS Logic**: Integration testing
4. **💾 INSERT Simulation**: Complete workflow
5. **📈 Performance Analysis**: Comparative metrics
6. **🚀 Production Validation**: Real-world testing

## 📊 Expected Results

### **Typical Performance Improvements**

| Scenario | Time Savings | Data Impact | Business Value |
|----------|--------------|-------------|----------------|
| **Light Workload** | 30-50% | <1% difference | Moderate gains |
| **Medium Workload** | 50-80% | 1-3% difference | Significant gains |
| **Heavy Workload** | 80-95% | 2-5% difference | Massive gains |

### **Success Criteria**

✅ **Implement Optimization** if:
- Performance improvement >50%
- Data difference <5%
- Clear business benefit

⚠️ **Review Carefully** if:
- Performance improvement 20-50%
- Data difference 5-10%
- Complex business requirements

❌ **Do Not Implement** if:
- Performance improvement <20%
- Data difference >10%
- High business risk

## 🛠️ Configuration Guide

### **Required Table Structure**

Your satellite table should have:

CREATE TABLE satellite_table (
    {HUB_KEY_COLUMN} VARCHAR,        -- Hub key for partitioning
    {CHANGE_HASH_COLUMN} VARCHAR,    -- Change hash for deduplication
    {LOAD_DATE_COLUMN} TIMESTAMP,    -- Load date for filtering
    -- ... other satellite columns
);


Your staging table should have:

CREATE TABLE staging_table (
    {HUB_KEY_COLUMN} VARCHAR,         -- Matching hub key
    {STAGING_HASH_COLUMN} VARCHAR,    -- Staging change hash
    {LOAD_DATE_COLUMN} TIMESTAMP,     -- Load date
    -- ... other staging columns
);


### **Column Mapping Examples**

| Component | Example 1 | Example 2 | Example 3 |
|-----------|-----------|-----------|-----------|
| **Hub Key** | `HK_H_CUSTOMER` | `HK_H_INVOICE` | `HK_H_PRODUCT` |
| **Change Hash** | `DSS_CHANGE_HASH` | `DSS_CHANGE_HASH` | `DSS_CHANGE_HASH` |
| **Load Date** | `DSS_LOAD_DATE` | `DSS_LOAD_DATE` | `DSS_LOAD_DATE` |
| **Staging Hash** | `DSS_CHANGE_HASH_CUSTOMER` | `DSS_CHANGE_HASH_INVOICE` | `DSS_CHANGE_HASH_PRODUCT` |

### **Environment Variables Template**

Create a `.env` file:

# Snowflake Connection
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse

# Database Configuration
DATABASE_NAME=your_database
SATELLITE_SCHEMA=your_satellite_schema
STAGING_SCHEMA=your_staging_schema

# Table Names
SATELLITE_TABLE_NAME=your_satellite_table
STAGING_TABLE_NAME=your_staging_table

# Column Names
HUB_KEY_COLUMN=your_hub_key_column
CHANGE_HASH_COLUMN=your_change_hash_column
LOAD_DATE_COLUMN=your_load_date_column
STAGING_HASH_COLUMN=your_staging_hash_column


## ⚠️ Known Issues & Limitations

### **Simple Test Limitations**
- **Sampling Bias**: 50K sample may not represent full data
- **Extrapolation Error**: Linear scaling assumptions
- **Limited Validation**: Reduced error checking

### **Comprehensive Test Limitations**
- **Long Runtime**: Can take 1-2 hours for large datasets
- **Resource Intensive**: High compute warehouse recommended
- **Complex Debugging**: Multi-phase failures difficult to trace

### **Common Issues**

#### **1. Column Name Mismatches**

Error: Column 'HK_H_CUSTOMER' not found

**Solution**: Update column names in script configuration

#### **2. Permission Errors**

Error: Insufficient privileges to access table

**Solution**: Ensure role has SELECT permissions on all tables

#### **3. Memory Issues**

Error: Query exceeded memory limits

**Solution**: Use larger warehouse or reduce sample size

#### **4. Session Timeouts**

Error: Session expired during execution

**Solution**: Increase session timeout or run during low-usage periods

## 📈 Performance Optimization

### **For Development/Testing**

# Quick configuration for faster testing
CONFIG = {
    "date_filter_days": 30,        # Shorter filter period
    "staging_sample_size": 10000,  # Smaller sample
    "test_readonly": True
}


### **For Production Validation**

# Production configuration for thorough testing
CONFIG = {
    "date_filter_days": 60,         # Conservative filter
    "staging_sample_size": 1000000, # Large sample
    "test_readonly": True
}


### **Warehouse Sizing Recommendations**

| Test Type | Recommended Size | Expected Cost | Duration |
|-----------|------------------|---------------|----------|
| **Simple Test** | Small/Medium | Low | 5-15 min |
| **Comprehensive** | Large/X-Large | Medium | 30-120 min |
| **Production** | X-Large | High | Variable |

## 🔒 Security & Safety

### **Data Protection**

✅ **Both scripts are 100% READ-ONLY**
- No INSERT/UPDATE/DELETE operations
- No table creation or modification
- Safe for production environment

✅ **Audit Trail**
- All operations logged in Snowflake query history
- Execution times and row counts tracked
- Error handling with detailed logging

### **Required Permissions**

-- Minimum permissions required
GRANT USAGE ON DATABASE {DATABASE_NAME} TO ROLE {YOUR_ROLE};
GRANT USAGE ON SCHEMA {DATABASE_NAME}.{SATELLITE_SCHEMA} TO ROLE {YOUR_ROLE};
GRANT USAGE ON SCHEMA {DATABASE_NAME}.{STAGING_SCHEMA} TO ROLE {YOUR_ROLE};
GRANT SELECT ON ALL TABLES IN SCHEMA {DATABASE_NAME}.{SATELLITE_SCHEMA} TO ROLE {YOUR_ROLE};
GRANT SELECT ON ALL TABLES IN SCHEMA {DATABASE_NAME}.{STAGING_SCHEMA} TO ROLE {YOUR_ROLE};

## 🤝 Integration Patterns

### **CI/CD Integration**

# automated_test.py
def regression_test():
    """Quick regression test for CI/CD"""
    from simple_satellite_test import quick_validation
    
    session = create_session()
    quick_validation(session)
    
    # Assert performance within acceptable ranges
    assert results['staging_improvement'] > 20  # Minimum improvement
    return "PASS"


### **Monitoring Integration**

# monitoring.py
def performance_monitoring():
    """Regular performance monitoring"""
    from simple_satellite_test import main
    
    results = main(session)
    
    # Send metrics to monitoring system
    send_metric("satellite_optimization_improvement", results['staging_improvement'])
    send_metric("satellite_data_difference", results['staging_insert_difference'])

### **Reporting Integration**

# reporting.py
def generate_executive_report():
    """Generate executive performance report"""
    from complete_satellite_test import main
    
    results_df = main(session)
    
    # Convert to business report
    report = create_business_report(results_df)
    send_email_report(report, recipients=["executives@company.com"])


## 📊 Business Value Analysis

### **ROI Calculator**

def calculate_roi(results):
    """Calculate business ROI from optimization"""
    
    time_saved_hours = results['estimated_time_saved'] / 3600
    daily_runs = 5  # Adjust for your environment
    annual_hours_saved = time_saved_hours * daily_runs * 365
    
    # Cost savings (adjust hourly rates for your environment)
    engineer_hourly_rate = 100
    compute_cost_per_hour = 50
    
    annual_savings = annual_hours_saved * (engineer_hourly_rate + compute_cost_per_hour)
    
    return {
        "annual_hours_saved": annual_hours_saved,
        "annual_cost_savings": annual_savings,
        "improvement_percentage": results['staging_improvement']
    }

### **Success Metrics**

Track these KPIs post-implementation:
- **Query execution time reduction**
- **Daily processing completion time**
- **Compute cost reduction**
- **Team productivity increase**
- **Data quality maintenance**

## 📝 Best Practices

### **Testing Strategy**

1. **Start Simple**: Always run simple test first
2. **Validate Assumptions**: Check data distribution patterns
3. **Monitor Closely**: Watch first week of production carefully
4. **Document Everything**: Keep detailed records of changes
5. **Plan Rollback**: Have immediate rollback strategy ready

### **Implementation Strategy**

1. **Week 1**: Development environment validation
2. **Week 2**: Staging environment testing
3. **Week 3**: Limited production testing
4. **Week 4**: Full production rollout

### **Monitoring Strategy**

- **Real-time**: Query execution time alerts
- **Daily**: Row count difference validation
- **Weekly**: Performance trend analysis
- **Monthly**: Business impact assessment

## 📞 Support & Troubleshooting

### **Getting Help**

1. **Documentation**: Check this README and inline comments
2. **Error Logs**: Review Snowflake query history for details
3. **GitHub Issues**: Report bugs and feature requests
4. **Community**: Engage with Data Vault community forums

### **Common Solutions**

**Slow Performance:**
- Use larger Snowflake warehouse
- Reduce sample sizes for testing
- Run during off-peak hours

**Data Mismatches:**
- Verify column names match exactly
- Check data types compatibility
- Validate table relationships

**Permission Issues:**
- Confirm role has necessary grants
- Test with simple SELECT statements first
- Contact DBA for permission troubleshooting

## 📄 License

MIT License - See LICENSE file for details

---

## 🎯 Quick Reference

### **Essential Commands**


# Quick start
python simple_satellite_test.py

# Comprehensive analysis
python complete_satellite_test.py

# Ultra-quick validation
python -c "from simple_satellite_test import quick_validation; quick_validation(session)"


### **Key Decision Points**

- **Use Simple Test** for: Development, CI/CD, quick decisions
- **Use Comprehensive Test** for: Production validation, executive reporting
- **Implement Optimization** if: >50% improvement, <5% data difference
- **Consider Date Filter**: Start with 60 days, adjust based on results

### **Expected Outcomes**

- **Time Savings**: 20-80% performance improvement typical
- **Data Risk**: Usually <5% record difference
- **Business Value**: Significant productivity and cost savings
- **Implementation**: Low risk, high reward optimization
