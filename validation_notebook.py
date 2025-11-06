# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2: Email Extraction Validation
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook validates the data extracted by Agent Bricks from customer emails.
# MAGIC
# MAGIC ### What We'll Do
# MAGIC 1. Load extracted data from Agent Bricks
# MAGIC 2. Apply Python validation functions
# MAGIC 3. Create a review queue based on validation results
# MAGIC 4. Route emails to appropriate queues (auto-approve vs. needs review)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import re
import json
from pyspark.sql.functions import udf, col, struct, lit, when, size, array, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, IntegerType

# Configuration
CATALOG = "main"
SCHEMA = "scotts_genai_workshop"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"✅ Using: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Agent Bricks Extraction Results
# MAGIC
# MAGIC First, let's load the data that was extracted by Agent Bricks.

# COMMAND ----------

# Load extracted data from Agent Bricks
# This assumes you've already run the extraction and stored results
# Note: response.result is a VARIANT type, so we use : accessor syntax
extracted_df = spark.sql("""
  WITH query_results AS (
    SELECT 
      email_id,
      sender,
      `body` AS email_body,
      ai_query(
        'kie-4cb95ce7-endpoint',
        `body`,
        failOnError => false
      ) AS response
    FROM synthetic_emails
    LIMIT 50
  )
  SELECT
    email_id,
    sender,
    email_body,
    response.result:sap_id::string AS sap_id,
    response.result:contact_name::string AS contact_name,
    response.result:contact_email::string AS contact_email,
    response.result:contact_phone::string AS contact_phone,
    response.errorMessage AS extraction_error
  FROM query_results
""")

# Cache for reuse
extracted_df.cache()
display(extracted_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Define Validation Functions
# MAGIC
# MAGIC Simple Python functions to validate extracted fields.

# COMMAND ----------

def validate_email(email):
    """Validate email format"""
    if not email or email.strip() == "":
        return {"is_valid": False, "error": "Email is empty"}
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    if re.match(pattern, email.strip()):
        return {"is_valid": True, "error": None}
    else:
        return {"is_valid": False, "error": f"Invalid email format: {email}"}

def validate_phone(phone):
    """Validate and normalize phone number"""
    if not phone or phone.strip() == "":
        return {"is_valid": False, "normalized": None, "error": "Phone is empty"}
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    
    # Handle +1 prefix
    if digits.startswith('1') and len(digits) == 11:
        digits = digits[1:]
    
    # Check if we have exactly 10 digits
    if len(digits) != 10:
        return {"is_valid": False, "normalized": None, "error": f"Phone must have 10 digits, got {len(digits)}"}
    
    # Validate area code
    if digits[0] in ['0', '1']:
        return {"is_valid": False, "normalized": None, "error": "Area code cannot start with 0 or 1"}
    
    # Format as (XXX) XXX-XXXX
    normalized = f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    
    return {"is_valid": True, "normalized": normalized, "error": None}

def validate_sap_id(sap_id):
    """Validate SAP ID format (SAPXXXXXX)"""
    if not sap_id or sap_id.strip() == "":
        return {"is_valid": False, "normalized": None, "error": "SAP ID is empty"}
    
    # Remove whitespace and convert to uppercase
    cleaned = sap_id.strip().upper()
    
    # Pattern: SAP followed by 6 digits
    pattern = r'^SAP\d{6}$'
    
    if re.match(pattern, cleaned):
        return {"is_valid": True, "normalized": cleaned, "error": None}
    else:
        return {"is_valid": False, "normalized": None, "error": f"Invalid SAP ID format (expected SAPXXXXXX): {sap_id}"}

def validate_name(name):
    """Validate contact name"""
    if not name or name.strip() == "":
        return {"is_valid": False, "normalized": None, "error": "Name is empty"}
    
    cleaned = name.strip()
    
    # Check minimum length
    if len(cleaned) < 2:
        return {"is_valid": False, "normalized": None, "error": "Name too short"}
    
    # Check for at least two parts (first and last name)
    parts = cleaned.split()
    if len(parts) < 2:
        return {"is_valid": False, "normalized": None, "error": "Name must include first and last name"}
    
    # Normalize to Title Case
    normalized = cleaned.title()
    
    return {"is_valid": True, "normalized": normalized, "error": None}

# Test the functions
print("Testing validation functions:")
print("Email:", validate_email("john.smith@example.com"))
print("Phone:", validate_phone("555-123-4567"))
print("SAP ID:", validate_sap_id("SAP123456"))
print("Name:", validate_name("john smith"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create UDFs for Spark

# COMMAND ----------

# Define return types
validation_result_type = StructType([
    StructField("is_valid", BooleanType()),
    StructField("normalized", StringType()),
    StructField("error", StringType())
])

email_validation_type = StructType([
    StructField("is_valid", BooleanType()),
    StructField("error", StringType())
])

# Register UDFs
validate_email_udf = udf(validate_email, email_validation_type)
validate_phone_udf = udf(validate_phone, validation_result_type)
validate_sap_id_udf = udf(validate_sap_id, validation_result_type)
validate_name_udf = udf(validate_name, validation_result_type)

print("✅ UDFs registered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Apply Validations

# COMMAND ----------

# Apply all validations
validated_df = extracted_df.select(
    col("email_id"),
    col("sender"),
    col("email_body"),
    col("sap_id"),
    col("contact_name"),
    col("contact_email"),
    col("contact_phone"),
    col("extraction_error"),
    
    # Validation results
    validate_sap_id_udf(col("sap_id")).alias("sap_validation"),
    validate_name_udf(col("contact_name")).alias("name_validation"),
    validate_email_udf(col("contact_email")).alias("email_validation"),
    validate_phone_udf(col("contact_phone")).alias("phone_validation")
)

# Extract validation details
validated_df = validated_df.select(
    "*",
    
    # Field validity flags
    col("sap_validation.is_valid").alias("sap_id_valid"),
    col("name_validation.is_valid").alias("name_valid"),
    col("email_validation.is_valid").alias("email_valid"),
    col("phone_validation.is_valid").alias("phone_valid"),
    
    # Normalized values
    col("sap_validation.normalized").alias("normalized_sap_id"),
    col("name_validation.normalized").alias("normalized_name"),
    col("phone_validation.normalized").alias("normalized_phone"),
    
    # Collect all errors
    array(
        col("sap_validation.error"),
        col("name_validation.error"),
        col("email_validation.error"),
        col("phone_validation.error")
    ).alias("error_list")
)

display(validated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Check SAP ID Existence
# MAGIC
# MAGIC Check if SAP IDs exist in our mock customer database.

# COMMAND ----------

# Drop and recreate the table to ensure schema matches
spark.sql("DROP TABLE IF EXISTS sap_customers")

spark.sql("""
  CREATE TABLE sap_customers (
    sap_id STRING,
    account_status STRING,
    last_updated TIMESTAMP
  )
""")

# Populate with valid SAP IDs from our synthetic emails
spark.sql("""
  INSERT INTO sap_customers
  SELECT DISTINCT
    UPPER(sap_id) AS sap_id,
    'ACTIVE' AS account_status,
    current_timestamp() AS last_updated
  FROM synthetic_emails
  WHERE sap_id IS NOT NULL
""")

print(f"✅ Mock SAP database has {spark.table('sap_customers').count()} active accounts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### MCP Integration (Optional)
# MAGIC Use MCP SAP server for sap_exists check instead of table lookup

# COMMAND ----------

# Configuration for MCP
import os
USE_MCP_FOR_SAP = os.getenv("USE_MCP_FOR_SAP", "false").lower() == "true"
MCP_SAP_URL = os.getenv("MCP_SAP_URL", "http://localhost:8000")

print(f"MCP Integration: {'✅ Enabled' if USE_MCP_FOR_SAP else '❌ Disabled'}")
if USE_MCP_FOR_SAP:
    print(f"MCP URL: {MCP_SAP_URL}")

# COMMAND ----------

def check_sap_via_mcp(sap_ids_list):
    """
    Check SAP IDs via MCP bulk endpoint using databricks-mcp client
    Returns dict mapping sap_id -> exists (bool)
    """
    if not USE_MCP_FOR_SAP:
        return {}
    
    try:
        import json
        from databricks_mcp import DatabricksMCPClient
        from databricks.sdk import WorkspaceClient
        
        # Normalize server URL
        server_url = MCP_SAP_URL if MCP_SAP_URL.endswith('/') else f"{MCP_SAP_URL}/"
        if not server_url.endswith('/mcp/'):
            server_url = f"{server_url}mcp/" if not server_url.endswith('mcp/') else server_url
        
        # Check if local or remote
        is_local = 'localhost' in server_url or '127.0.0.1' in server_url
        
        if is_local:
            # For local testing
            workspace_client = WorkspaceClient(
                host="https://localhost",
                token="local-dev-token"
            )
        else:
            # For Databricks Apps
            workspace_client = WorkspaceClient()
        
        mcp_client = DatabricksMCPClient(
            server_url=server_url,
            workspace_client=workspace_client
        )
        
        # Process in chunks to avoid timeout
        chunk_size = 200
        results_map = {}
        
        for i in range(0, len(sap_ids_list), chunk_size):
            chunk = sap_ids_list[i:i+chunk_size]
            
            # Call MCP tool
            result_obj = mcp_client.call_tool(
                "sap_bulk_check_exists",
                arguments={"sap_ids": chunk}
            )
            
            # Extract JSON from CallToolResult
            if hasattr(result_obj, 'content') and result_obj.content:
                for item in result_obj.content:
                    if hasattr(item, 'text'):
                        result = json.loads(item.text)
                        for res_item in result.get("results", []):
                            results_map[res_item["sap_id"]] = res_item["exists"]
                        break
            else:
                print(f"⚠️ MCP no response for chunk {i//chunk_size}")
                for sap_id in chunk:
                    results_map[sap_id] = False
        
        print(f"✅ MCP checked {len(results_map)} SAP IDs: {sum(results_map.values())} found")
        return results_map
    
    except Exception as e:
        print(f"❌ MCP check failed: {str(e)}")
        import traceback
        traceback.print_exc()
        print("Falling back to table lookup")
        return {}

# COMMAND ----------

# Join with SAP customers to check existence
# Use MCP if enabled, otherwise use table lookup
sap_customers_df = spark.table("sap_customers")

if USE_MCP_FOR_SAP:
    # Collect unique SAP IDs to check
    sap_ids_to_check = [row.normalized_sap_id for row in validated_df.select("normalized_sap_id").distinct().collect() if row.normalized_sap_id]
    
    # Check via MCP
    mcp_results = check_sap_via_mcp(sap_ids_to_check)
    
    if mcp_results:
        # Convert MCP results to DataFrame
        from pyspark.sql import Row
        mcp_df = spark.createDataFrame([Row(sap_id=k, mcp_exists=v) for k, v in mcp_results.items()])
        
        # Join with validated data
        validated_with_lookup_df = validated_df.join(
            mcp_df,
            validated_df.normalized_sap_id == mcp_df.sap_id,
            "left"
        ).select(
            validated_df["*"],
            when(mcp_df.mcp_exists.isNotNull(), mcp_df.mcp_exists).otherwise(False).alias("sap_exists")
        )
        print("✅ Using MCP for SAP existence check")
    else:
        # Fallback to table lookup
        print("⚠️ MCP failed or returned empty, using table lookup")
        validated_with_lookup_df = validated_df.join(
            sap_customers_df,
            validated_df.normalized_sap_id == sap_customers_df.sap_id,
            "left"
        ).select(
            validated_df["*"],
            when(sap_customers_df.sap_id.isNotNull(), True).otherwise(False).alias("sap_exists")
        )
else:
    # Standard table lookup
    validated_with_lookup_df = validated_df.join(
        sap_customers_df,
        validated_df.normalized_sap_id == sap_customers_df.sap_id,
        "left"
    ).select(
        validated_df["*"],
        when(sap_customers_df.sap_id.isNotNull(), True).otherwise(False).alias("sap_exists")
    )
    print("✅ Using table lookup for SAP existence check")

display(validated_with_lookup_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Determine Validation Status

# COMMAND ----------

# Calculate final validation status
final_df = validated_with_lookup_df.withColumn(
    "validation_status",
    when(
        # All fields valid and SAP exists
        (col("sap_id_valid") & col("name_valid") & col("email_valid") & col("phone_valid") & col("sap_exists")),
        "PASS"
    ).when(
        # All fields valid but SAP doesn't exist
        (col("sap_id_valid") & col("name_valid") & col("email_valid") & col("phone_valid") & ~col("sap_exists")),
        "NEEDS_REVIEW"
    ).otherwise(
        "FAIL"
    )
).withColumn(
    "queue_type",
    when(col("validation_status") == "PASS", "quick_approval")
    .when(col("validation_status") == "NEEDS_REVIEW", "detailed_review")
    .otherwise("rejected")
)

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. View Statistics

# COMMAND ----------

# Summary statistics
summary_df = final_df.groupBy("validation_status", "queue_type").count().orderBy("queue_type")
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Create Review Queue Table

# COMMAND ----------

# Create the review queue table with all relevant information
review_queue_df = final_df.select(
    col("email_id"),
    col("sender"),
    col("validation_status"),
    col("queue_type"),
    
    # Original extracted values
    col("sap_id"),
    col("contact_name"),
    col("contact_email"),
    col("contact_phone"),
    
    # Normalized values
    col("normalized_sap_id"),
    col("normalized_name"),
    col("normalized_phone"),
    
    # Validation flags
    col("sap_id_valid"),
    col("name_valid"),
    col("email_valid"),
    col("phone_valid"),
    col("sap_exists"),
    
    # Errors (filter out nulls)
    array(
        when(~col("sap_id_valid"), col("sap_validation.error")),
        when(~col("name_valid"), col("name_validation.error")),
        when(~col("email_valid"), col("email_validation.error")),
        when(~col("phone_valid"), col("phone_validation.error")),
        when(col("sap_id_valid") & ~col("sap_exists"), lit("SAP ID not found in database"))
    ).alias("errors"),
    
    current_timestamp().alias("queued_at")
)

# Write to table
review_queue_df.write.mode("overwrite").saveAsTable("review_queue")

print(f"✅ Review queue created with {review_queue_df.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. View Items Needing Review

# COMMAND ----------

# Show items that need human review
needs_review_df = spark.sql("""
  SELECT 
    email_id,
    validation_status,
    queue_type,
    sap_id,
    contact_name,
    contact_email,
    contact_phone,
    normalized_sap_id,
    normalized_name,
    normalized_phone,
    errors
  FROM review_queue
  WHERE queue_type IN ('detailed_review', 'rejected')
  ORDER BY validation_status, email_id
""")

display(needs_review_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. View Items Ready for Auto-Approval

# COMMAND ----------

# Show items that passed validation
auto_approve_df = spark.sql("""
  SELECT 
    email_id,
    validation_status,
    normalized_sap_id,
    normalized_name,
    normalized_phone AS contact_phone,
    contact_email,
    queued_at
  FROM review_queue
  WHERE queue_type = 'quick_approval'
  ORDER BY email_id
""")

display(auto_approve_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### What We Built
# MAGIC - ✅ Simple Python validation functions (similar to what you're already using)
# MAGIC - ✅ Applied validations to Agent Bricks extraction results
# MAGIC - ✅ Checked SAP ID existence in mock database
# MAGIC - ✅ Created intelligent routing (auto-approve vs. needs review)
# MAGIC - ✅ Review queue table for human-in-the-loop
# MAGIC
# MAGIC ### Key Advantages
# MAGIC 1. **Simple & Maintainable** - Pure Python, easy to modify
# MAGIC 2. **Reusable** - Same validation logic can be used anywhere
# MAGIC 3. **Scalable** - Spark UDFs process data in parallel
# MAGIC 4. **Production-Ready** - Clear error handling and routing
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Part 3**: Human-in-the-loop review process
# MAGIC - **Part 4**: Action execution (write approved changes to Delta/SAP)
# MAGIC - **Part 5**: Orchestration with Databricks Workflows

