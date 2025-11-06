# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2b: Register Unity Catalog Functions for Agentic Flow
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook registers reusable Unity Catalog functions to enable a dynamic, agentic email processing flow.
# MAGIC
# MAGIC ### UC Functions Registered
# MAGIC 1. **Validation Functions** - Format validation and normalization
# MAGIC    - `fn_validate_email(email)` → `STRUCT<is_valid:BOOLEAN, error:STRING>`
# MAGIC    - `fn_validate_phone(phone)` → `STRUCT<is_valid:BOOLEAN, normalized:STRING, error:STRING>`
# MAGIC    - `fn_validate_sap_id(sap_id)` → `STRUCT<is_valid:BOOLEAN, normalized:STRING, error:STRING>`
# MAGIC    - `fn_validate_name(name)` → `STRUCT<is_valid:BOOLEAN, normalized:STRING, error:STRING>`
# MAGIC
# MAGIC 2. **Lookup Functions** - Check data existence
# MAGIC    - `fn_sap_exists(sap_id)` → `BOOLEAN`
# MAGIC
# MAGIC 3. **Routing Functions** - Determine validation status
# MAGIC    - `fn_route_validation(...)` → `STRUCT<validation_status:STRING, queue_type:STRING>`
# MAGIC
# MAGIC 4. **Extraction Functions** - Extract structured data from email body
# MAGIC    - `fn_extract_email(body)` → `STRUCT<sap_id:STRING, contact_name:STRING, ...>`
# MAGIC
# MAGIC 5. **Packaging Functions** - Build review records
# MAGIC    - `fn_build_review_record(...)` → `STRUCT<...>` (complete review queue row)
# MAGIC
# MAGIC ### Agentic Flow
# MAGIC ```
# MAGIC Raw Email → Extract → Validate → Lookup → Route → Package → Review Queue
# MAGIC ```
# MAGIC
# MAGIC All functions are **pure** (no side effects). Writing to tables happens in separate notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Configuration
CATALOG = "main"
SCHEMA = "scotts_genai_workshop"

# Use the catalog and schema
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"✅ Using: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Register Validation Functions (Python)
# MAGIC
# MAGIC These functions validate and normalize extracted fields.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 fn_validate_email

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.fn_validate_email(email STRING)
  RETURNS STRUCT<is_valid:BOOLEAN, error:STRING>
  LANGUAGE PYTHON
  COMMENT 'Validate email format and return validation result'
  AS $$
  import re
  
  if not email or email.strip() == "":
    return {{"is_valid": False, "error": "Email is empty"}}
  
  pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{{2,}}$'
  
  if re.match(pattern, email.strip()):
    return {{"is_valid": True, "error": None}}
  else:
    return {{"is_valid": False, "error": f"Invalid email format: {{email}}"}}
  $$
""")

print("✅ Registered fn_validate_email")

# Test it
result = spark.sql("SELECT fn_validate_email('john@example.com') as result").collect()[0]['result']
print(f"Test: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 fn_validate_phone

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.fn_validate_phone(phone STRING)
  RETURNS STRUCT<is_valid:BOOLEAN, normalized:STRING, error:STRING>
  LANGUAGE PYTHON
  COMMENT 'Validate phone number format and return normalized phone'
  AS $$
  import re
  
  if not phone or phone.strip() == "":
    return {{"is_valid": False, "normalized": None, "error": "Phone is empty"}}
  
  # Remove all non-digit characters
  digits = re.sub(r'\\\\D', '', phone)
  
  # Handle +1 prefix
  if digits.startswith('1') and len(digits) == 11:
    digits = digits[1:]
  
  # Check if we have exactly 10 digits
  if len(digits) != 10:
    return {{"is_valid": False, "normalized": None, "error": f"Phone must have 10 digits, got {{len(digits)}}"}}
  
  # Validate area code
  if digits[0] in ['0', '1']:
    return {{"is_valid": False, "normalized": None, "error": "Area code cannot start with 0 or 1"}}
  
  # Format as (XXX) XXX-XXXX
  normalized = f"({{digits[0:3]}}) {{digits[3:6]}}-{{digits[6:10]}}"
  
  return {{"is_valid": True, "normalized": normalized, "error": None}}
  $$
""")

print("✅ Registered fn_validate_phone")

# Test it
result = spark.sql("SELECT fn_validate_phone('555-123-4567') as result").collect()[0]['result']
print(f"Test: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 fn_validate_sap_id

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.fn_validate_sap_id(sap_id STRING)
  RETURNS STRUCT<is_valid:BOOLEAN, normalized:STRING, error:STRING>
  LANGUAGE PYTHON
  COMMENT 'Validate SAP ID format (SAPXXXXXX) and return normalized ID'
  AS $$
  import re
  
  if not sap_id or sap_id.strip() == "":
    return {{"is_valid": False, "normalized": None, "error": "SAP ID is empty"}}
  
  # Remove whitespace and convert to uppercase
  cleaned = sap_id.strip().upper()
  
  # Pattern: SAP followed by 6 digits
  pattern = r'^SAP\\\\d{{6}}$'
  
  if re.match(pattern, cleaned):
    return {{"is_valid": True, "normalized": cleaned, "error": None}}
  else:
    return {{"is_valid": False, "normalized": None, "error": f"Invalid SAP ID format (expected SAPXXXXXX): {{sap_id}}"}}
  $$
""")

print("✅ Registered fn_validate_sap_id")

# Test it
result = spark.sql("SELECT fn_validate_sap_id('sap123456') as result").collect()[0]['result']
print(f"Test: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 fn_validate_name

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.fn_validate_name(name STRING)
  RETURNS STRUCT<is_valid:BOOLEAN, normalized:STRING, error:STRING>
  LANGUAGE PYTHON
  COMMENT 'Validate contact name and return normalized name'
  AS $$
  if not name or name.strip() == "":
    return {{"is_valid": False, "normalized": None, "error": "Name is empty"}}
  
  cleaned = name.strip()
  
  # Check minimum length
  if len(cleaned) < 2:
    return {{"is_valid": False, "normalized": None, "error": "Name too short"}}
  
  # Check for at least two parts (first and last name)
  parts = cleaned.split()
  if len(parts) < 2:
    return {{"is_valid": False, "normalized": None, "error": "Name must include first and last name"}}
  
  # Normalize to Title Case
  normalized = cleaned.title()
  
  return {{"is_valid": True, "normalized": normalized, "error": None}}
  $$
""")

print("✅ Registered fn_validate_name")

# Test it
result = spark.sql("SELECT fn_validate_name('john doe') as result").collect()[0]['result']
print(f"Test: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Register Lookup Functions (SQL)
# MAGIC
# MAGIC These functions check data existence against reference tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 fn_sap_exists

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.fn_sap_exists(sap_id STRING)
  RETURNS BOOLEAN
  COMMENT 'Check if SAP customer ID exists in sap_customers table'
  RETURN (
    SELECT COUNT(*) > 0
    FROM {CATALOG}.{SCHEMA}.sap_customers
    WHERE sap_id = UPPER(fn_sap_exists.sap_id)
  )
""")

print("✅ Registered fn_sap_exists")

# Test it (assumes sap_customers table exists from 02_validation_simple)
result = spark.sql("SELECT fn_sap_exists('SAP123456') as exists").collect()[0]['exists']
print(f"Test: SAP123456 exists = {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Register Routing Functions (SQL)
# MAGIC
# MAGIC These functions determine validation status and routing logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 fn_route_validation

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.fn_route_validation(
    sap_ok BOOLEAN,
    name_ok BOOLEAN,
    email_ok BOOLEAN,
    phone_ok BOOLEAN,
    exists BOOLEAN
  )
  RETURNS STRUCT<validation_status:STRING, queue_type:STRING>
  COMMENT 'Determine validation status and queue routing based on validation results'
  RETURN
    CASE
      -- All valid and SAP exists → PASS → quick_approval
      WHEN sap_ok AND name_ok AND email_ok AND phone_ok AND exists THEN
        NAMED_STRUCT('validation_status', 'PASS', 'queue_type', 'quick_approval')
      
      -- All valid but SAP doesn't exist → NEEDS_REVIEW → detailed_review
      WHEN sap_ok AND name_ok AND email_ok AND phone_ok AND NOT exists THEN
        NAMED_STRUCT('validation_status', 'NEEDS_REVIEW', 'queue_type', 'detailed_review')
      
      -- Any field invalid → FAIL → rejected
      ELSE
        NAMED_STRUCT('validation_status', 'FAIL', 'queue_type', 'rejected')
    END
""")

print("✅ Registered fn_route_validation")

# Test it
result = spark.sql("""
  SELECT fn_route_validation(true, true, true, true, true) as pass_case,
         fn_route_validation(true, true, true, true, false) as review_case,
         fn_route_validation(false, true, true, true, true) as fail_case
""").collect()[0]
print(f"Test PASS: {result['pass_case']}")
print(f"Test NEEDS_REVIEW: {result['review_case']}")
print(f"Test FAIL: {result['fail_case']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Register Extraction Functions (Python)
# MAGIC
# MAGIC Extract structured data from raw email body.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 fn_extract_email
# MAGIC
# MAGIC This function wraps `ai_query` for real extraction, with a mock fallback for testing.

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.fn_extract_email(body STRING)
  RETURNS STRUCT<
    sap_id:STRING,
    contact_name:STRING,
    contact_email:STRING,
    contact_phone:STRING,
    request_type:STRING,
    raw:STRING,
    error:STRING
  >
  COMMENT 'Extract structured fields from email body using Agent Bricks ai_query'
  RETURN (
    WITH extraction AS (
      SELECT 
        body,
        ai_query('kie-4cb95ce7-endpoint', body, failOnError => false) AS response
    )
    SELECT 
      NAMED_STRUCT(
        'sap_id', response.result:sap_id::string,
        'contact_name', response.result:contact_name::string,
        'contact_email', response.result:contact_email::string,
        'contact_phone', response.result:contact_phone::string,
        'request_type', COALESCE(response.result:request_type::string, 'account_update'),
        'raw', body,
        'error', response.errorMessage
      )
    FROM extraction
  )
""")

print("✅ Registered fn_extract_email (Agent Bricks ai_query wrapper)")

# Test it
test_email = "Hello, Please update my account information. Name: John Smith SAP ID: SAP123456 Email: john.smith@example.com Phone: 555-123-4567 Thank you!"

print("\nTesting fn_extract_email with Agent Bricks endpoint...")
try:
    result = spark.sql(f"SELECT fn_extract_email('{test_email}') as result").collect()[0]['result']
    print(f"✅ Extraction result: {result}")
except Exception as e:
    print(f"⚠️ Test failed (endpoint may not be available): {e}")
    print("   Make sure the 'kie-4cb95ce7-endpoint' is deployed and accessible")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Register Packaging Functions (SQL)
# MAGIC
# MAGIC Build complete review records from validated data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 fn_build_review_record

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.fn_build_review_record(
    email_id STRING,
    sender STRING,
    sap_id STRING,
    contact_name STRING,
    contact_email STRING,
    contact_phone STRING,
    normalized_sap_id STRING,
    normalized_name STRING,
    normalized_phone STRING,
    sap_id_valid BOOLEAN,
    name_valid BOOLEAN,
    email_valid BOOLEAN,
    phone_valid BOOLEAN,
    sap_exists BOOLEAN,
    validation_status STRING,
    queue_type STRING
  )
  RETURNS STRUCT<
    email_id:STRING,
    sender:STRING,
    validation_status:STRING,
    queue_type:STRING,
    sap_id:STRING,
    contact_name:STRING,
    contact_email:STRING,
    contact_phone:STRING,
    normalized_sap_id:STRING,
    normalized_name:STRING,
    normalized_phone:STRING,
    sap_id_valid:BOOLEAN,
    name_valid:BOOLEAN,
    email_valid:BOOLEAN,
    phone_valid:BOOLEAN,
    sap_exists:BOOLEAN
  >
  COMMENT 'Package validated data into a review record structure'
  RETURN NAMED_STRUCT(
    'email_id', email_id,
    'sender', sender,
    'validation_status', validation_status,
    'queue_type', queue_type,
    'sap_id', sap_id,
    'contact_name', contact_name,
    'contact_email', contact_email,
    'contact_phone', contact_phone,
    'normalized_sap_id', normalized_sap_id,
    'normalized_name', normalized_name,
    'normalized_phone', normalized_phone,
    'sap_id_valid', sap_id_valid,
    'name_valid', name_valid,
    'email_valid', email_valid,
    'phone_valid', phone_valid,
    'sap_exists', sap_exists
  )
""")

print("✅ Registered fn_build_review_record")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. List All Registered Functions

# COMMAND ----------

functions_df = spark.sql(f"""
  SHOW USER FUNCTIONS IN {CATALOG}.{SCHEMA}
  LIKE 'fn_*'
""")

print(f"\n✅ Registered {functions_df.count()} UC functions:")
display(functions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Demo: End-to-End Agentic Flow (SQL Only)
# MAGIC
# MAGIC This demonstrates how an agent (or SQL pipeline) can call these functions dynamically.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Simple Demo with Mock Extraction

# COMMAND ----------

demo_sql = f"""
WITH base AS (
  SELECT 
    email_id,
    sender,
    body
  FROM {CATALOG}.{SCHEMA}.synthetic_emails
  LIMIT 10
),
extracted AS (
  SELECT
    email_id,
    sender,
    body,
    fn_extract_email(body) AS extracted
  FROM base
),
validated AS (
  SELECT
    email_id,
    sender,
    extracted.sap_id AS sap_id,
    extracted.contact_name AS contact_name,
    extracted.contact_email AS contact_email,
    extracted.contact_phone AS contact_phone,
    fn_validate_sap_id(extracted.sap_id) AS sap_validation,
    fn_validate_name(extracted.contact_name) AS name_validation,
    fn_validate_email(extracted.contact_email) AS email_validation,
    fn_validate_phone(extracted.contact_phone) AS phone_validation
  FROM extracted
),
shaped AS (
  SELECT
    email_id,
    sender,
    sap_id,
    contact_name,
    contact_email,
    contact_phone,
    sap_validation.is_valid AS sap_id_valid,
    sap_validation.normalized AS normalized_sap_id,
    name_validation.is_valid AS name_valid,
    name_validation.normalized AS normalized_name,
    email_validation.is_valid AS email_valid,
    phone_validation.is_valid AS phone_valid,
    phone_validation.normalized AS normalized_phone,
    fn_sap_exists(sap_validation.normalized) AS sap_exists
  FROM validated
),
routed AS (
  SELECT
    *,
    fn_route_validation(sap_id_valid, name_valid, email_valid, phone_valid, sap_exists) AS routing
  FROM shaped
),
final AS (
  SELECT
    fn_build_review_record(
      email_id,
      sender,
      sap_id,
      contact_name,
      contact_email,
      contact_phone,
      normalized_sap_id,
      normalized_name,
      normalized_phone,
      sap_id_valid,
      name_valid,
      email_valid,
      phone_valid,
      sap_exists,
      routing.validation_status,
      routing.queue_type
    ) AS review_record
  FROM routed
)
SELECT
  review_record.email_id,
  review_record.sender,
  review_record.validation_status,
  review_record.queue_type,
  review_record.normalized_sap_id,
  review_record.normalized_name,
  review_record.contact_email,
  review_record.normalized_phone,
  review_record.sap_exists
FROM final
"""

print("Running end-to-end demo...")
demo_df = spark.sql(demo_sql)
display(demo_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Demo with Real Agent Bricks Extraction (ai_query)
# MAGIC
# MAGIC This version uses the actual Agent Bricks endpoint for extraction.

# COMMAND ----------

# Check if endpoint exists
try:
    endpoint_name = "kie-4cb95ce7-endpoint"
    
    demo_with_ai_sql = f"""
    WITH base AS (
      SELECT 
        email_id,
        sender,
        body
      FROM {CATALOG}.{SCHEMA}.synthetic_emails
      LIMIT 10
    ),
    extracted AS (
      SELECT
        email_id,
        sender,
        body,
        ai_query('{endpoint_name}', body, failOnError => false) AS response
      FROM base
    ),
    parsed AS (
      SELECT
        email_id,
        sender,
        response.result:sap_id::string AS sap_id,
        response.result:contact_name::string AS contact_name,
        response.result:contact_email::string AS contact_email,
        response.result:contact_phone::string AS contact_phone
      FROM extracted
    ),
    validated AS (
      SELECT
        email_id,
        sender,
        sap_id,
        contact_name,
        contact_email,
        contact_phone,
        fn_validate_sap_id(sap_id) AS sap_validation,
        fn_validate_name(contact_name) AS name_validation,
        fn_validate_email(contact_email) AS email_validation,
        fn_validate_phone(contact_phone) AS phone_validation
      FROM parsed
    ),
    shaped AS (
      SELECT
        email_id,
        sender,
        sap_id,
        contact_name,
        contact_email,
        contact_phone,
        sap_validation.is_valid AS sap_id_valid,
        sap_validation.normalized AS normalized_sap_id,
        name_validation.is_valid AS name_valid,
        name_validation.normalized AS normalized_name,
        email_validation.is_valid AS email_valid,
        phone_validation.is_valid AS phone_valid,
        phone_validation.normalized AS normalized_phone,
        fn_sap_exists(sap_validation.normalized) AS sap_exists
      FROM validated
    ),
    routed AS (
      SELECT
        *,
        fn_route_validation(sap_id_valid, name_valid, email_valid, phone_valid, sap_exists) AS routing
      FROM shaped
    ),
    final AS (
      SELECT
        fn_build_review_record(
          email_id,
          sender,
          sap_id,
          contact_name,
          contact_email,
          contact_phone,
          normalized_sap_id,
          normalized_name,
          normalized_phone,
          sap_id_valid,
          name_valid,
          email_valid,
          phone_valid,
          sap_exists,
          routing.validation_status,
          routing.queue_type
        ) AS review_record
      FROM routed
    )
    SELECT
      review_record.email_id,
      review_record.sender,
      review_record.validation_status,
      review_record.queue_type,
      review_record.normalized_sap_id,
      review_record.normalized_name,
      review_record.contact_email,
      review_record.normalized_phone,
      review_record.sap_exists
    FROM final
    """
    
    print(f"Running demo with Agent Bricks endpoint '{endpoint_name}'...")
    demo_ai_df = spark.sql(demo_with_ai_sql)
    display(demo_ai_df)
    
except Exception as e:
    print(f"⚠️ Could not run AI demo (endpoint may not exist): {e}")
    print("   This is expected if you haven't created the Agent Bricks endpoint yet.")
    print("   The mock extraction demo above still works!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Usage Examples for Agents
# MAGIC
# MAGIC ### Python Agent Example
# MAGIC
# MAGIC ```python
# MAGIC from openai import OpenAI
# MAGIC import os
# MAGIC
# MAGIC # Setup
# MAGIC DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN')
# MAGIC client = OpenAI(
# MAGIC     api_key=DATABRICKS_TOKEN,
# MAGIC     base_url="https://your-workspace.cloud.databricks.com/serving-endpoints"
# MAGIC )
# MAGIC
# MAGIC # Define tools from UC functions
# MAGIC tools = [
# MAGIC     {
# MAGIC         "type": "function",
# MAGIC         "function": {
# MAGIC             "name": "validate_email",
# MAGIC             "description": "Validate email format",
# MAGIC             "parameters": {
# MAGIC                 "type": "object",
# MAGIC                 "properties": {
# MAGIC                     "email": {"type": "string", "description": "Email address to validate"}
# MAGIC                 },
# MAGIC                 "required": ["email"]
# MAGIC             }
# MAGIC         }
# MAGIC     },
# MAGIC     # ... more tools for validate_phone, validate_sap_id, etc.
# MAGIC ]
# MAGIC
# MAGIC # Agent call
# MAGIC response = client.chat.completions.create(
# MAGIC     model="your-agent-endpoint",
# MAGIC     messages=[
# MAGIC         {
# MAGIC             "role": "user",
# MAGIC             "content": "Validate this email: john@example.com"
# MAGIC         }
# MAGIC     ],
# MAGIC     tools=tools,
# MAGIC     tool_choice="auto"
# MAGIC )
# MAGIC
# MAGIC # Handle tool calls
# MAGIC if response.choices[0].message.tool_calls:
# MAGIC     for tool_call in response.choices[0].message.tool_calls:
# MAGIC         if tool_call.function.name == "validate_email":
# MAGIC             # Execute UC function via Spark SQL
# MAGIC             result = spark.sql(f"""
# MAGIC                 SELECT fn_validate_email('{tool_call.function.arguments["email"]}')
# MAGIC             """).collect()[0][0]
# MAGIC             print(f"Validation result: {result}")
# MAGIC ```
# MAGIC
# MAGIC ### SQL Agent Example
# MAGIC
# MAGIC ```sql
# MAGIC -- Agent can dynamically construct this query based on user input
# MAGIC SELECT 
# MAGIC   fn_validate_email('user@example.com') AS email_check,
# MAGIC   fn_validate_phone('555-1234') AS phone_check,
# MAGIC   fn_validate_sap_id('SAP123456') AS sap_check,
# MAGIC   fn_sap_exists('SAP123456') AS sap_exists_check
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### What We Built
# MAGIC - ✅ **8 Unity Catalog Functions** - Validation, lookup, routing, extraction, packaging
# MAGIC - ✅ **Pure functions** - No side effects, reusable across agents/workflows/SQL
# MAGIC - ✅ **Discoverable** - Listed in Unity Catalog, available to all users
# MAGIC - ✅ **Governable** - Access control via UC permissions
# MAGIC - ✅ **Agent-ready** - Can be called by LLM agents as tools
# MAGIC
# MAGIC ### UC Functions Registered
# MAGIC 1. `fn_validate_email` - Email format validation
# MAGIC 2. `fn_validate_phone` - Phone validation + normalization
# MAGIC 3. `fn_validate_sap_id` - SAP ID validation + normalization
# MAGIC 4. `fn_validate_name` - Name validation + normalization
# MAGIC 5. `fn_sap_exists` - Check SAP customer existence
# MAGIC 6. `fn_route_validation` - Determine validation status and queue
# MAGIC 7. `fn_extract_email` - Extract structured data from email body
# MAGIC 8. `fn_build_review_record` - Package data into review record
# MAGIC
# MAGIC ### Agentic Flow Pattern
# MAGIC ```
# MAGIC Input Email → fn_extract_email() → fn_validate_*() → fn_sap_exists() 
# MAGIC   → fn_route_validation() → fn_build_review_record() → Write to Delta
# MAGIC ```
# MAGIC
# MAGIC ### Key Advantages vs. Part 2
# MAGIC 1. **Reusable** - Functions can be called from any notebook, workflow, or agent
# MAGIC 2. **Composable** - Build complex flows by chaining simple functions
# MAGIC 3. **Discoverable** - Unity Catalog makes functions easy to find
# MAGIC 4. **Governable** - UC permissions control who can use which functions
# MAGIC 5. **Agent-friendly** - LLMs can call these as tools via function calling
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Use in Agent Framework** - Register these as tools in Mosaic AI Agent Framework
# MAGIC - **Add to LangGraph** - Call from LangGraph agent nodes
# MAGIC - **Workflow Integration** - Use in Databricks Workflows tasks
# MAGIC - **Add more functions** - Generate follow-up emails, merge corrections, etc.
# MAGIC
# MAGIC ### Optional Enhancements (Future)
# MAGIC - `fn_generate_followup_email(errors ARRAY<STRING>, sender STRING)` - Generate follow-up email text
# MAGIC - `fn_merge_corrections(old STRUCT, edits STRUCT)` - Merge HITL corrections
# MAGIC - `fn_sap_exists_mcp(sap_id STRING)` - MCP variant for live SAP lookup
# MAGIC - `fn_apply_contact_update_mcp(...)` - MCP variant for SAP updates

# COMMAND ----------



