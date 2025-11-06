# Email Extraction & Validation Workshop

A practical example of using Databricks Agent Bricks for information extraction from customer emails, with validation workflows.

## Files

### 1. `synthetic_emails.parquet`
200 synthetic customer emails requesting SAP account updates. Realistic email data including clean, messy, incomplete, and ambiguous examples.

### 2. `validation_notebook.py` 
Databricks notebook that validates extracted data using Python UDFs. **Batch/workflow approach** - processes emails sequentially with validation logic, then routes to appropriate queues.

### 3. `register_uc_functions.py`
Databricks notebook that registers Unity Catalog functions for validation, lookup, and routing. **Agentic approach** - enables dynamic, reusable function calls that AI agents can orchestrate.

---

## Quick Start

### Step 1: Import Data to Databricks

Upload `synthetic_emails.parquet` to Databricks and create a Delta table. Go to the catalog and schema of choice, select add table, and import the file.

### Step 2: Extract Fields with Agent Bricks

Create an Agent Bricks endpoint configured to extract these 4 fields from the email body. When prompted for **sample output JSON**, use something like (correspoding to the example in the page):

```json
{
  "sap_id": "SAP123456",
  "contact_name": "John Smith",
  "contact_email": "john.smith@example.com",
  "contact_phone": "(555) 123-4567"
}
```

**Agent Bricks will:**
- Analyze the input table schema
- Use the sample JSON to understand output structure
- Create an AI endpoint for extraction

### Step 3: Choose Your Validation Approach

#### **Option A: Batch/Workflow (Notebook 02)**

Use `validation_notebook.py` for:
- Scheduled batch processing
- Databricks Workflows triggers
- Predictable, sequential validation

```python
# The notebook calls ai_query() inline and validates immediately
extracted_df = spark.sql("""
  SELECT 
    email_id,
    ai_query('your-endpoint', body) AS response
  FROM synthetic_emails
""")
```

**Best for:** Production pipelines, scheduled jobs, compliance workflows

#### **Option B: Agentic (Notebook 02b)**

Use `register_uc_functions.py` to:
- Register UC functions that AI agents can call
- Enable dynamic decision-making
- Compose validation logic on-the-fly

```python
# AI agents can call these functions as tools
SELECT 
  email_id,
  fn_validate_email(contact_email) as email_validation,
  fn_validate_phone(contact_phone) as phone_validation,
  fn_sap_exists(sap_id) as sap_check,
  fn_route_validation(...) as routing
FROM extracted_data
```

**Best for:** Agent-driven workflows, Genie, iterative exploration

---

## Workflow Examples

### Batch Processing (Workflow)
```
1. Schedule: Daily at 9 AM
2. Run: 02_validation_notebook.py
3. Output: review_queue table
4. Notify: Slack alert for high-priority items
```

### Agentic Processing
```
1. User asks: "Process new emails and show me any with issues"
2. Agent: Calls fn_extract_email() → fn_validate_*() → fn_route_validation()
3. Agent: Returns filtered results with explanations
```

---

## Tables Created

- `synthetic_emails` - Raw email data (200 rows)
- `review_queue` - Validated records needing human review
- `auto_approved` - Records passing all validations

## Fields Extracted

| Field | Description | Example |
|-------|-------------|---------|
| `sap_id` | SAP account number | `SAP123456` |
| `contact_name` | Customer name | `John Smith` |
| `contact_email` | New email address | `john@example.com` |
| `contact_phone` | New phone number | `(555) 123-4567` |

---

## Notes

- Update `CATALOG` and `SCHEMA` variables in notebooks to match your environment
- Agent Bricks endpoint name referenced as `kie-4cb95ce7-endpoint` - update to your endpoint
- Validation logic includes format checks, SAP ID lookup simulation, and routing rules
- UC functions in 02b are **pure** (no side effects) - write operations handled separately

