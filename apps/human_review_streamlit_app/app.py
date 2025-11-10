"""
Human Review Streamlit App for Email Validation
Review AI-extracted email data and approve or send follow-ups
"""

import os
import re
from typing import Dict, List
import streamlit as st
import pandas as pd
from dotenv import load_dotenv

# Import data access layer
import data

# Load environment variables for local development
load_dotenv()

# Configuration - read from environment with defaults
CATALOG = os.getenv("DATABRICKS_CATALOG", "main")
SCHEMA = os.getenv("DATABRICKS_SCHEMA", "scotts_genai_workshop")
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# Page config
st.set_page_config(
    page_title="Email Review - Scott's Workshop",
    page_icon="📧",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================================================
# CONNECTION & USER
# ============================================================================

@st.cache_resource
def get_connection():
    """Create cached SQL connection to Databricks warehouse"""
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        raise ValueError("DATABRICKS_WAREHOUSE_ID environment variable not set")
    
    http_path = f"/sql/1.0/warehouses/{warehouse_id}"
    return data.get_connection(http_path)


def get_current_user() -> str:
    """Get current user email from headers or fallback"""
    try:
        headers = st.context.headers
        email = headers.get("X-Forwarded-Email")
        if email:
            return email
    except:
        pass
    return "local_user@example.com"


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_email(email: str) -> Dict:
    """Validate email format"""
    if not email or email.strip() == "":
        return {"is_valid": False, "error": "Email is empty"}
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    if re.match(pattern, email.strip()):
        return {"is_valid": True, "error": None}
    else:
        return {"is_valid": False, "error": f"Invalid email format"}


def validate_phone(phone: str) -> Dict:
    """Validate and normalize phone number"""
    if not phone or phone.strip() == "":
        return {"is_valid": False, "normalized": None, "error": "Phone number is missing"}
    
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


def validate_sap_id(sap_id: str) -> Dict:
    """Validate SAP ID format (SAPXXXXXX)"""
    if not sap_id or sap_id.strip() == "":
        return {"is_valid": False, "normalized": None, "error": "SAP ID is missing"}
    
    # Remove whitespace and convert to uppercase
    cleaned = sap_id.strip().upper()
    
    # Pattern: SAP followed by 6 digits
    pattern = r'^SAP\d{6}$'
    
    if re.match(pattern, cleaned):
        return {"is_valid": True, "normalized": cleaned, "error": None}
    else:
        return {"is_valid": False, "normalized": None, "error": f"Invalid SAP ID format (expected SAPXXXXXX)"}


def validate_name(name: str) -> Dict:
    """Validate contact name"""
    if not name or name.strip() == "":
        return {"is_valid": False, "normalized": None, "error": "Contact name is missing"}
    
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


def validate_all_fields(sap_id: str, name: str, email: str, phone: str) -> Dict:
    """Validate all fields and return combined results"""
    sap_result = validate_sap_id(sap_id)
    name_result = validate_name(name)
    email_result = validate_email(email)
    phone_result = validate_phone(phone)
    
    all_valid = (sap_result["is_valid"] and name_result["is_valid"] and 
                 email_result["is_valid"] and phone_result["is_valid"])
    
    errors = []
    if not sap_result["is_valid"]:
        errors.append({"field": "SAP ID", "error": sap_result['error']})
    if not name_result["is_valid"]:
        errors.append({"field": "Contact Name", "error": name_result['error']})
    if not email_result["is_valid"]:
        errors.append({"field": "Email", "error": email_result['error']})
    if not phone_result["is_valid"]:
        errors.append({"field": "Phone", "error": phone_result['error']})
    
    return {
        "all_valid": all_valid,
        "sap": sap_result,
        "name": name_result,
        "email": email_result,
        "phone": phone_result,
        "errors": errors
    }


# ============================================================================
# UI HELPER FUNCTIONS
# ============================================================================

def generate_followup_email(item: pd.Series, validation_errors: List[Dict]) -> tuple:
    """Generate follow-up email subject and body"""
    sender_name = item.get('contact_name', '').split()[0] if item.get('contact_name') else 'there'
    
    # Build list of missing/invalid fields
    missing_items = "\n".join([f"  • {err['field']}: {err['error']}" for err in validation_errors])
    
    # Extract available SAP ID for reference
    sap_ref = item.get('normalized_sap_id') or item.get('sap_id') or '[your account]'
    
    subject = f"Additional Information Needed - SAP Account Update Request"
    
    body = f"""Hi {sender_name},

Thank you for contacting us about updating your SAP account information.

We received your request for account {sap_ref}, but we need some additional information or corrections to process it:

{missing_items}

Please reply to this email with the corrected information in the following format:

SAP Account ID: SAP123456
Contact Name: First Last
Contact Email: name@example.com
Contact Phone: (555) 123-4567

We'll process your update as soon as we receive the complete information.

Best regards,
Customer Service Team
Scott's Miracle-Gro"""
    
    return subject, body


def render_status_badge(status: str):
    """Render a colored status badge"""
    if status == "PASS":
        st.success("✅ **Complete** - All fields extracted successfully")
    elif status == "NEEDS_REVIEW":
        st.warning("⚠️ **Needs Review** - SAP ID not found in database")
    else:
        st.error("❌ **Incomplete** - Missing or invalid fields")


# ============================================================================
# CACHED DATA FUNCTIONS
# ============================================================================

@st.cache_data(ttl=30)
def get_kpi_counts(_conn) -> Dict:
    """Get KPI counts with 30s cache"""
    return data.get_kpi_counts(_conn, FULL_SCHEMA)


@st.cache_data(ttl=30)
def get_pending_items(_conn, search_query: str = "") -> pd.DataFrame:
    """Get pending items with 30s cache"""
    return data.get_pending_items(_conn, FULL_SCHEMA, search_query)


# ============================================================================
# MAIN APP
# ============================================================================

def main():
    st.title("📧 Email Review System")
    st.markdown("Review extracted email data and take action")
    
    # Get connection
    try:
        conn = get_connection()
        data.ensure_tables_exist(conn, FULL_SCHEMA)
    except Exception as e:
        st.error(f"❌ Failed to connect to Databricks: {str(e)}")
        st.info("Check that DATABRICKS_WAREHOUSE_ID is set and credentials are configured")
        return
    
    # Get current user
    current_user = get_current_user()
    
    # Sidebar - User Info
    st.sidebar.info(f"👤 **User:** {current_user}")
    st.sidebar.markdown("---")
    
    # About the app
    with st.sidebar.expander("ℹ️ About This App"):
        st.markdown("""
        **Workshop Demo: Human-in-the-Loop Review**
        
        This app demonstrates:
        - ✅ Email extraction validation
        - 👤 Human approval workflow  
        - 📊 Delta Lake for data storage
        - 🔍 Audit trail tracking
        
        **Data Flow:**
        1. AI extracts data from emails (`validation_notebook.py`)
        2. You review and validate in this app
        3. Approved changes → Delta tables
        4. Ready for downstream processing (SAP, CRM, etc.)
        """)
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["📋 Review Emails", "📊 Activity Log", "📤 Follow-up Emails"])
    
    # ========================================================================
    # TAB 1: REVIEW EMAILS
    # ========================================================================
    with tab1:
        # Quick guide
        with st.expander("📖 How to Use This App"):
            st.markdown("""
            **Review Process:**
            1. Select an email from the list below
            2. Review the original email vs. extracted data
            3. Check validation results (✅/❌ indicators)
            4. Take action:
               - **If complete:** Click "Confirm & Approve" → Saves to Delta
               - **If incomplete:** Send follow-up email to customer
            
            **What Happens When You Approve:**
            - ✅ Data saved to `approved_changes` Delta table
            - 📊 Action logged in `review_actions` audit trail
            - 🔄 Ready for downstream processing
            """)
        
        # KPIs
        kpis = get_kpi_counts(conn)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📨 Total Pending", kpis["total"])
        with col2:
            st.metric("✅ Complete", kpis["pass"])
        with col3:
            st.metric("⚠️ Needs Review", kpis["needs_review"])
        with col4:
            st.metric("❌ Incomplete", kpis["fail"])
        
        st.markdown("---")
        
        # Search
        search_query = st.text_input("🔎 Search emails", placeholder="Email ID, Sender, SAP ID")
        
        # Get pending items
        pending_items = get_pending_items(conn, search_query)
        
        if pending_items.empty:
            st.success("🎉 No emails to review!")
            return
        
        st.markdown(f"### 📬 {len(pending_items)} Emails Awaiting Review")
        
        # Display table
        display_df = pending_items[["email_id", "sender", "validation_status", "normalized_sap_id", "normalized_name"]].copy()
        display_df.columns = ["Email ID", "From", "Status", "SAP ID", "Name"]
        
        # Add status emoji
        status_map = {"PASS": "✅", "NEEDS_REVIEW": "⚠️", "FAIL": "❌"}
        display_df["Status"] = display_df["Status"].apply(lambda x: f"{status_map.get(x, '')} {x}")
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # Select email
        selected_email_id = st.selectbox(
            "**Select an email to review:**",
            pending_items["email_id"].tolist(),
            format_func=lambda x: f"{x} - {pending_items[pending_items['email_id']==x]['sender'].iloc[0]}"
        )
        
        if selected_email_id:
            item = pending_items[pending_items["email_id"] == selected_email_id].iloc[0]
            email_body = data.get_email_body(conn, FULL_SCHEMA, selected_email_id)
            
            st.markdown(f"## 📧 {selected_email_id}")
            render_status_badge(item['validation_status'])
            
            st.markdown("---")
            
            # Two column layout
            left_col, right_col = st.columns([1, 1])
            
            with left_col:
                st.markdown("### 📨 Original Email")
                st.markdown(f"**From:** {item['sender']}")
                st.text_area("Email Body", email_body or "Email not found", height=400, disabled=True, label_visibility="collapsed")
            
            with right_col:
                st.markdown("### 🔍 Extracted Information")
                
                # Show extracted fields with validation
                st.markdown("#### Fields")
                
                # Get normalized or raw values
                sap_value = item['normalized_sap_id'] or item['sap_id'] or ""
                name_value = item['normalized_name'] or item['contact_name'] or ""
                email_value = item['contact_email'] or ""
                phone_value = item['normalized_phone'] or item['contact_phone'] or ""
                
                # Display with status indicators
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.text_input("SAP ID", value=sap_value, disabled=True, key=f"sap_display_{selected_email_id}")
                with col2:
                    if item['sap_id_valid']:
                        st.markdown("<div style='padding-top: 30px;'>✅</div>", unsafe_allow_html=True)
                    else:
                        st.markdown("<div style='padding-top: 30px;'>❌</div>", unsafe_allow_html=True)
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.text_input("Contact Name", value=name_value, disabled=True, key=f"name_display_{selected_email_id}")
                with col2:
                    if item['name_valid']:
                        st.markdown("<div style='padding-top: 30px;'>✅</div>", unsafe_allow_html=True)
                    else:
                        st.markdown("<div style='padding-top: 30px;'>❌</div>", unsafe_allow_html=True)
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.text_input("Email", value=email_value, disabled=True, key=f"email_display_{selected_email_id}")
                with col2:
                    if item['email_valid']:
                        st.markdown("<div style='padding-top: 30px;'>✅</div>", unsafe_allow_html=True)
                    else:
                        st.markdown("<div style='padding-top: 30px;'>❌</div>", unsafe_allow_html=True)
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.text_input("Phone", value=phone_value, disabled=True, key=f"phone_display_{selected_email_id}")
                with col2:
                    if item['phone_valid']:
                        st.markdown("<div style='padding-top: 30px;'>✅</div>", unsafe_allow_html=True)
                    else:
                        st.markdown("<div style='padding-top: 30px;'>❌</div>", unsafe_allow_html=True)
                
                # Special case: SAP exists check
                if item['sap_id_valid'] and not item['sap_exists']:
                    st.warning("⚠️ **Note:** SAP ID format is correct but not found in customer database")
                
                st.markdown("---")
                st.markdown("### 🎯 Actions")
                
                # Validate fields
                validation = validate_all_fields(sap_value, name_value, email_value, phone_value)
                
                # Decision: Complete or needs follow-up
                if validation["all_valid"] and item['sap_exists']:
                    # COMPLETE - Ready to confirm
                    st.success("✅ All information is complete and valid!")
                    
                    # Educational info about what will happen
                    with st.expander("ℹ️ What happens when you approve?"):
                        st.markdown(f"""
                        **Delta Lake:**
                        - Writes to `{FULL_SCHEMA}.approved_changes`
                        - Logs action to `{FULL_SCHEMA}.review_actions`
                        
                        **Downstream Processing:**
                        - Data is ready for SAP, CRM, or other systems
                        - Build connectors to consume approved_changes table
                        """)
                    
                    if st.button("✅ Confirm & Approve", type="primary", use_container_width=True):
                        data.confirm_extraction(
                            conn,
                            FULL_SCHEMA,
                            selected_email_id,
                            validation["sap"]["normalized"],
                            validation["name"]["normalized"],
                            email_value,
                            validation["phone"]["normalized"],
                            email_body or "",
                            current_user
                        )
                        
                        st.success(f"✅ **Approved!** Data saved to `{FULL_SCHEMA}.approved_changes`")
                        st.info("🔄 Ready for downstream processing")
                        
                        st.cache_data.clear()
                        st.rerun()
                
                else:
                    # INCOMPLETE - Generate follow-up email
                    st.warning("❌ Missing or invalid information detected")
                    
                    if validation["errors"]:
                        st.markdown("**Issues found:**")
                        for err in validation["errors"]:
                            st.error(f"• {err['field']}: {err['error']}")
                    
                    st.markdown("---")
                    st.markdown("### ✉️ Follow-up Email")
                    
                    with st.expander("ℹ️ What happens when you send a follow-up?"):
                        st.markdown(f"""
                        **Actions:**
                        - Email saved to `{FULL_SCHEMA}.outgoing_emails` (status: pending)
                        - Logged to `{FULL_SCHEMA}.review_actions`
                        - Email removed from your review queue
                        
                        **Note:** This demo saves emails to Delta. In production, connect to an email service.
                        """)
                    
                    # Auto-generate follow-up
                    subject, body = generate_followup_email(item, validation["errors"])
                    
                    st.text_input("To:", value=item['sender'], disabled=True)
                    edited_subject = st.text_input("Subject:", value=subject)
                    edited_body = st.text_area("Message:", value=body, height=300)
                    
                    if st.button("📤 Send Follow-up Email", type="primary", use_container_width=True):
                        data.send_followup_email(
                            conn,
                            FULL_SCHEMA,
                            selected_email_id,
                            item['sender'],
                            edited_subject,
                            edited_body,
                            validation["errors"],
                            current_user
                        )
                        st.success(f"✅ **Saved to Delta:** `{FULL_SCHEMA}.outgoing_emails`")
                        st.info(f"📤 Follow-up queued for {item['sender']}")
                        st.cache_data.clear()
                        st.rerun()
    
    # ========================================================================
    # TAB 2: ACTIVITY LOG
    # ========================================================================
    with tab2:
        st.subheader("📊 Recent Activity")
        
        audit_df = data.get_audit_log(conn, FULL_SCHEMA, limit=100)
        
        if audit_df.empty:
            st.info("No activity yet")
        else:
            # Format action names
            action_map = {
                "confirmed": "✅ Confirmed",
                "followup_sent": "📤 Follow-up Sent"
            }
            audit_df["action"] = audit_df["action"].apply(lambda x: action_map.get(x, x))
            
            st.dataframe(audit_df, use_container_width=True, hide_index=True)
            
            csv = audit_df.to_csv(index=False)
            st.download_button("📥 Download Log", csv, "activity_log.csv", "text/csv")
    
    # ========================================================================
    # TAB 3: FOLLOW-UP EMAILS
    # ========================================================================
    with tab3:
        st.subheader("📤 Follow-up Emails Sent")
        
        followup_df = data.get_followup_emails(conn, FULL_SCHEMA, limit=100)
        
        if followup_df.empty:
            st.info("No follow-up emails sent yet")
        else:
            st.dataframe(followup_df, use_container_width=True, hide_index=True)
            
            # Show details
            if len(followup_df) > 0:
                st.markdown("---")
                selected_followup = st.selectbox("View email details:", followup_df["email_id"].tolist())
                
                if selected_followup:
                    followup_item = followup_df[followup_df["email_id"] == selected_followup].iloc[0]
                    result = data.get_followup_email_detail(conn, FULL_SCHEMA, selected_followup)
                    if result:
                        st.markdown(f"**To:** {followup_item['to_email']}")
                        st.markdown(f"**Subject:** {result[0]}")
                        st.text_area("Body:", value=result[1], height=300, disabled=True)


if __name__ == "__main__":
    main()
