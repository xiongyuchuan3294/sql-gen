import jinja2
import yaml
import sys

# 1. Define the Jinja2 Template (Mocking the Knowledge Base template)
# This template supports inserting random data or mapped data into a partitioned table.
template_str = """
-- Generated SQL for Intent: {{ intent }}
INSERT {{ 'OVERWRITE' if options.overwrite else 'INTO' }} TABLE {{ table }} 
PARTITION ({{ partition.key }}='{{ partition.value }}')
SELECT
{%- for field, definition in data.fields.items() %}
  {{ definition }} AS {{ field }}{{ "," if not loop.last else "" }}
{%- endfor %}
FROM (SELECT 1) t 
LATERAL VIEW posexplode(split(space({{ options.limit - 1 }}), ' ')) pe as i, x
LIMIT {{ options.limit }};
"""

# 2. Define the Input Configuration (Mocking the NL->YAML output)
# Scenario: Generate 5 rows for 'user_activity' table, partition dt='2024-05-20'.
# Fields: uid (random), activity (random selection), timestamp (current).
yaml_config = """
intent: "Generate Mock Data"
table: "user_activity"
partition:
  key: "dt"
  value: "2024-05-20"
options:
  overwrite: true
  limit: 5
data:
  fields:
    uid: "floor(rand() * 1000000)"
    activity: "elt(ceil(rand()*3), 'login', 'logout', 'purchase')"
    server_time: "current_timestamp()"
"""

def verify_generation():
    print("--- [TEST START] Verifying SQL Generation Design ---")
    
    # Step A: Load Configuration
    print("\n[Step 1] Loading Configuration (YAML)...")
    config = yaml.safe_load(yaml_config)
    print("Config Loaded:")
    print(yaml.dump(config, default_flow_style=False))

    # Step B: Load Template
    print("\n[Step 2] Loading Jinja2 Template...")
    env = jinja2.Environment()
    template = env.from_string(template_str)

    # Step C: Render
    print("\n[Step 3] Rendering SQL...")
    try:
        sql_output = template.render(**config)
    except Exception as e:
        print(f"Error rendering template: {e}")
        sys.exit(1)

    # Step D: Output & Validation Check (Simulation)
    print("\n[Step 4] Generated SQL Output:")
    print("-" * 40)
    print(sql_output.strip())
    print("-" * 40)

    # Validation Checks (as defined in SKILL.md)
    print("\n[Step 5] Auto-Validation (Simulating Agent Self-Correction)...")
    validation_failures = []
    
    # 1. LIMIT Check
    if "LIMIT" not in sql_output:
        validation_failures.append("Missing LIMIT clause")
    
    # 2. PARTITION Check
    if "PARTITION" not in sql_output:
        validation_failures.append("Missing PARTITION clause")
        
    if not validation_failures:
        print("✅ VALIDATION PASSED: SQL contains required 'LIMIT' and 'PARTITION'.")
    else:
        print(f"❌ VALIDATION FAILED: {validation_failures}")

    print("\n--- [TEST END] Feasibility Verification COmplete ---")

if __name__ == "__main__":
    verify_generation()
