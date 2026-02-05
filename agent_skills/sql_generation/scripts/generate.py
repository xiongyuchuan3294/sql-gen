#!/usr/bin/env python3
import argparse
import sys
import yaml
import os
from jinja2 import Environment, FileSystemLoader, TemplateNotFound

def find_project_root():
    """Find the root of the sql-gen workspace."""
    # Start from current script location and go up until finding 'agent_skills'
    current_path = os.path.abspath(os.path.dirname(__file__))
    while "agent_skills" in current_path:
        parent = os.path.dirname(current_path)
        if parent == current_path: # Root reached
            break
        current_path = parent
    
    # We expect to be in agent_skills/sql_generation/scripts
    # So root should be .../agent_skills/sql_generation/
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    return root

def load_yaml(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def render_template(template_name, params):
    root_dir = find_project_root()
    sql_dir = os.path.join(root_dir, 'templates', 'sql')
    shell_dir = os.path.join(root_dir, 'templates', 'shell')
    
    # Search paths for Jinja2
    env = Environment(loader=FileSystemLoader([sql_dir, shell_dir]))
    
    # Try extensions in order
    candidates = [
        (f"{template_name}.sql", "sql"),
        (f"{template_name}.sh", "sh")
    ]
    
    for filename, ext in candidates:
        try:
            template = env.get_template(filename)
            return template.render(params=params), ext
        except TemplateNotFound:
            continue
            
    print(f"Error: Template {template_name} not found in sql or shell directories.")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Generate Hive SQL from YAML config.")
    parser.add_argument('--yaml', required=True, help="Path to YAML configuration file")
    parser.add_argument('--template', help="Template name (defaults to 'type' in YAML)")
    
    args = parser.parse_args()
    
    # Load Config
    config = load_yaml(args.yaml)
    
    # Determine Template
    template_name = args.template
    if not template_name:
        template_name = config.get('type')
        if not template_name:
            print("Error: YAML file must specify 'type' or --template argument required.")
            sys.exit(1)
            
    # Render
    params = config.get('params', {})
    content, ext = render_template(template_name, params)
    
    print("-" * 20 + f" Generated {ext.upper()} " + "-" * 20)
    print(content)
    print("-" * 55)

    # Optional: Write to output file
    output_dir = os.path.join(find_project_root(), 'output')
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{template_name}_generated.{ext}")
    with open(output_file, 'w') as f:
        f.write(content)
    print(f"Saved to: {output_file}")

if __name__ == "__main__":
    main()
