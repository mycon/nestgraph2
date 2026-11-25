"""
Grafana JSON Dashboard Snippet Helper

A utility to extract multi-line JavaScript and Flux queries from
Grafana dashboard JSON files into separate, readable files for easy
editing. Once edited, it can inject them back into the JSON file safely escaped.
"""

import argparse
import json
import os
import sys
from argparse import RawDescriptionHelpFormatter

def load_json(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading JSON file {filepath}: {e}")
        sys.exit(1)

def save_json(filepath, data):
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.write('\n')
        print(f"Successfully saved updated dashboard to {filepath}")
    except Exception as e:
        print(f"Error saving JSON file {filepath}: {e}")
        sys.exit(1)

def extract_snippets(dashboard_path, out_dir):
    dash = load_json(dashboard_path)
    os.makedirs(out_dir, exist_ok=True)
    
    elements = dash.get("elements", {})
    extracted_count = 0
    panel_mapping = {}

    for panel_key, panel in elements.items():
        panel_id = panel.get("spec", {}).get("id", panel_key)
        panel_title = panel.get("spec", {}).get("title", "Unknown Title")
        panel_mapping[panel_id] = panel_title
        
        # 1. Extract Plotly Scripts
        viz_spec = panel.get("spec", {}).get("vizConfig", {}).get("spec", {})
        script = viz_spec.get("options", {}).get("script")
        if script:
            script_path = os.path.join(out_dir, f"panel_{panel_id}_script.js")
            with open(script_path, 'w', encoding='utf-8') as f_out:
                f_out.write(script)
            print(f"Extracted: {script_path}")
            extracted_count += 1

        # 2. Extract InfluxDB Queries
        queries = panel.get("spec", {}).get("data", {}).get("spec", {}).get("queries", [])
        for q in queries:
            ref_id = q.get("spec", {}).get("refId", "unknown")
            query_str = q.get("spec", {}).get("query", {}).get("spec", {}).get("query")
            if query_str:
                q_path = os.path.join(out_dir, f"panel_{panel_id}_query_{ref_id}.flux")
                with open(q_path, 'w', encoding='utf-8') as f_out:
                    f_out.write(query_str)
                print(f"Extracted: {q_path}")
                extracted_count += 1
                
    mapping_path = os.path.join(out_dir, "panel_mapping.md")
    try:
        with open(mapping_path, 'w', encoding='utf-8') as f_out:
            f_out.write("# Panel Mapping\n\n| Panel ID | Panel Title |\n| :--- | :--- |\n")
            def sort_key(k):
                return int(k) if str(k).isdigit() else str(k)
            for pid in sorted(panel_mapping.keys(), key=sort_key):
                f_out.write(f"| `panel_{pid}_*` | {panel_mapping[pid]} |\n")
        print(f"Generated panel mapping at: {mapping_path}")
    except Exception as e:
        print(f"Warning: Failed to write panel mapping: {e}")

    print(f"\nExtraction complete. {extracted_count} files written to '{out_dir}'.")

def inject_snippets(dashboard_path, in_dir, out_path):
    dash = load_json(dashboard_path)
    elements = dash.get("elements", {})
    injected_count = 0

    for panel_key, panel in elements.items():
        panel_id = panel.get("spec", {}).get("id", panel_key)
        
        # 1. Inject Plotly Scripts
        script_path = os.path.join(in_dir, f"panel_{panel_id}_script.js")
        if os.path.exists(script_path):
            with open(script_path, 'r', encoding='utf-8') as f_in:
                new_script = f_in.read()
                
                # Traverse and set
                try:
                    panel["spec"]["vizConfig"]["spec"]["options"]["script"] = new_script
                    print(f"Injected: {script_path} into Panel {panel_id}")
                    injected_count += 1
                except KeyError:
                    print(f"Warning: Panel {panel_id} doesn't have the expected vizConfig schema. Skipping script.")

        # 2. Inject InfluxDB Queries
        queries = panel.get("spec", {}).get("data", {}).get("spec", {}).get("queries", [])
        for q in queries:
            ref_id = q.get("spec", {}).get("refId", "unknown")
            q_path = os.path.join(in_dir, f"panel_{panel_id}_query_{ref_id}.flux")
            
            if os.path.exists(q_path):
                with open(q_path, 'r', encoding='utf-8') as f_in:
                    new_query = f_in.read()
                    try:
                        q["spec"]["query"]["spec"]["query"] = new_query
                        print(f"Injected: {q_path} into Panel {panel_id} (RefId: {ref_id})")
                        injected_count += 1
                    except KeyError:
                        print(f"Warning: Panel {panel_id} Query {ref_id} doesn't have the expected schema. Skipping.")

    print(f"\nInjection complete. {injected_count} elements updated.")
    save_json(out_path, dash)

def main():
    examples = """
Examples:
  1. Extract snippets from a dashboard into a folder:
     python dash_snip_helper.py extract metrics.json --outdir snippets_folder
     
     This will create files like:
       snippets_folder/panel_10_script.js
       snippets_folder/panel_11_query_A.flux
 
  2. Inject modified snippets back into the dashboard:
     python dash_snip_helper.py inject metrics.json --indir snippets_folder --out updated_metrics.json
     
     (You can overwrite the original by setting --out metrics.json)
"""
    parser = argparse.ArgumentParser(
        description="Grafana JSON Dashboard Snippet Helper\n\nExtracts and injects scripts/queries from/to Grafana dashboard JSON.",
        epilog=examples,
        formatter_class=RawDescriptionHelpFormatter
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands to run")
    subparsers.required = True

    # Extract command
    extract_parser = subparsers.add_parser("extract", help="Extract embedded scripts and queries to separate files.")
    extract_parser.add_argument("dashboard", help="Path to the JSON dashboard file (e.g. metrics.json)")
    extract_parser.add_argument("--outdir", default="extracted_snippets", help="Output directory for snippets")

    # Inject command
    inject_parser = subparsers.add_parser("inject", help="Inject modified scripts and queries back into a dashboard JSON.")
    inject_parser.add_argument("dashboard", help="Path to the source JSON dashboard file")
    inject_parser.add_argument("--indir", default="extracted_snippets", help="Input directory containing modified snippets")
    inject_parser.add_argument("--out", required=True, help="Path to save the updated JSON dashboard")

    args = parser.parse_args()

    if args.command == "extract":
        extract_snippets(args.dashboard, args.outdir)
    elif args.command == "inject":
        inject_snippets(args.dashboard, args.indir, args.out)

if __name__ == "__main__":
    main()
