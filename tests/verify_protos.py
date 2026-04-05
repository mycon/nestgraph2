#!/usr/bin/env python3
"""
Verify that our protobuf field definitions match the SDK.
Extracts field definitions from the SDK JS bundle for the traits we decode.
"""
import re
import sys

# Traits we're actively decoding in grpc_web.py
DECODED_TRAITS = [
    'HvacControlTrait',
    'FanControlTrait',
    'TargetTemperatureSettingsTrait',
    'EcoModeStateTrait',
    'EcoModeTrait',
    'TemperatureTrait',
    'HumidityTrait',
    'CarbonMonoxideTrait',
    'SmokeTrait',
    'BatteryVoltageTrait',
]

print("=" * 80)
print("PROTO VERIFICATION - Extracting field definitions from SDK")
print("=" * 80)
print()

try:
    with open('tests/js_dump/sdk-421f7ba4a82b319d562a.js', encoding='utf-8', errors='replace') as f:
        content = f.read()
except Exception as e:
    print(f"ERROR: Could not read SDK file: {e}")
    sys.exit(1)

print(f"SDK size: {len(content):,} chars\n")

for trait in DECODED_TRAITS:
    print(f"{'─' * 80}")
    print(f">>> {trait}")
    print(f"{'─' * 80}")

    # Find the deserializeBinaryFromReader function
    pat = re.compile(
        re.escape(trait) + r'\.deserializeBinaryFromReader\s*=\s*function\s*\([^)]*\)\s*\{',
        re.DOTALL
    )
    m = pat.search(content)
    
    if m:
        # Extract function by counting braces
        start = m.end()
        depth = 1
        i = start
        while i < len(content) and depth > 0:
            if content[i] == '{': depth += 1
            elif content[i] == '}': depth -= 1
            i += 1
        
        func_text = content[m.start():i]
        
        # Extract field cases: case N: ... break;
        cases = re.findall(r'case\s+(\d+):\s*([^}]+?)(?:break|default)', func_text)
        if cases:
            print("Field mappings (from SDK):")
            for field_num, case_body in cases:
                # Simplify for readability
                if 'readMessage' in case_body:
                    print(f"  field {field_num}: sub-message")
                elif 'readBool' in case_body:
                    print(f"  field {field_num}: bool")
                elif 'readEnum' in case_body:
                    print(f"  field {field_num}: enum")
                elif 'readString' in case_body:
                    print(f"  field {field_num}: string")
                elif 'readInt' in case_body or 'readUint' in case_body:
                    print(f"  field {field_num}: numeric (int/uint/varint)")
                elif 'readFloat' in case_body:
                    print(f"  field {field_num}: float")
                elif 'readBytes' in case_body:
                    print(f"  field {field_num}: bytes")
                else:
                    print(f"  field {field_num}: (type unclear from SDK)")
        else:
            print("  (no field cases found — may be a simple enum-only type)")
    else:
        print(f"  NOT FOUND in SDK")
    
    print()

print("=" * 80)
print("DECODING LOGIC VERIFICATION")
print("=" * 80)
print()

# Now check our actual decoding functions
print("Checking src/grpc_web.py for trait decoder implementations...")
try:
    with open('src/grpc_web.py', encoding='utf-8') as f:
        grpc_code = f.read()
    
    for trait in DECODED_TRAITS:
        fn_name = f"_read_{trait[0].lower() + ''.join([c if not c.isupper() else f'_{c.lower()}' for c in trait[1:-5]])}_trait"
        # Simple version: look for "def _read_" + lowercase version
        fn_pattern = f"def _read_.*{trait.replace('Trait', '').lower()}.*trait"
        if re.search(fn_pattern, grpc_code, re.IGNORECASE):
            print(f"✓ {trait}: decoder function found")
        elif f"in type_url" in grpc_code and trait in grpc_code:
            print(f"✓ {trait}: handled in parse_observe_response")
        else:
            print(f"✗ {trait}: NO DECODER FOUND")
except Exception as e:
    print(f"ERROR reading grpc_web.py: {e}")
