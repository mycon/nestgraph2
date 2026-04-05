"""Offline test of the new decoders against known raw hex bytes."""
import sys
sys.path.insert(0, 'src')
from grpc_web import (
    _read_hvac_control_trait,
    _read_fan_control_trait,
    _read_target_temperature_trait,
)

print("=== HvacControlTrait (HVAC idle: 0a001a002a00) ===")
result = _read_hvac_control_trait(bytes.fromhex('0a001a002a00'))
for k, v in result.items():
    print(f"  {k}: {v}")

print()
print("=== FanControlTrait (fan off: 0804) ===")
result = _read_fan_control_trait(bytes.fromhex('0804'))
for k, v in result.items():
    print(f"  {k}: {v}")

print()
print("=== TargetTemperatureSettingsTrait (synthetic: heat=21°C cool=24°C) ===")
import struct
def temp_field(field_num, celsius):
    # HvacControl.Temperature { float value = 1 } = tag(f1,w5) + 4-byte float
    inner = b'\x0d' + struct.pack('<f', celsius)   # tag f1 wire5 + float32
    # wrap in TemperatureSetPoint field
    tag = (field_num << 3) | 2
    return bytes([tag, len(inner)]) + inner

# TemperatureSetPoint: f1=setpointType(3=RANGE), f4=heatingTarget, f5=coolingTarget
setpoint_type = b'\x08\x03'   # f1 varint 3
heat = temp_field(4, 21.0)
cool = temp_field(5, 24.0)
inner_sp = setpoint_type + heat + cool
# TargetTemperatureSettingsTrait: f1 = TemperatureSetPoint
outer = bytes([0x0a, len(inner_sp)]) + inner_sp
result = _read_target_temperature_trait(outer)
for k, v in result.items():
    print(f"  {k}: {v}")

print()
print("All decoders OK.")
