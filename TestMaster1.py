import serial
import time
from lora_e32 import LoRaE32
from lora_e32_operation_constant import ResponseStatusCode

# ===== CONFIG =====
PORT = "/dev/ttyUSB0"
BAUD = 9600
MODULE = "433T20D"

# ===== INIT =====
ser = serial.Serial(PORT, BAUD, timeout=1)
lora = LoRaE32(MODULE, ser)

code = lora.begin()
if code != ResponseStatusCode.SUCCESS:
    print("❌ Init failed")
    exit()

print("🚀 START SIMPLE SENDER")

# ===== LOOP =====
try:
    counter = 0

    while True:
        msg = f"HELLO_{counter}\n"   # 🔥 luôn < 58 byte

        status = lora.send_transparent_message(msg)

        if status == ResponseStatusCode.SUCCESS:
            print("📤 Sent:", msg.strip())
        else:
            print("❌ Send error:", ResponseStatusCode.get_description(status))

        counter += 1
        time.sleep(1)

except KeyboardInterrupt:
    print("🛑 Stop")

finally:
    ser.close()
