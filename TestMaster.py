import serial
import time
from lora_e32 import LoRaE32
from lora_e32_operation_constant import ResponseStatusCode

# ================= CONFIG =================
PORT = "/dev/ttyUSB0"
BAUD = 9600
MODULE = "433T20D"

# ================= INIT =================
ser = serial.Serial(PORT, BAUD, timeout=1)
lora = LoRaE32(MODULE, ser)

code = lora.begin()
if code != ResponseStatusCode.SUCCESS:
    print("? LoRa init failed:", ResponseStatusCode.get_description(code))
    exit()

print(" LoRa LISTEN MODE - Pi4 Ready")

# ================= LISTEN LOOP =================
try:
    while True:
        if lora.available() > 0:
            code, data = lora.receive_message()
            if code == ResponseStatusCode.SUCCESS:
                msg = data.strip()
                print(" RX:", msg)
            else:
                print(" RX error:", ResponseStatusCode.get_description(code))

        time.sleep(0.05)

except KeyboardInterrupt:
    print("\n Stop")

finally:
    ser.close()
