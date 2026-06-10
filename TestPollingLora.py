# -*- coding: utf-8 -*-

import serial
import time
from lora_e32 import LoRaE32
from lora_e32_operation_constant import ResponseStatusCode

# ================= CONFIG =================
PORT = "/dev/ttyUSB0"
BAUD = 9600
MODULE = "433T20D"

MASTER_ADDH = 0x00
MASTER_ADDL = 0x01
CHANNEL     = 15

SLAVE_ADDH  = 0x00
SLAVE_ADDL  = 0x03
SLAVE_ID    = "SLAVE_03"

POLL_INTERVAL    = 5    # seconds
RESPONSE_TIMEOUT = 2    # seconds

# ================= INIT =================
ser = serial.Serial(PORT, BAUD, timeout=0.2)
lora = LoRaE32(MODULE, ser)

code = lora.begin()
if code != ResponseStatusCode.SUCCESS:
    print("LoRa init failed:",
          ResponseStatusCode.get_description(code))
    exit(1)

print("MASTER READY - POLLING MODE")

# ================= POLLING FUNCTION =================
def poll_slave():
    poll_cmd = "POLL|" + SLAVE_ID
    print("SEND POLL:", poll_cmd)

    code = lora.send_fixed_message(
        SLAVE_ADDH,
        SLAVE_ADDL,
        CHANNEL,
        poll_cmd
    )

    if code != ResponseStatusCode.SUCCESS:
        print("Poll send error:",
              ResponseStatusCode.get_description(code))
        return

    start_time = time.time()
    while time.time() - start_time < RESPONSE_TIMEOUT:
        if lora.available() > 0:
            code, data = lora.receive_message()

            if code == ResponseStatusCode.SUCCESS:
                msg = data.strip()
                print("RX DATA:", msg)
                return
            else:
                print("RX error:",
                      ResponseStatusCode.get_description(code))

        time.sleep(0.05)

    print("Timeout - no response")

# ================= MAIN LOOP =================
try:
    while True:
        poll_slave()
        print("----------------------------------------")
        time.sleep(POLL_INTERVAL)

except KeyboardInterrupt:
    print("\nStop polling")

finally:
    ser.close()
