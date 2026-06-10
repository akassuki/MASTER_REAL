import time
import serial
from lora_e32 import LoRaE32
from lora_e32_operation_constant import ResponseStatusCode


class ModbusLoRa:
    def __init__(self, port="/dev/ttyS0", baudrate=9600, model="433T30D", channel=18):
        self.port = port
        self.baudrate = baudrate
        self.model = model
        self.channel = channel
        self.ser = serial.Serial(self.port, baudrate=self.baudrate, timeout=1)
        self.lora = LoRaE32(self.model, self.ser)

    # =====================================================
    # INIT
    # =====================================================
    def begin(self):
        code = self.lora.begin()
        if code != ResponseStatusCode.SUCCESS:
            print(" LoRa init failed:", code)
            return False
        print("LoRa Modbus Master started")
        return True

    # =====================================================
    # CRC16 MODBUS RTU
    # =====================================================
    @staticmethod
    def modbus_crc(data):
        crc = 0xFFFF
        for pos in data:
            crc ^= pos
            for _ in range(8):
                if crc & 1:
                    crc = (crc >> 1) ^ 0xA001
                else:
                    crc >>= 1
        return crc

    # =====================================================
    # BUILD READ HOLDING REGISTERS (0x03)
    # =====================================================
    def build_modbus_read(self, slave, start, qty):
        frame = bytearray([
            slave,
            0x03,
            (start >> 8) & 0xFF,
            start & 0xFF,
            (qty >> 8) & 0xFF,
            qty & 0xFF
        ])
        crc = self.modbus_crc(frame)
        frame += bytes([crc & 0xFF, (crc >> 8) & 0xFF])
        return frame

    # =====================================================
    # BUILD WRITE SINGLE REGISTER (0x06)
    # =====================================================
    def build_modbus_write_single(self, slave, reg, value):
        frame = bytearray([
            slave,
            0x06,
            (reg >> 8) & 0xFF,
            reg & 0xFF,
            (value >> 8) & 0xFF,
            value & 0xFF
        ])
        crc = self.modbus_crc(frame)
        frame += bytes([crc & 0xFF, (crc >> 8) & 0xFF])
        return frame

    # =====================================================
    # BUILD WRITE MULTIPLE REGISTERS (0x10)
    # =====================================================
    def build_modbus_write_multi(self, slave, start, values):
        qty = len(values)
        frame = bytearray([
            slave,
            0x10,
            (start >> 8) & 0xFF,
            start & 0xFF,
            (qty >> 8) & 0xFF,
            qty & 0xFF,
            qty * 2
        ])

        for v in values:
            frame.append((v >> 8) & 0xFF)
            frame.append(v & 0xFF)

        crc = self.modbus_crc(frame)
        frame += bytes([crc & 0xFF, (crc >> 8) & 0xFF])
        return frame

    # =====================================================
    # SEND FRAME & WAIT RESPONSE
    # =====================================================
    def _send_and_wait(self, frame, dest_h, dest_l, timeout):
        msg = " ".join(f"{b:02X}" for b in frame)
        print(f"Send to 0x{dest_l:02X}: {msg}")

        result = self.lora.send_fixed_message(dest_h, dest_l, self.channel, msg)
        if result != ResponseStatusCode.SUCCESS:
            print(" Send failed:", result)
            return None

        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.lora.available() > 0:
                status, response = self.lora.receive_message()
                if status == ResponseStatusCode.SUCCESS:
                    print(f"ACK from 0x{dest_l:02X}: {response}")
                    return response
            time.sleep(0.1)

        print(" Timeout waiting response")
        return None

    # =====================================================
    # READ REGISTERS
    # =====================================================
    def read_registers(self, slave, start, qty, dest_h, dest_l, timeout=3):
        frame = self.build_modbus_read(slave, start, qty)
        resp = self._send_and_wait(frame, dest_h, dest_l, timeout)
        if not resp:
            return None

        try:
            parts = resp.strip().split()
            rx = [int(x, 16) for x in parts]
            if len(rx) < 5:
                return None

            byte_count = rx[2]
            data_bytes = rx[3:3 + byte_count]
            regs = [(data_bytes[i] << 8) | data_bytes[i + 1]
                    for i in range(0, len(data_bytes), 2)]

            return {
                "slave": rx[0],
                "func": rx[1],
                "registers": regs
            }
        except Exception as e:
            print("Invalid response:", resp, e)
            return None

    # =====================================================
    # WRITE SINGLE REGISTER
    # =====================================================
    def write_single_register(self, slave, reg, value, dest_h, dest_l, timeout=3):
        frame = self.build_modbus_write_single(slave, reg, value)
        return self._send_and_wait(frame, dest_h, dest_l, timeout) is not None

    # =====================================================
    # WRITE MULTIPLE REGISTERS
    # =====================================================
    def write_registers(self, slave, start, values, dest_h, dest_l, timeout=3):
        frame = self.build_modbus_write_multi(slave, start, values)
        return self._send_and_wait(frame, dest_h, dest_l, timeout) is not None

    # =====================================================
    # CLOSE
    # =====================================================
    def close(self):
        if self.ser and self.ser.is_open:
            self.ser.close()
