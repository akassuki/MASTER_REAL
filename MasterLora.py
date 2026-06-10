# -*- coding: utf-8 -*-
import json
import time
import struct
import threading
import paho.mqtt.client as mqtt
from datetime import datetime
from modbuslora import ModbusLoRa
import requests
import os

CONFIG_PATH = "/home/neit/Projects/Desktop/device_config.json"
OTA_SERVER_URL     = "http://14.224.150.7:5000/version.json"
OTA_CHECK_INTERVAL = 30.0
OTA_BINARY_PATH    = "/home/neit/Projects/CHUNK/chunked"
OTA_FIRMWARE_DIR   = "/home/neit/Projects/Desktop/ota_cache"
OTA_PUBLIC_KEY     = "./bin/public.pem"
OTA_LOCAL_JSON     = "./JSON/version.json"
MASTER_BIN         = "/home/neit/Projects/communication/master"

# ================= GLOBAL STATE =================
pending_cmd = {}
polling_enabled = True
current_device = None
config_data = None
config_last_modified = 0

device_mac_cache = {}

lock = threading.Lock()
polling_idle_event = threading.Event()
polling_idle_event.set()

# ================= CONFIG =================
MAX_REGISTERS_PER_READ = 7
MAX_REGISTERS_PER_WRITE = 4
DELAY_BETWEEN_CHUNKS = 0.3
DELAY_BETWEEN_WRITES = 0.6

# ================= UTILS =================
def load_config():
    global config_last_modified
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config_last_modified = os.path.getmtime(CONFIG_PATH)
        return json.load(f)

def check_config_changed():
    try:
        current_mtime = os.path.getmtime(CONFIG_PATH)
        return current_mtime != config_last_modified
    except:
        return False

def parse_int(v):
    return int(v, 0) if isinstance(v, str) else int(v)

def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def write_log(path, data):
    data["ts"] = now()
    with open(path, "a", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
        f.write("\n")

def get_active_cfg(cfg):
    global config_data
    return config_data if config_data else cfg

# ================= BUILD IUP LOOKUP =================
def build_iup_lookup(dev):
    lookup = {}
    for f in dev.get("read", {}).get("fields", []):
        if "iot_unit_point" in f:
            lookup[f["name"]] = f["iot_unit_point"]
    return lookup

# ================= AUTO SPLIT READS =================
def split_read_config(read_cfg):
    start = read_cfg["start"]
    qty = read_cfg["qty"]
    fields = read_cfg["fields"]

    if qty <= MAX_REGISTERS_PER_READ:
        return [read_cfg]

    chunks = []
    current_start = start
    remaining = qty

    while remaining > 0:
        chunk_size = min(MAX_REGISTERS_PER_READ, remaining)
        chunk_end = current_start + chunk_size

        chunk_fields = []
        for f in fields:
            field_index = f.get("index", 0)
            field_type = f.get("type", "u16")
            field_start = start + field_index
            if field_type in ("u32", "f32"):
                field_end = field_start + 2
            else:
                field_end = field_start + 1

            if field_start >= current_start and field_start < chunk_end:
                adjusted_field = f.copy()
                adjusted_field["index"] = field_start - current_start
                chunk_fields.append(adjusted_field)

        if chunk_fields:
            chunks.append({"start": current_start, "qty": chunk_size, "fields": chunk_fields})

        current_start += chunk_size
        remaining -= chunk_size

    print(f"[AUTO SPLIT READ] Split {qty} regs into {len(chunks)} chunks: {[c['qty'] for c in chunks]}")
    return chunks

# ================= AUTO SPLIT WRITES =================
def split_write_groups(write_groups):
    split_groups = []
    for group in write_groups:
        start_reg = group["start_reg"]
        values = group["values"]
        fields = group.get("fields", [])

        if len(values) <= MAX_REGISTERS_PER_WRITE:
            split_groups.append(group)
            continue

        num_values = len(values)
        chunk_start = 0
        while chunk_start < num_values:
            chunk_size = min(MAX_REGISTERS_PER_WRITE, num_values - chunk_start)
            chunk_end = chunk_start + chunk_size
            chunk_group = {
                "start_reg": start_reg + chunk_start,
                "values": values[chunk_start:chunk_end]
            }
            if fields:
                chunk_group["fields"] = fields[chunk_start:chunk_end]
            split_groups.append(chunk_group)
            chunk_start += chunk_size

    return split_groups

# ================= MAC ADDRESS =================
def read_mac_address(modbus, slave, dh, dl, timeout):
    try:
        res = modbus.read_registers(
            slave=slave, start=0, qty=6,
            dest_h=dh, dest_l=dl, timeout=timeout
        )
        if res and "registers" in res and len(res["registers"]) >= 6:
            return "".join([f"{b:02X}" for b in res["registers"][:6]])
        return None
    except Exception as e:
        print(f"[MAC READ ERROR] Slave 0x{slave:02X}: {e}")
        return None

def get_mac_each_poll(modbus, dev, timeout):
    dev_name = dev["name"]
    slave = parse_int(dev["slave_id"])
    dh = parse_int(dev["dest_h"])
    dl = parse_int(dev["dest_l"])
    mac = read_mac_address(modbus, slave, dh, dl, timeout)
    if mac:
        device_mac_cache[dev_name] = mac
        return mac
    return device_mac_cache.get(dev_name, "Unknown")

# ================= FIELD DECODE =================
def decode_fields(regs, fields):
    out = {}
    for f in fields:
        i = f.get("index", 0)
        t = f.get("type")
        scale = f.get("scale", 1)
        if i >= len(regs):
            continue
        if t == "u16":
            v = regs[i]
        elif t == "s16":
            r = regs[i]
            v = r - 65536 if r > 32767 else r
        elif t == "u32":
            if i + 1 >= len(regs):
                continue
            v = (regs[i] << 16) | regs[i + 1]
        elif t == "f32":
            if i + 1 >= len(regs):
                continue
            raw = (regs[i] << 16) | regs[i + 1]
            v = struct.unpack(">f", raw.to_bytes(4, "big"))[0]
        else:
            continue
        out[f["name"]] = round(v * scale, 3)
    return out

# ================= BUILD MQTT PAYLOAD =================
def build_mqtt_payload(dev_name, mac, data, iup_lookup):
    data_list = []
    for field_name, value in data.items():
        if field_name == "ID_Mac":
            continue
        entry = {"iot_unit_point": iup_lookup.get(field_name, ""), "name": field_name, "value": value}
        data_list.append(entry)

    return {
        "Device": dev_name,
        "ID_Mac": mac,
        "ts": now(),
        "data": data_list
    }

# ================= BUILD STATUS PAYLOAD =================
def build_status_payload(dev_name, mac, status, reason=None):
    payload = {
        "Device": dev_name,
        "ID_Mac": mac,
        "ts": now(),
        "status": status
    }
    if reason:
        payload["reason"] = reason
    return payload

# ================= PUBLISH STATUS =================
def publish_status(mqtt_client, dev, mac, status, reason=None):
    topic_status = dev.get("topic_status")
    if not topic_status:
        return
    payload = build_status_payload(dev["name"], mac, status, reason)
    mqtt_client.publish(topic_status, json.dumps(payload, ensure_ascii=False))
    print(f"  [STATUS] {dev['name']} -> {topic_status} : {status}" + (f" ({reason})" if reason else ""))

# ================= SEND ZEROS TO HMI =================
def send_zeros_to_hmi(modbus, display_cfg, dev_name):
    if "mappings" not in display_cfg:
        return

    slave = parse_int(display_cfg["slave_id"])
    dh = parse_int(display_cfg["dest_h"])
    dl = parse_int(display_cfg["dest_l"])
    mappings = display_cfg["mappings"]

    write_groups = []
    current_group = None

    for mapping in sorted(mappings, key=lambda x: x["hmi_reg"]):
        hmi_reg = mapping["hmi_reg"]
        if current_group is None:
            current_group = {"start_reg": hmi_reg, "values": [0], "count": 1}
        elif hmi_reg == current_group["start_reg"] + current_group["count"]:
            current_group["values"].append(0)
            current_group["count"] += 1
        else:
            write_groups.append(current_group)
            current_group = {"start_reg": hmi_reg, "values": [0], "count": 1}

    if current_group:
        write_groups.append(current_group)

    split_groups = split_write_groups(write_groups)
    if len(split_groups) > len(write_groups):
        print(f"[HMI ZERO SPLIT] {dev_name}: {len(write_groups)} groups -> {len(split_groups)} chunks")

    polling_idle_event.clear()
    try:
        for idx, group in enumerate(split_groups):
            modbus.write_registers(
                slave=slave, start=group["start_reg"],
                values=group["values"], dest_h=dh, dest_l=dl, timeout=3
            )
            print(f"[HMI ZERO] {dev_name} [{idx+1}/{len(split_groups)}] -> Reg[{group['start_reg']}] = {group['values']}")
            if idx < len(split_groups) - 1:
                time.sleep(DELAY_BETWEEN_WRITES)
    except Exception as e:
        print(f"[HMI ZERO ERROR] {dev_name}: {e}")
    finally:
        polling_idle_event.set()

# ================= FORWARD TO HMI =================
def forward_to_hmi(modbus, display_cfg, data, dev_name):
    if "mappings" not in display_cfg:
        print(f"[HMI] {dev_name}: No mappings defined")
        return

    slave = parse_int(display_cfg["slave_id"])
    dh = parse_int(display_cfg["dest_h"])
    dl = parse_int(display_cfg["dest_l"])
    mappings = display_cfg["mappings"]

    write_groups = []
    current_group = None

    for mapping in sorted(mappings, key=lambda x: x["hmi_reg"]):
        field_name = mapping["field"]
        hmi_reg = mapping["hmi_reg"]
        scale = mapping.get("scale", 1)

        if field_name not in data:
            continue

        scaled_value = int(data[field_name] * scale)

        if current_group is None:
            current_group = {"start_reg": hmi_reg, "values": [scaled_value], "fields": [field_name]}
        elif hmi_reg == current_group["start_reg"] + len(current_group["values"]):
            current_group["values"].append(scaled_value)
            current_group["fields"].append(field_name)
        else:
            write_groups.append(current_group)
            current_group = {"start_reg": hmi_reg, "values": [scaled_value], "fields": [field_name]}

    if current_group:
        write_groups.append(current_group)

    split_groups = split_write_groups(write_groups)
    if len(split_groups) > len(write_groups):
        print(f"[HMI WRITE SPLIT] {dev_name}: {len(write_groups)} groups -> {len(split_groups)} chunks")

    polling_idle_event.clear()
    try:
        for idx, group in enumerate(split_groups):
            modbus.write_registers(
                slave=slave, start=group["start_reg"],
                values=group["values"], dest_h=dh, dest_l=dl, timeout=3
            )
            fields_str = f"({group['fields']})" if "fields" in group else ""
            print(f"[HMI WRITE] {dev_name} [{idx+1}/{len(split_groups)}] -> Reg[{group['start_reg']}] = {group['values']} {fields_str}")
            if idx < len(split_groups) - 1:
                time.sleep(DELAY_BETWEEN_WRITES)
    except Exception as e:
        print(f"[HMI WRITE ERROR] {dev_name}: {e}")
    finally:
        polling_idle_event.set()

# ================= MQTT =================
def on_mqtt_message(client, userdata, msg):
    """
    Xử lý message từ topic_control riêng của từng device có control config.

    Payload adjust_param:
        {"group": "adjust_param", "value": 80, "iot_device": "IOD_1191", "device": "DEV_147"}
        -> Ghi value * 100 vào freq_reg (forward speed/param)

    Payload change_state:
        {"group": "change_state", "action": 1, "iot_device": "IOD_1191", "device": "DEV_147"}
        -> Ghi action (1=bật / 0=tắt) vào cmd_reg
    """
    try:
        active = get_active_cfg(userdata["cfg"])

        # Tìm device theo topic nhận được
        dev = None
        for d in active["devices"]:
            if d.get("topic_control") == msg.topic:
                dev = d
                break

        if dev is None:
            print(f"[MQTT] No device matched topic: {msg.topic}")
            return

        payload = json.loads(msg.payload.decode())
        group = payload.get("group")

        if group not in ("adjust_param", "change_state"):
            print(f"[MQTT CTRL] Unknown group: '{group}' from {msg.topic}")
            return

        dev_name = dev["name"]

        with lock:
            pending_cmd[dev_name] = payload
            polling_enabled = False

        print(f"[MQTT CMD] topic={msg.topic} device={dev_name} group={group} payload={payload}")

    except Exception as e:
        print("[MQTT ERROR]", e)


def connect_mqtt(cfg, devices):
    c = mqtt.Client(userdata={"cfg": cfg})
    if cfg.get("username"):
        c.username_pw_set(cfg["username"], cfg["password"])
    c.on_message = on_mqtt_message
    c.connect(cfg["host"], cfg["port"], 60)

    # Subscribe topic_control riêng của từng device có control config
    subscribed = []
    for dev in devices:
        if "control" not in dev:
            continue
        topic_control = dev.get("topic_control")
        if topic_control:
            c.subscribe(topic_control)
            subscribed.append(topic_control)
            print(f"[MQTT] Subscribed control topic: {topic_control} (device: {dev['name']})")

    if not subscribed:
        print("[MQTT] WARNING: No device has topic_control defined")

    c.loop_start()
    print("[MQTT] connected")
    return c


# ================= CONFIG MONITOR THREAD =================
def config_monitor_loop(mqtt_client):
    global config_data

    # Track subscribed control topics to avoid duplicate subscriptions
    subscribed_control_topics = set()

    while True:
        try:
            if check_config_changed():
                print("\n" + "=" * 60)
                print("CONFIG FILE CHANGED - RELOADING...")
                print("=" * 60)

                new_config = load_config()

                with lock:
                    config_data = new_config

                # Re-subscribe topic_control mới của từng device có control
                for dev in new_config["devices"]:
                    if "control" not in dev:
                        continue
                    topic = dev.get("topic_control")
                    if topic and topic not in subscribed_control_topics:
                        mqtt_client.subscribe(topic)
                        subscribed_control_topics.add(topic)
                        print(f"  [MQTT SUB] topic_control: {topic} (device: {dev['name']})")

                print(f"Config reloaded: {len(config_data['devices'])} devices")
                print("=" * 60 + "\n")

        except Exception as e:
            print(f"[CONFIG MONITOR ERROR] {e}")

        time.sleep(2)


# ================= POLLING THREAD =================
def polling_loop(cfg, modbus, mqtt_client):
    global current_device, polling_enabled

    dev_index = 0

    while True:
        active = get_active_cfg(cfg)

        devices = active["devices"]
        log_file = active["LogFile"]
        response_timeout = active["ResponseTimeout"]

        total_cycle_ms = active.get("PollInterval", 300)
        per_device_sleep = max(total_cycle_ms / max(len(devices), 1), 1) / 1000.0

        with lock:
            if not polling_enabled:
                time.sleep(0.05)
                continue
            dev = devices[dev_index]
            current_device = dev["name"]

        slave = parse_int(dev["slave_id"])
        dh = parse_int(dev["dest_h"])
        dl = parse_int(dev["dest_l"])
        r = dev["read"]

        iup_lookup = build_iup_lookup(dev)
        dev_topic = dev.get("topic")

        read_chunks = split_read_config(r)

        print(f"\n[POLL START] {dev['name']} (Slave 0x{slave:02X}) -> {dev_topic}")

        polling_idle_event.clear()

        # 1) Read MAC
        mac = get_mac_each_poll(modbus, dev, response_timeout)

        # 2) Read data
        all_data = {}
        success = False
        failed_chunks = []

        for chunk_idx, chunk in enumerate(read_chunks):
            try:
                res = modbus.read_registers(
                    slave=slave, start=chunk["start"], qty=chunk["qty"],
                    dest_h=dh, dest_l=dl, timeout=response_timeout
                )
                if res and "registers" in res:
                    chunk_data = decode_fields(res["registers"], chunk["fields"])
                    all_data.update(chunk_data)
                    success = True
                    print(f"  [Chunk {chunk_idx+1}/{len(read_chunks)}] Regs[{chunk['start']}:{chunk['start']+chunk['qty']-1}] -> {len(chunk_data)} fields OK")
                else:
                    failed_chunks.append(chunk_idx + 1)
                    print(f"  [Chunk {chunk_idx+1}/{len(read_chunks)}] NO RESPONSE")

                if chunk_idx < len(read_chunks) - 1:
                    time.sleep(DELAY_BETWEEN_CHUNKS)

            except Exception as e:
                failed_chunks.append(chunk_idx + 1)
                print(f"  [Chunk {chunk_idx+1}/{len(read_chunks)}] ERROR - {e}")

        polling_idle_event.set()

        if success and all_data:
            payload = build_mqtt_payload(dev["name"], mac, all_data, iup_lookup)
            print(f"  [MERGED PACKET] {len(all_data)} fields:")
            print(f"  {json.dumps(payload, ensure_ascii=False)}")

            if failed_chunks:
                print(f"  [Warning] Chunks {failed_chunks} failed, partial data sent")

            if dev_topic:
                mqtt_client.publish(dev_topic, json.dumps(payload, ensure_ascii=False))
                print(f"  [OK] MQTT Published -> {dev_topic}")

            write_log(log_file, payload)
            publish_status(mqtt_client, dev, mac, "online")

            if "display_target" in dev:
                forward_to_hmi(modbus, dev["display_target"], all_data, dev["name"])
        else:
            print(f"  [FAILED] No data from {dev['name']} (MAC: {mac})")
            publish_status(mqtt_client, dev, mac, "offline", reason="no_response")

            if "display_target" in dev:
                send_zeros_to_hmi(modbus, dev["display_target"], dev["name"])

        print("=" * 60)

        dev_index = (dev_index + 1) % len(devices)
        time.sleep(per_device_sleep)


# ================= CONTROL THREAD =================
def control_loop(cfg, modbus):
    """
    Xử lý lệnh điều khiển từ pending_cmd.

    group = "adjust_param":
        - Ghi value * 100 vào freq_reg
        - Ý nghĩa: thay đổi tần số/tốc độ (ví dụ: 50 Hz -> ghi 5000)

    group = "change_state":
        - action = 1 -> Ghi 1 vào cmd_reg (bật)
        - action = 0 -> Ghi 0 vào cmd_reg (tắt)
    """
    global polling_enabled

    while True:
        with lock:
            if not pending_cmd:
                time.sleep(0.05)
                continue
            dev_name, cmd = pending_cmd.popitem()
            polling_enabled = False

        # Chờ polling đang chạy dở kết thúc
        polling_idle_event.wait(timeout=2.0)

        active = get_active_cfg(cfg)
        devices = active["devices"]
        response_timeout = active["ResponseTimeout"]

        try:
            dev = next(d for d in devices if d["name"] == dev_name)
        except StopIteration:
            print(f"[CTRL] Device '{dev_name}' not found in config")
            with lock:
                polling_enabled = True
            continue

        ctrl = dev.get("control")
        if not ctrl:
            print(f"[CTRL] Device '{dev_name}' has no control config")
            with lock:
                polling_enabled = True
            continue

        slave = parse_int(dev["slave_id"])
        dh = parse_int(dev["dest_h"])
        dl = parse_int(dev["dest_l"])

        group = cmd.get("group")
        cmd_reg  = ctrl["cmd_reg"]
        freq_reg = ctrl["freq_reg"]

        polling_idle_event.clear()
        try:
            if group == "adjust_param":
                # Bước 1: ghi value * 100 vào freq_reg
                raw_value = int(float(cmd.get("value", 0)) * 100)
                modbus.write_registers(
                    slave=slave,
                    start=freq_reg,
                    values=[raw_value],
                    dest_h=dh, dest_l=dl,
                    timeout=response_timeout
                )
                print(f"[CTRL] {dev_name}: adjust_param -> Reg[{freq_reg}] = {raw_value} (value={cmd.get('value')} x100)")
                time.sleep(DELAY_BETWEEN_WRITES)

                # Bước 2: ghi cmd_reg = 2 (FORWARD) để áp dụng thông số mới
                modbus.write_registers(
                    slave=slave,
                    start=cmd_reg,
                    values=[2],
                    dest_h=dh, dest_l=dl,
                    timeout=response_timeout
                )
                print(f"[CTRL] {dev_name}: adjust_param -> Reg[{cmd_reg}] = 2 (FORWARD)")

            elif group == "change_state":
                action = int(cmd.get("action", 0))
                if action not in (0, 1):
                    print(f"[CTRL] {dev_name}: Invalid action value '{action}', expected 0 or 1")
                else:
                    # action=1 (bật)  -> cmd_reg = 2 (FORWARD)
                    # action=0 (tắt)  -> cmd_reg = 1 (STOP)
                    cmd_val  = 2 if action == 1 else 1
                    state_str = "ON (FORWARD)" if action == 1 else "OFF (STOP)"
                    modbus.write_registers(
                        slave=slave,
                        start=cmd_reg,
                        values=[cmd_val],
                        dest_h=dh, dest_l=dl,
                        timeout=response_timeout
                    )
                    print(f"[CTRL] {dev_name}: change_state -> Reg[{cmd_reg}] = {cmd_val} ({state_str})")

            else:
                print(f"[CTRL] {dev_name}: Unknown group '{group}'")

        except Exception as e:
            print(f"[CTRL ERROR] {dev_name}: {e}")
        finally:
            polling_idle_event.set()

        with lock:
            polling_enabled = True

def ota_parse_ver(v: str):
    try:
        parts = tuple(int(x) for x in v.strip().split("."))
        return parts if len(parts) == 3 else (0, 0, 0)
    except Exception:
        return (0, 0, 0)

 
def ota_load_local_versions():
    """Đọc version.json local → dict {node_name: version}"""
    try:
        with open(OTA_LOCAL_JSON, "r", encoding="utf-8") as f:
            entries = json.load(f)
        if isinstance(entries, dict):
            entries = [entries]
        return {e["node"]: e["version"] for e in entries if "node" in e and "version" in e}
    except FileNotFoundError:
        return {}
    except Exception as e:
        return {}

# ================= OTA LOOP =================
def ota_update_loop():
    """
    Luồng kiểm tra OTA mỗi OTA_CHECK_INTERVAL giây.
    - GET version.json từ server
    - So sánh từng node với local JSON
    - Nếu server mới hơn → log ra, sẵn sàng xử lý tiếp (download/verify/apply)
    """
    print("[OTA] Thread bắt đầu")

    while True:
        try:
            print(f"[OTA] Đang kiểm tra: {OTA_SERVER_URL}")
            resp = requests.get(OTA_SERVER_URL, timeout=10)
            resp.raise_for_status()

            data = resp.json()

            # Hỗ trợ cả dict (1 node) lẫn list (nhiều node)
            if isinstance(data, dict):
                server_entries = [data]
            elif isinstance(data, list):
                server_entries = data
            else:
                print(f"[OTA] Format không hợp lệ: {type(data)}")
                time.sleep(OTA_CHECK_INTERVAL)
                continue

            local_versions = ota_load_local_versions()

            for entry in server_entries:
                if not isinstance(entry, dict):
                    continue

                node    = entry.get("node", "")
                version = entry.get("version", "")

                if not node or not version:
                    print(f"[OTA] Entry thiếu node/version: {entry}")
                    continue

                local_ver  = local_versions.get(node, "0.0.0")
                server_ver = version

                if ota_parse_ver(server_ver) > ota_parse_ver(local_ver):
                    print(f"[OTA] [{node}] Có bản mới: local={local_ver} → server={server_ver}")
                    # TODO: gọi download + verify + apply ở đây
                    firmware_file = entry.get("firmware","")
                    signature_file = entry.get("signature","")

                    if firmware_file:
                        try:
                            if firmware_file.startswith("http"):
                                fw_url = firmware_file
                            else:
                                base_url = OTA_SERVER_URL.rsplit("/", 1)[0]
                                fw_url = f"{base_url}/{firmware_file}"
                            local_fw_name = firmware_file.rsplit("/", 1)[-1]
                            fw_path = os.path.join(OTA_FIRMWARE_DIR, f"{node}_{server_ver}_{local_fw_name}")
                            resp = requests.get(fw_url, timeout=20,verify = False)
                            resp.raise_for_status()
                            with open(fw_path, "wb") as f:
                                f.write(resp.content)
                        except Exception as e:
                            print(f"[OTA] Lỗi tải firmware cho {node}: {e}")
                            continue
                    if signature_file:
                        try:
                            if signature_file.startswith("http"):
                                sig_url = signature_file
                            else:
                                base_url = OTA_SERVER_URL.rsplit("/", 1)[0]
                                sig_url = f"{base_url}/{signature_file}"
                            local_sig_name = signature_file.rsplit("/", 1)[-1]
                            sig_path = os.path.join(OTA_FIRMWARE_DIR, f"{node}_{server_ver}_{local_sig_name}")
                            resp = requests.get(sig_url, timeout=20,verify = False)
                            resp.raise_for_status()
                            with open(sig_path, "wb") as f:
                                f.write(resp.content)
                        except Exception as e:
                            print(f"[OTA] Lỗi tải signature cho {node}: {e}")
                            continue
                else:
                    print(f"[OTA] [{node}] Up-to-date (local={local_ver} >= server={server_ver})")

        except requests.exceptions.ConnectionError:
            print("[OTA] Không kết nối được server")
        except requests.exceptions.Timeout:
            print("[OTA] Server timeout")
        except Exception as e:
            print(f"[OTA] Lỗi: {e}")

        time.sleep(OTA_CHECK_INTERVAL)

# ================= MAIN =================
def main():
    global config_data

    print("=" * 60)
    print("   MASTER MODBUS LORA - AUTO SPLIT READS & WRITES")
    print("=" * 60)

    cfg = load_config()
    config_data = cfg
    print(f"[CONFIG] Loaded {len(cfg['devices'])} devices")
    print(f"[CONFIG] Max registers per read:  {MAX_REGISTERS_PER_READ}")
    print(f"[CONFIG] Max registers per write: {MAX_REGISTERS_PER_WRITE}")
    print(f"[CONFIG] Delay between read chunks:  {DELAY_BETWEEN_CHUNKS}s")
    print(f"[CONFIG] Delay between write chunks: {DELAY_BETWEEN_WRITES}s")

    # Liệt kê device có control
    ctrl_devices = [d["name"] for d in cfg["devices"] if "control" in d]
    print(f"[CONFIG] Controllable devices ({len(ctrl_devices)}): {ctrl_devices}")

    mqtt_client = connect_mqtt(cfg["mqtt"], cfg["devices"])

    lora = cfg["lora"]
    modbus = ModbusLoRa(
        port=lora["port"],
        baudrate=lora["baudrate"],
        model=lora["model"],
        channel=lora["channel"]
    )

    if not modbus.begin():
        print("[ERROR] LoRa init failed")
        return

    print("[MASTER] started")
    print("=" * 60)

    threading.Thread(target=config_monitor_loop, args=(mqtt_client,), daemon=True).start()
    threading.Thread(target=polling_loop,         args=(cfg, modbus, mqtt_client), daemon=True).start()
    threading.Thread(target=control_loop,         args=(cfg, modbus), daemon=True).start()
    threading.Thread(target=ota_update_loop,      args=(cfg, modbus), daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[MASTER] Shutting down...")


if __name__ == "__main__":
    main()
