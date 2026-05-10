from flask import Flask, jsonify, send_from_directory
import re
from datetime import datetime
from collections import defaultdict
import threading
import time
import paramiko
import os

app = Flask(__name__)

# ═══════════════════════════════════════════════════════════════
#  SERVER CONFIGS
# ═══════════════════════════════════════════════════════════════

SERVER_CONFIG = {
    'host': '192.168.61.202',
    'port': 22,
    'user': 'dwhadmin',
    'password': 'dwhadmin',
    'base_path': '/data02/scripts/process',
    'cdr_base_path': '/data02/cbs_cdrs',
    'registration_path': '/data02/scripts/process/bin/file_registration/reg_to_process',
    'ipdr_path': '/data02/scripts/dwh/file_move',
    'loading_path': '/data02/scripts/process/bin/loading_l1/temp_etl'
}

SERVER_CONFIG_204 = {
    'host': '192.168.61.204',
    'port': 22,
    'user': 'dwhadmin',
    'password': 'dwhadmin',
}

# ═══════════════════════════════════════════════════════════════
#  202 — Log file definitions
# ═══════════════════════════════════════════════════════════════

IPDR_LOG_FILE = 'ipdr_transfer_tms_v1.log'

L1_LOADING_LOG_FILES = {
    'voice': 'esms_cbs01_loader_voice_test.log',
    'data': 'esms_cbs01_loader_data_test.log',
    'sms': 'esms_cbs01_loader_sms_test.log',
    'mon': 'esms_cbs01_loader_mon_test.log',
    'com': 'esms_cbs01_loader_com_test.log',
    'cm': 'esms_cbs01_loader_cm_test.log',
    'transfer': 'esms_cbs01_loader_transfer_test.log',
    'vou': 'esms_cbs01_loader_vou_test.log',
    'adj': 'esms_cbs01_loader_adj_test.log'
}

LOG_FILES = {
    'voice': 'registration_voice_procc.log',
    'data': 'registration_data_procc.log',
    'sms': 'registration_sms_procc.log',
    'transfer': 'registration_transfer_procc.log',
    'vou': 'registration_vou_procc.log',
    'adj': 'registration_adj_procc.log',
    'com': 'registration_com_procc.log',
    'mon': 'registration_mon_procc.log',
    'cm': 'registration_cm_procc.log'
}

CDR_CATEGORIES = ['adj', 'cm', 'com', 'data', 'mon', 'sms', 'transfer', 'voice', 'vou']

MERGE_LOG_FILES = {
    'voice': 'cdr_file_merger_voice_test.log',
    'data': 'cdr_file_merger_data_test.log',
    'sms': 'cdr_file_merger_sms_test.log',
    'transfer': 'cdr_file_merger_transfer_test.log',
    'vou': 'cdr_file_merger_vou_test.log',
    'adj': 'cdr_file_merger_adj_test.log',
    'com': 'cdr_file_merger_com_test.log',
    'mon': 'cdr_file_merger_mon_test.log',
    'cm': 'cdr_file_merger_cm_test.log'
}

REGISTRATION_LOG_FILES = {
    'voice': 'reg_proc_voice_test_v1.log',
    'data': 'reg_proc_data_test_v1.log',
    'sms': 'reg_proc_sms_test_v1.log',
    'transfer': 'reg_proc_transfer_test_v1.log',
    'vou': 'reg_proc_vou_test_v1.log',
    'adj': 'reg_proc_adj_test_v1.log',
    'com': 'reg_proc_com_test_v1.log',
    'mon': 'reg_proc_mon_test_v1.log',
    'cm': 'reg_proc_cm_test_v1.log'
}

# ═══════════════════════════════════════════════════════════════
#  204 — Log file definitions
# ═══════════════════════════════════════════════════════════════

MSC_REGISTRATION_LOG_FILES = {
    'msc_nokia':  '/data02/scripts/process/bin/file_registration/nk_msc_cdr_reg_proc_test_v1.log',
    'msc_huawei': '/data02/scripts/process/bin/file_registration/hw_msc_cdr_reg_proc_test_v1.log',
}

MSC_MERGE_LOG_FILES = {
    'msc_huawei': '/data02/scripts/process/bin/file_merge/hw_msc_cdr_merge_test_v1.log',
}

MSC_L1_LOADING_LOG_FILES = {
    'msc_nokia':  '/data02/scripts/process/bin/loading_l1/msc_nokia/nk_msc_cdr_loader_test_v1.log',
    'msc_huawei': '/data02/scripts/process/bin/loading_l1/msc_huawei/hw_msc_cdr_loader_test_v1.log',
}

MSC_DELETE_LOG_FILES = {
    'msc_nokia': '/data02/scripts/process/bin/delete_cdr/nk_msc_procd_cdr_del_test_v1.log',
}

# ═══════════════════════════════════════════════════════════════
#  In-memory stores
# ═══════════════════════════════════════════════════════════════

merge_logs = defaultdict(list)
registration_logs = defaultdict(dict)
ipdr_logs = []
l1_loading_logs = defaultdict(list)
file_counts = {}

msc_registration_logs = defaultdict(list)
msc_merge_logs = defaultdict(list)
msc_l1_loading_logs = defaultdict(list)
msc_delete_logs = defaultdict(list)
msc_file_counts = {}          # real file counts from sftp_msc dirs on 204

merge_lock = threading.Lock()
registration_lock = threading.Lock()
ipdr_lock = threading.Lock()
l1_loading_lock = threading.Lock()
count_lock = threading.Lock()

msc_registration_lock = threading.Lock()
msc_merge_lock = threading.Lock()
msc_l1_loading_lock = threading.Lock()
msc_delete_lock = threading.Lock()
msc_count_lock = threading.Lock()

connection_status = {'connected': False, 'last_error': None, 'last_update': None}

# ═══════════════════════════════════════════════════════════════
#  SSH helpers
# ═══════════════════════════════════════════════════════════════

def get_ssh_client():
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=SERVER_CONFIG['host'],
            port=SERVER_CONFIG['port'],
            username=SERVER_CONFIG['user'],
            password=SERVER_CONFIG['password'],
            timeout=10
        )
        return client
    except Exception as e:
        print(f"SSH Connection Error (202): {e}")
        connection_status['connected'] = False
        connection_status['last_error'] = str(e)
        return None


def get_ssh_client_204():
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=SERVER_CONFIG_204['host'],
            port=SERVER_CONFIG_204['port'],
            username=SERVER_CONFIG_204['user'],
            password=SERVER_CONFIG_204['password'],
            timeout=10
        )
        return client
    except Exception as e:
        print(f"SSH Connection Error (204): {e}")
        return None


def remote_tail_204(client, filepath, lines=300):
    command = f"tail -n {lines} {filepath} 2>/dev/null || echo 'FILE_NOT_FOUND'"
    stdin, stdout, stderr = client.exec_command(command)
    stdout.channel.recv_exit_status()
    output = stdout.read().decode('utf-8', errors='ignore')
    if 'FILE_NOT_FOUND' in output:
        print(f"[204] File not found: {filepath}")
        return []
    return [l.strip() for l in output.strip().split('\n') if l.strip()]

# ═══════════════════════════════════════════════════════════════
#  202 — Parsers
# ═══════════════════════════════════════════════════════════════

def parse_log_entry(line):
    try:
        if 'moving from' in line.lower():
            file_match = re.search(r'for ([\w_]+\.add)', line)
            time_match = re.search(r'(\w{3}\s+\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\w+\s+\d{4})', line)
            if file_match and time_match:
                return {'type': 'moving', 'file': file_match.group(1), 'timestamp': time_match.group(1), 'action': '📦 Moving to process_dir', 'raw_time': time_match.group(1)}
        elif 'file registration end' in line.lower():
            file_match = re.search(r'for ([\w_]+)\s+\w{3}', line)
            time_match = re.search(r'(\w{3}\s+\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\w+\s+\d{4})', line)
            if file_match and time_match:
                return {'type': 'completed', 'file': file_match.group(1), 'timestamp': time_match.group(1), 'action': '✅ Registration Complete', 'raw_time': time_match.group(1)}
        return None
    except Exception as e:
        print(f"Error parsing line: {e}")
        return None


def read_remote_log(client, log_file, lines=200):
    try:
        file_path = f"{SERVER_CONFIG['base_path']}/{log_file}"
        command = f"tail -n {lines} {file_path} 2>/dev/null || echo 'FILE_NOT_FOUND'"
        stdin, stdout, stderr = client.exec_command(command)
        output = stdout.read().decode('utf-8', errors='ignore')
        if 'FILE_NOT_FOUND' in output:
            return []
        lines_list = output.strip().split('\n')
        file_status_map = {}
        file_order = {}
        order_counter = 0
        for line in lines_list:
            line = line.strip()
            if not line:
                continue
            if line.endswith('.add') and 'moving from' not in line.lower():
                filename = line.split()[-1] if ' ' in line else line
                if filename.endswith('.add') and filename not in file_order:
                    file_order[filename] = order_counter
                    order_counter += 1
                    time_match = re.search(r'_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})_', filename)
                    if time_match:
                        year, month, day, hour, minute, second = time_match.groups()
                        file_time = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
                        now = datetime.now()
                        pending_duration = now - file_time
                        total_seconds = int(pending_duration.total_seconds())
                        hours = total_seconds // 3600
                        minutes = (total_seconds % 3600) // 60
                        seconds = total_seconds % 60
                        if hours > 0:
                            pending_time = f"{hours}h {minutes}m {seconds}s"
                        elif minutes > 0:
                            pending_time = f"{minutes}m {seconds}s"
                        else:
                            pending_time = f"{seconds}s"
                    else:
                        pending_time = "Unknown"
                    file_status_map[filename] = {'type': 'incomplete', 'file': filename, 'timestamp': f"Pending: {pending_time}", 'status': 'Registration Incomplete', 'order': file_order[filename]}
        for i, line in enumerate(lines_list):
            line = line.strip()
            if not line:
                continue
            if 'file registration end' in line.lower():
                file_match = re.search(r'for ([\w_]+)', line)
                if file_match:
                    filename_base = file_match.group(1)
                    time_match = re.search(r'(\w{3}\s+\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\w+\s+\d{4})', line)
                    timestamp = time_match.group(1) if time_match else 'N/A'
                    filename_with_ext = filename_base + '.add'
                    if filename_with_ext in file_status_map:
                        file_status_map[filename_with_ext].update({'type': 'completed', 'timestamp': timestamp, 'status': 'Registration Complete'})
                    else:
                        if filename_with_ext not in file_order:
                            file_order[filename_with_ext] = order_counter
                            order_counter += 1
                        file_status_map[filename_with_ext] = {'type': 'completed', 'file': filename_with_ext, 'timestamp': timestamp, 'status': 'Registration Complete', 'order': file_order[filename_with_ext]}
        parsed_entries = sorted(file_status_map.values(), key=lambda x: x['order'])
        return parsed_entries[-10:] if len(parsed_entries) > 10 else parsed_entries
    except Exception as e:
        print(f"Error reading remote log {log_file}: {e}")
        return []


def read_merge_log(client, log_file, lines=300):
    try:
        file_path = f"/data02/scripts/process/bin/file_merge/{log_file}"
        command = f"tail -n {lines} {file_path} 2>/dev/null || echo 'FILE_NOT_FOUND'"
        stdin, stdout, stderr = client.exec_command(command)
        output = stdout.read().decode('utf-8', errors='ignore')
        if 'FILE_NOT_FOUND' in output:
            return []
        lines_list = output.strip().split('\n')
        merge_entries = []
        for line in lines_list:
            line = line.strip()
            if 'Total' in line and ('merged' in line or 'merge' in line):
                try:
                    count_match = re.search(r'Total (?:files:? )?(\d+)', line) or re.search(r'Total (\d+)', line)
                    file_match = re.search(r'(?:merged (?:in|to file):?\s+)([\w_]+\.add)', line)
                    size_match = re.search(r'Size: ([\d.]+[KMG]?)', line)
                    time_match = re.search(r'time: (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', line)
                    if count_match and file_match:
                        merge_entries.append({'count': int(count_match.group(1)), 'file': file_match.group(1), 'size': size_match.group(1) if size_match else 'N/A', 'time': time_match.group(1) if time_match else 'N/A', 'status': 'Merge'})
                except Exception as e:
                    print(f"[MERGE ERROR] {e}")
                    continue
            elif 'No cdr files' in line and 'at time:' in line:
                try:
                    time_match = re.search(r'time: (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', line)
                    if time_match:
                        merge_entries.append({'count': 0, 'file': 'No file', 'size': 'N/A', 'time': time_match.group(1), 'status': 'No file for merge'})
                except Exception as e:
                    print(f"[MERGE ERROR] {e}")
                    continue
        return merge_entries[-10:] if len(merge_entries) > 10 else merge_entries
    except Exception as e:
        print(f"[MERGE ERROR] Reading {log_file}: {e}")
        return []


def read_registration_log(client, log_file, lines=300):
    try:
        file_path = f"{SERVER_CONFIG['registration_path']}/{log_file}"
        command = f"tail -n {lines} {file_path} 2>/dev/null || echo 'FILE_NOT_FOUND'"
        stdin, stdout, stderr = client.exec_command(command)
        output = stdout.read().decode('utf-8', errors='ignore')
        if 'FILE_NOT_FOUND' in output:
            return []
        lines_list = output.strip().split('\n')
        registration_entries = []
        i = 0
        while i < len(lines_list):
            line = lines_list[i].strip()
            if 'CDR registration' in line and 'start at time:' in line:
                entry = {}
                start_match = re.search(r'time: (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', line)
                if start_match:
                    entry['start_time'] = start_match.group(1)
                    if i + 1 < len(lines_list):
                        next_line = lines_list[i + 1].strip()
                        if 'File moved:' in next_line and 'total:' in next_line:
                            count_match = re.search(r'total: (\d+)', next_line)
                            if count_match:
                                entry['count'] = int(count_match.group(1))
                            end_match = re.search(r'registration end time: (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', next_line)
                            if end_match:
                                entry['end_time'] = end_match.group(1)
                            if i + 2 < len(lines_list):
                                status_line = lines_list[i + 2].strip()
                                if 'CDR load' in status_line:
                                    entry['status'] = 'Registration Complete' if 'successfull' in status_line else ('Failed' if 'failed' in status_line else 'Unknown')
                                else:
                                    entry['status'] = 'Registration Complete'
                            else:
                                entry['status'] = 'Registration Complete'
                            if 'count' in entry and 'start_time' in entry and 'end_time' in entry:
                                registration_entries.append(entry)
                                i += 3
                                continue
            elif 'No file for' in line and 'at time:' in line:
                entry = {}
                entry['count'] = 0
                entry['status'] = 'No file for Registration'
                time_match = re.search(r'time: (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', line)
                if time_match:
                    entry['start_time'] = time_match.group(1)
                    entry['end_time'] = time_match.group(1)
                    registration_entries.append(entry)
            i += 1
        return registration_entries[-10:] if len(registration_entries) > 10 else registration_entries
    except Exception as e:
        print(f"[REGISTRATION ERROR] Reading {log_file}: {e}")
        return []


def read_ipdr_log(client, log_file, lines=300):
    ipdr_client = None
    try:
        ipdr_client = paramiko.SSHClient()
        ipdr_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ipdr_client.connect(hostname='192.168.61.20', port=22, username='dwhadmin', password='dwhadmin', timeout=10)
        log_path = f"/data02/scripts/dwh/file_move/{log_file}"
        command = f"tail -{lines} {log_path}"
        stdin, stdout, stderr = ipdr_client.exec_command(command)
        stdout.channel.recv_exit_status()
        output = stdout.read().decode('utf-8', errors='ignore')
        log_lines = output.strip().split('\n')
        entries = []
        for line in log_lines:
            line = line.strip()
            if not line or 'UDR file:' in line or '+++++' in line:
                continue
            if 'Start Time:' in line and 'End Time' in line and 'files are transfered' in line:
                match = re.search(r'Total\s+(\d+)\s+(?:IPDR|UDR)\s+files.*?Start Time:\s*([0-9/\-:]+)\s*and\s*End Time\s*:\s*([0-9/\-:]+)', line)
                if match:
                    entries.append({'count': int(match.group(1)), 'status': 'Transferred', 'start_time': match.group(2).strip(), 'end_time': match.group(3).strip()})
            elif 'files are transfered at time' in line:
                match = re.search(r'Total\s+(\d+)\s+(?:IPDR|UDR)\s+files.*?at time\s*:\s*([0-9/\-:]+)', line)
                if match:
                    entries.append({'count': int(match.group(1)), 'status': 'Transferred', 'start_time': match.group(2).strip(), 'end_time': match.group(2).strip()})
            elif 'No IPDR files at time' in line or 'No UDR files at time' in line:
                match = re.search(r'No (?:IPDR|UDR) files at time\s*:\s*([0-9/\-:]+)', line)
                if match:
                    entries.append({'count': 0, 'status': 'No IPDR files', 'start_time': match.group(1).strip(), 'end_time': match.group(1).strip()})
            elif 'is not transfered due error_code' in line:
                match = re.search(r'error_code:\s*(\d+)\s*at time\s*:\s*([0-9/\-:]+)', line)
                if match:
                    entries.append({'count': 0, 'status': f'Transfer Error (code: {match.group(1)})', 'start_time': match.group(2).strip(), 'end_time': match.group(2).strip()})
        ipdr_client.close()
        return entries[-10:] if len(entries) > 10 else entries
    except Exception as e:
        print(f"[IPDR ERROR] {e}")
        return []
    finally:
        if ipdr_client:
            try:
                ipdr_client.close()
            except:
                pass


def read_l1_loading_log(client, log_file, file_type, lines=100):
    try:
        log_path = f"{SERVER_CONFIG['loading_path']}/{log_file}"
        command = f"tail -{lines} {log_path}"
        stdin, stdout, stderr = client.exec_command(command)
        stdout.channel.recv_exit_status()
        output = stdout.read().decode('utf-8', errors='ignore')
        log_lines = output.strip().split('\n')
        entries = []
        for line in log_lines:
            line = line.strip()
            if not line or line.startswith('+'):
                continue
            if 'Total' in line and 'file load' in line and 'L1_' in line and 'at time:' in line:
                match = re.search(r'Total\s+(\d+)\s+(?:The\s+)?\w+\s+file load(?:ed)?\s+to\s+L1_\w+_TEMP\s+at time:\s*([0-9/\-:]+)', line)
                if match:
                    count = int(match.group(1))
                    time_val = match.group(2).strip().replace('----', ' ').replace('---', ' ')
                    entries.append({'count': count, 'status': 'Loaded' if count > 0 else 'Not Loaded', 'time': time_val})
            elif 'No' in line and 'file' in line and 'L1_' in line and 'at time:' in line:
                match = re.search(r'No\s+\w+\s+file (?:to load|loaded to)\s+L1_\w+_TEMP\s+at time:\s*([0-9/\-:]+)', line)
                if match:
                    time_val = match.group(1).strip().replace('----', ' ').replace('---', ' ')
                    entries.append({'count': 0, 'status': 'Not Loaded', 'time': time_val})
        return entries[-10:] if len(entries) > 10 else entries
    except Exception as e:
        print(f"[L1 ERROR] {e}")
        return []


def count_directory_files(client, category):
    try:
        base_path = f"{SERVER_CONFIG['cdr_base_path']}/{category}"
        counts = {'main': 0, 'process': 0, 'merge': 0}
        for key, path in [('main', base_path), ('process', f"{base_path}/process_dir"), ('merge', f"{base_path}/merge_dir")]:
            cmd = f"cd {path} && ls -1 *.add 2>/dev/null | wc -l"
            stdin, stdout, stderr = client.exec_command(cmd)
            stdout.channel.recv_exit_status()
            result = stdout.read().decode().strip()
            counts[key] = int(result) if result.isdigit() else 0
        return counts
    except Exception as e:
        print(f"Error counting files for {category}: {e}")
        return {'main': 0, 'process': 0, 'merge': 0}

# ═══════════════════════════════════════════════════════════════
#  204 — Parsers
# ═══════════════════════════════════════════════════════════════

def parse_msc_registration_log(lines_list, label):
    entries = []
    i = 0
    while i < len(lines_list):
        line = lines_list[i]
        if 'CDR registration' in line and 'start at time:' in line:
            start_match = re.search(r'time: (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', line)
            if start_match:
                start_time = start_match.group(1)
                if i + 1 < len(lines_list):
                    next_line = lines_list[i + 1]
                    if 'File moved:' in next_line and 'total:' in next_line:
                        count_match = re.search(r'total: (\d+)', next_line)
                        end_match = re.search(r'registration end time: (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', next_line)
                        entries.append({
                            'count': int(count_match.group(1)) if count_match else 0,
                            'status': 'Registration Complete',
                            'start_time': start_time,
                            'end_time': end_match.group(1) if end_match else start_time,
                        })
                        i += 2
                        continue
                    elif 'No file for' in next_line and 'at time:' in next_line:
                        entries.append({'count': 0, 'status': 'No file for Registration', 'start_time': start_time, 'end_time': start_time})
                        i += 2
                        continue
        elif 'No file for' in line and 'at time:' in line and 'CDR registration' not in line:
            time_match = re.search(r'time: (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', line)
            if time_match:
                entries.append({'count': 0, 'status': 'No file for Registration', 'start_time': time_match.group(1), 'end_time': time_match.group(1)})
        i += 1
    print(f"[204][REGISTRATION] {label}: {len(entries)} entries")
    return entries[-10:] if len(entries) > 10 else entries


def parse_msc_merge_log(lines_list, label):
    entries = []
    for line in lines_list:
        if 'Total' in line and 'merged in' in line and 'at time:' in line:
            try:
                count_match = re.search(r'Total\s+(\d+)', line)
                file_match  = re.search(r'merged in:?\s+([\w_\.]+)', line)
                size_match  = re.search(r'Size:\s*([\d.]+[KMG]?)', line)
                time_match  = re.search(r'at time:\s*(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', line)
                if count_match and file_match:
                    entries.append({
                        'count': int(count_match.group(1)),
                        'file':  file_match.group(1),
                        'size':  size_match.group(1) if size_match else 'N/A',
                        'time':  time_match.group(1) if time_match else 'N/A',
                        'status': 'Merge',
                    })
            except Exception as e:
                print(f"[204][MERGE] {e}")
        elif 'No cdr files' in line and 'at time:' in line:
            try:
                time_match = re.search(r'at time:\s*(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', line)
                if time_match:
                    entries.append({'count': 0, 'file': 'No file', 'size': 'N/A', 'time': time_match.group(1), 'status': 'No file for merge'})
            except Exception as e:
                print(f"[204][MERGE] {e}")
    print(f"[204][MERGE] {label}: {len(entries)} entries")
    return entries[-10:] if len(entries) > 10 else entries


def parse_msc_l1_loading_log(lines_list, label):
    entries = []
    for line in lines_list:
        if not line or line.startswith('+'):
            continue
        if 'Total' in line and 'file load' in line and 'L1_' in line and 'at time:' in line:
            match = re.search(
                r'Total\s+(\d+)\s+(?:The\s+)?\w+\s+file\s+load(?:ed)?\s+to\s+L1_\w+\s+at time:\s*([0-9/\-:]+)',
                line
            )
            if match:
                count    = int(match.group(1))
                time_val = match.group(2).replace('----', ' ').replace('---', ' ')
                entries.append({'count': count, 'status': 'Loaded' if count > 0 else 'Not Loaded', 'time': time_val})
        elif 'No' in line and 'file' in line and 'L1_' in line and 'at time:' in line:
            match = re.search(
                r'No\s+\w+\s+file\s+(?:to load|loaded to)\s+L1_\w+\s+at time:\s*([0-9/\-:]+)',
                line
            )
            if match:
                time_val = match.group(1).replace('----', ' ').replace('---', ' ')
                entries.append({'count': 0, 'status': 'Not Loaded', 'time': time_val})
    print(f"[204][L1] {label}: {len(entries)} entries")
    return entries[-10:] if len(entries) > 10 else entries


def parse_msc_delete_log(lines_list, label):
    entries = []
    i = 0
    while i < len(lines_list):
        line = lines_list[i]
        if 'Nokia Processed CDR delete start time:' in line:
            start_raw = re.search(r'start time:\s*(\d{2}/\d{2}/\d{2})-*\+?(\d{2}:\d{2}:\d{2})', line)
            start_time = 'N/A'
            if start_raw:
                d, t = start_raw.group(1), start_raw.group(2)
                parts = d.split('/')
                start_time = f"20{parts[2]}/{parts[0]}/{parts[1]} {t}"
            end_time = start_time
            count = 0
            status = 'Delete Complete'
            if i + 1 < len(lines_list):
                next_line = lines_list[i + 1]
                if 'Nokia MSC processed CDR files are deleted at time:' in next_line:
                    count_match = re.search(r'Total\s+(\d+)', next_line)
                    end_raw = re.search(r'at time:\s*(\d{2}/\d{2}/\d{4})-*(\d{2}:\d{2}:\d{2})', next_line)
                    if count_match:
                        count = int(count_match.group(1))
                    if end_raw:
                        d2, t2 = end_raw.group(1), end_raw.group(2)
                        p = d2.split('/')
                        end_time = f"{p[2]}/{p[1]}/{p[0]} {t2}"
                    if count == 0:
                        status = 'No Files Deleted'
                    entries.append({'count': count, 'status': status, 'start_time': start_time, 'end_time': end_time})
                    i += 2
                    continue
        i += 1
    print(f"[204][DELETE] {label}: {len(entries)} entries")
    return entries[-10:] if len(entries) > 10 else entries


# ═══════════════════════════════════════════════════════════════
#  NEW: real file count from /data02/sftp_msc on 204
# ═══════════════════════════════════════════════════════════════

def count_msc_directory_files(client):
    """
    Same approach as count_directory_files() on 202:
    SSH to 204, run ls | wc -l for each directory.

    Nokia  : main, process_dir, dump_dir
    Huawei : main, process_dir, merge_dir, dump_dir
    """
    result = {
        'nokia':  {'main': 0, 'process': 0, 'dump': 0},
        'huawei': {'main': 0, 'process': 0, 'merge': 0, 'dump': 0},
    }

    def ls_count(path):
        cmd = f"ls -1 {path} 2>/dev/null | wc -l"
        stdin, stdout, stderr = client.exec_command(cmd)
        stdout.channel.recv_exit_status()
        val = stdout.read().decode().strip()
        return int(val) if val.isdigit() else 0

    try:
        nb = '/data02/sftp_msc/nokia'
        hb = '/data02/sftp_msc/huawei'

        result['nokia']['main']    = ls_count(nb)
        result['nokia']['process'] = ls_count(f"{nb}/process_dir")
        result['nokia']['dump']    = ls_count(f"{nb}/dump_dir")

        result['huawei']['main']    = ls_count(hb)
        result['huawei']['process'] = ls_count(f"{hb}/process_dir")
        result['huawei']['merge']   = ls_count(f"{hb}/merge_dir")
        result['huawei']['dump']    = ls_count(f"{hb}/dump_dir")

        print(f"[204][COUNT] nokia={result['nokia']}  huawei={result['huawei']}")
    except Exception as e:
        print(f"[204][COUNT ERROR] {e}")

    return result

# ═══════════════════════════════════════════════════════════════
#  Background threads — 202
# ═══════════════════════════════════════════════════════════════

def update_file_counts():
    while True:
        try:
            client = get_ssh_client()
            if client:
                with count_lock:
                    for category in CDR_CATEGORIES:
                        counts = count_directory_files(client, category)
                        file_counts[category] = counts
                client.close()
            time.sleep(300)
        except Exception as e:
            print(f"Error in file count thread: {e}")
            time.sleep(300)


def update_merge_logs():
    print("[MERGE THREAD] Started")
    while True:
        try:
            client = get_ssh_client()
            if client:
                with merge_lock:
                    for segment, log_file in MERGE_LOG_FILES.items():
                        merge_logs[segment] = read_merge_log(client, log_file, lines=300)
                client.close()
            time.sleep(300)
        except Exception as e:
            print(f"[MERGE THREAD ERROR] {e}")
            time.sleep(300)


def update_registration_logs():
    print("[REGISTRATION THREAD] Started")
    while True:
        try:
            client = get_ssh_client()
            if client:
                with registration_lock:
                    for segment, log_file in REGISTRATION_LOG_FILES.items():
                        registration_logs[segment] = read_registration_log(client, log_file, lines=300)
                client.close()
            time.sleep(300)
        except Exception as e:
            print(f"[REGISTRATION THREAD ERROR] {e}")
            time.sleep(300)


def update_ipdr_logs():
    print("[IPDR THREAD] Started")
    while True:
        try:
            with ipdr_lock:
                global ipdr_logs
                ipdr_logs = read_ipdr_log(None, IPDR_LOG_FILE, lines=300)
            time.sleep(300)
        except Exception as e:
            print(f"[IPDR THREAD ERROR] {e}")
            time.sleep(300)


def update_l1_loading_logs():
    print("[L1_LOADING THREAD] Started")
    while True:
        try:
            client = get_ssh_client()
            if client:
                with l1_loading_lock:
                    for file_type, log_file in L1_LOADING_LOG_FILES.items():
                        l1_loading_logs[file_type] = read_l1_loading_log(client, log_file, file_type, lines=100)
                client.close()
            time.sleep(300)
        except Exception as e:
            print(f"[L1_LOADING THREAD ERROR] {e}")
            time.sleep(300)

# ═══════════════════════════════════════════════════════════════
#  Background threads — 204
# ═══════════════════════════════════════════════════════════════

def update_msc_all_logs():
    print("[204][ALL THREAD] Started")
    time.sleep(8)   # stagger: 202 connects first
    while True:
        try:
            client = get_ssh_client_204()
            if client:
                with msc_registration_lock:
                    for segment, filepath in MSC_REGISTRATION_LOG_FILES.items():
                        raw = remote_tail_204(client, filepath, lines=300)
                        msc_registration_logs[segment] = parse_msc_registration_log(raw, segment)
                with msc_merge_lock:
                    for segment, filepath in MSC_MERGE_LOG_FILES.items():
                        raw = remote_tail_204(client, filepath, lines=300)
                        msc_merge_logs[segment] = parse_msc_merge_log(raw, segment)
                with msc_l1_loading_lock:
                    for segment, filepath in MSC_L1_LOADING_LOG_FILES.items():
                        raw = remote_tail_204(client, filepath, lines=100)
                        msc_l1_loading_logs[segment] = parse_msc_l1_loading_log(raw, segment)
                with msc_delete_lock:
                    for segment, filepath in MSC_DELETE_LOG_FILES.items():
                        raw = remote_tail_204(client, filepath, lines=300)
                        msc_delete_logs[segment] = parse_msc_delete_log(raw, segment)
                # real file counts from sftp_msc dirs
                counts = count_msc_directory_files(client)
                with msc_count_lock:
                    msc_file_counts.update(counts)
                client.close()
                print("[204][ALL THREAD] Refresh complete")
            else:
                print("[204][ALL THREAD] Failed to connect")
            time.sleep(300)
        except Exception as e:
            print(f"[204][ALL THREAD ERROR] {e}")
            time.sleep(300)

# ═══════════════════════════════════════════════════════════════
#  API endpoints
# ═══════════════════════════════════════════════════════════════

@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

@app.route('/api/file-counts')
def get_file_counts():
    with count_lock:
        return jsonify({'timestamp': datetime.now().isoformat(), 'counts': dict(file_counts)})

@app.route('/api/merge-logs')
def get_merge_logs():
    with merge_lock:
        return jsonify({'timestamp': datetime.now().isoformat(), 'segments': dict(merge_logs)})

@app.route('/api/registration-logs')
def get_registration_logs():
    with registration_lock:
        return jsonify({'timestamp': datetime.now().isoformat(), 'segments': dict(registration_logs)})

@app.route('/api/ipdr-logs')
def get_ipdr_logs():
    with ipdr_lock:
        return jsonify({'timestamp': datetime.now().isoformat(), 'logs': ipdr_logs})

@app.route('/api/l1-loading-logs')
def get_l1_loading_logs():
    with l1_loading_lock:
        return jsonify({'timestamp': datetime.now().isoformat(), 'segments': dict(l1_loading_logs)})

@app.route('/api/204/registration-logs')
def get_msc_registration_logs():
    with msc_registration_lock:
        return jsonify({'timestamp': datetime.now().isoformat(), 'segments': dict(msc_registration_logs)})

@app.route('/api/204/merge-logs')
def get_msc_merge_logs():
    with msc_merge_lock:
        return jsonify({'timestamp': datetime.now().isoformat(), 'segments': dict(msc_merge_logs)})

@app.route('/api/204/l1-loading-logs')
def get_msc_l1_loading_logs():
    with msc_l1_loading_lock:
        return jsonify({'timestamp': datetime.now().isoformat(), 'segments': dict(msc_l1_loading_logs)})

@app.route('/api/204/delete-logs')
def get_msc_delete_logs():
    with msc_delete_lock:
        return jsonify({'timestamp': datetime.now().isoformat(), 'segments': dict(msc_delete_logs)})

@app.route('/api/204/file-counts')
def get_msc_file_counts():
    with msc_count_lock:
        return jsonify({'timestamp': datetime.now().isoformat(), 'counts': dict(msc_file_counts)})

# ═══════════════════════════════════════════════════════════════
#  Dashboard HTML
# ═══════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CDR Log Monitor</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Roboto",sans-serif; background:#f5f5f5; color:#212529; line-height:1.6; }
.container { max-width:1600px; margin:0 auto; padding:0; }

/* Header */
.header { background:linear-gradient(135deg,#71bd44 0%,#5fae3a 100%); color:white; padding:24px 32px; margin-bottom:20px; box-shadow:0 2px 8px rgba(0,0,0,.1); }
.header-content { display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:16px; max-width:1600px; margin:0 auto; }
.header-left { display:flex; align-items:center; gap:20px; }
.header-logo { width:60px; height:60px; background:white; border-radius:8px; padding:8px; display:flex; align-items:center; justify-content:center; box-shadow:0 2px 8px rgba(0,0,0,.15); }
.header-logo img { width:100%; height:100%; object-fit:contain; }
.header-title-group { display:flex; flex-direction:column; }
.header-left h1 { font-size:26px; font-weight:600; color:white; margin:0 0 4px 0; }
.header-subtitle { font-size:14px; color:rgba(255,255,255,.9); }
.header-right { display:flex; align-items:center; gap:20px; }
.status-badge { display:flex; align-items:center; gap:8px; padding:8px 16px; background:rgba(255,255,255,.2); border-radius:6px; font-size:14px; color:white; }
.status-dot { width:8px; height:8px; border-radius:50%; background:#fff; animation:pulse 2s ease-in-out infinite; }
@keyframes pulse { 0%,100%{opacity:1}50%{opacity:.5} }
.live-clock { padding:8px 16px; background:rgba(255,255,255,.2); border-radius:6px; display:flex; flex-direction:column; align-items:center; gap:2px; }
.clock-time { font-size:16px; font-weight:600; color:white; font-family:"Courier New",monospace; letter-spacing:1px; }

/* ─── File Count — fixed 7-column grid, items same style ─── */
.file-count-section { background:white; margin:0 20px 20px 20px; border-radius:8px; box-shadow:0 1px 3px rgba(0,0,0,.1); overflow:hidden; }
.file-count-header { background:linear-gradient(135deg,#71bd44 0%,#5fae3a 100%); color:white; padding:14px 20px; font-weight:600; font-size:16px; }
.file-count-grid {
  display: grid;
  grid-template-columns: repeat(7, 1fr);
  gap: 0;
}
.file-count-item {
  padding: 20px 16px;
  border-right: 1px solid #e0e0e0;
  border-bottom: 1px solid #e0e0e0;
  transition: background .2s;
}
.file-count-item:hover { background:#f9f9f9; }
.file-count-category { font-weight:600; font-size:13px; color:#424242; text-transform:uppercase; margin-bottom:12px; letter-spacing:.5px; }
.file-count-stats { display:flex; flex-direction:column; gap:8px; }
.file-count-row { display:flex; justify-content:space-between; align-items:center; font-size:12px; }
.file-count-label { color:#757575; }
.file-count-value { font-weight:600; font-size:14px; min-width:40px; text-align:right; }
/* CBS CDR colours */
.file-count-value.main    { color:#f57c00; }
.file-count-value.process { color:#2979ff; }
.file-count-value.merge   { color:#9c27b0; }
/* MSC — same three colours + green for dump */
.file-count-value.msc-orange { color:#f57c00; }
.file-count-value.msc-blue   { color:#2979ff; }
.file-count-value.msc-purple { color:#9c27b0; }
.file-count-value.msc-green  { color:#2e7d1f; }

/* Tabs */
.tab-navigation { background:white; margin:0 20px 20px 20px; border-radius:8px; box-shadow:0 1px 3px rgba(0,0,0,.1); overflow:hidden; }
.tab-buttons { display:flex; border-bottom:2px solid #e0e0e0; }
.tab-button { flex:1; padding:16px 24px; background:white; border:none; font-size:15px; font-weight:600; color:#757575; cursor:pointer; transition:all .3s; border-bottom:3px solid transparent; }
.tab-button:hover { background:#f9f9f9; color:#424242; }
.tab-button.active { color:#71bd44; border-bottom-color:#71bd44; background:#f9fff9; }
.tab-content { display:none; }
.tab-content.active { display:block; }

/* Section divider — same green for ALL */
.section-divider { display:flex; align-items:center; margin:0 20px 16px 20px; }
.section-divider-line { flex:1; height:2px; background:#e0e0e0; }
.section-divider-label { padding:6px 16px; background:#71bd44; color:white; border-radius:20px; font-size:13px; font-weight:600; margin:0 12px; white-space:nowrap; }

/* Grid */
.logs-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(450px,1fr)); gap:20px; padding:0 20px 20px 20px; }

/* Cards — ALL same green header */
.segment-card { background:white; border-radius:8px; box-shadow:0 1px 3px rgba(0,0,0,.1); overflow:hidden; transition:box-shadow .2s; }
.segment-card:hover { box-shadow:0 4px 12px rgba(0,0,0,.15); }
.segment-header { padding:16px 20px; background:linear-gradient(135deg,#71bd44 0%,#5fae3a 100%); color:white; display:flex; justify-content:space-between; align-items:center; }
.segment-title { font-size:17px; font-weight:600; text-transform:uppercase; }
.segment-count { background:rgba(255,255,255,.25); color:white; padding:4px 12px; border-radius:12px; font-size:13px; font-weight:600; }

/* Table */
.logs-table-container { max-height:500px; overflow-y:auto; overflow-x:auto; }
.logs-table-container::-webkit-scrollbar { width:6px; height:6px; }
.logs-table-container::-webkit-scrollbar-track { background:#f5f5f5; }
.logs-table-container::-webkit-scrollbar-thumb { background:#71bd44; border-radius:3px; }
.logs-table { width:100%; border-collapse:collapse; font-size:13px; }
.logs-table thead { position:sticky; top:0; z-index:10; }
.logs-table th { background:#f5f5f5; color:#424242; padding:12px 14px; text-align:left; font-weight:600; border-bottom:2px solid #e0e0e0; white-space:nowrap; }
.logs-table tbody tr { border-bottom:1px solid #f0f0f0; transition:background .2s; }
.logs-table tbody tr:hover { background:#fafafa; }
.logs-table td { padding:12px 14px; color:#424242; vertical-align:middle; }
.logs-table td.filename { font-family:"Consolas","Monaco","Courier New",monospace; font-size:12px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:300px; }
.logs-table td.status { font-weight:600; white-space:nowrap; }
.logs-table td.status.completed  { color:#2e7d1f; }
.logs-table td.status.no-files   { color:#ff9800; }
.logs-table td.status.incomplete { color:#f57c00; }
.logs-table td.time { color:#757575; font-size:12px; white-space:nowrap; }

.loading-state { text-align:center; padding:60px 20px; color:#6c757d; grid-column:1/-1; }
.loading-spinner { width:36px; height:36px; border:3px solid #e0e0e0; border-top-color:#71bd44; border-radius:50%; animation:spin 1s linear infinite; margin:0 auto 14px; }
@keyframes spin { to{transform:rotate(360deg)} }

@media(max-width:1400px){ .file-count-grid{ grid-template-columns:repeat(5,1fr); } }
@media(max-width:1024px){ .file-count-grid{ grid-template-columns:repeat(4,1fr); } }
@media(max-width:768px){
  .logs-grid{ grid-template-columns:1fr; padding:0 10px 10px 10px; }
  .header{ padding:16px 20px; }
  .tab-button{ font-size:13px; padding:12px 8px; }
  .file-count-grid{ grid-template-columns:repeat(3,1fr); }
}
</style>
</head>
<body>
<div class="container">

  <!-- Header -->
  <div class="header">
    <div class="header-content">
      <div class="header-left">
        <div class="header-logo"><img src="/static/teletalk.png" alt="Teletalk Logo"></div>
        <div class="header-title-group">
          <h1>CDR Log Monitor</h1>
          <div class="header-subtitle">Real-time monitoring of CDR registration logs</div>
        </div>
      </div>
      <div class="header-right">
        <div class="status-badge"><span class="status-dot"></span><span>Live Monitoring</span></div>
        <div class="live-clock" id="liveClock"><div class="clock-time"></div></div>
      </div>
    </div>
  </div>

  <!-- File Count Dashboard -->
  <div class="file-count-section">
    <div class="file-count-header">📊 CDR Directory File Counts (Live)</div>
    <div class="file-count-grid" id="fileCountGrid">
      <div class="loading-state" style="grid-column:1/-1;padding:30px;">Loading file counts...</div>
    </div>
  </div>

  <!-- Tabs -->
  <div class="tab-navigation">
    <div class="tab-buttons">
      <button class="tab-button active" onclick="switchTab(event,'file-registration')">📋 File Registration</button>
      <button class="tab-button"        onclick="switchTab(event,'merge')">🔄 Merge Logs</button>
      <button class="tab-button"        onclick="switchTab(event,'loading')">📥 Loading</button>
      <button class="tab-button"        onclick="switchTab(event,'regulatory')">📜 Regulatory and LEA Process</button>
    </div>
  </div>

  <!-- File Registration Tab -->
  <div class="tab-content active" id="file-registration-content">
    <div class="section-divider">
      <div class="section-divider-line"></div>
      <div class="section-divider-label">CBS CDR — 192.168.61.202</div>
      <div class="section-divider-line"></div>
    </div>
    <div class="logs-grid" id="fileRegistrationGrid">
      <div class="loading-state"><div class="loading-spinner"></div><div>Loading...</div></div>
    </div>
    <div class="section-divider" style="margin-top:8px;">
      <div class="section-divider-line"></div>
      <div class="section-divider-label">MSC CDR — 192.168.61.204</div>
      <div class="section-divider-line"></div>
    </div>
    <div class="logs-grid" id="mscRegistrationGrid">
      <div class="loading-state"><div class="loading-spinner"></div><div>Loading MSC registration logs...</div></div>
    </div>
  </div>

  <!-- Merge Logs Tab -->
  <div class="tab-content" id="merge-content">
    <div class="section-divider">
      <div class="section-divider-line"></div>
      <div class="section-divider-label">CBS CDR — 192.168.61.202</div>
      <div class="section-divider-line"></div>
    </div>
    <div class="logs-grid" id="mergeLogsGrid">
      <div class="loading-state"><div class="loading-spinner"></div><div>Loading...</div></div>
    </div>
    <div class="section-divider" style="margin-top:8px;">
      <div class="section-divider-line"></div>
      <div class="section-divider-label">MSC CDR — 192.168.61.204</div>
      <div class="section-divider-line"></div>
    </div>
    <div class="logs-grid" id="mscMergeGrid">
      <div class="loading-state"><div class="loading-spinner"></div><div>Loading MSC merge logs...</div></div>
    </div>
  </div>

  <!-- Loading Tab -->
  <div class="tab-content" id="loading-content">
    <div class="section-divider">
      <div class="section-divider-line"></div>
      <div class="section-divider-label">CBS CDR — 192.168.61.202</div>
      <div class="section-divider-line"></div>
    </div>
    <div class="logs-grid" id="loadingLogsGrid">
      <div class="loading-state"><div class="loading-spinner"></div><div>Loading...</div></div>
    </div>
    <div class="section-divider" style="margin-top:8px;">
      <div class="section-divider-line"></div>
      <div class="section-divider-label">MSC CDR — 192.168.61.204</div>
      <div class="section-divider-line"></div>
    </div>
    <div class="logs-grid" id="mscLoadingGrid">
      <div class="loading-state"><div class="loading-spinner"></div><div>Loading MSC loading logs...</div></div>
    </div>
  </div>

  <!-- Regulatory Tab -->
  <div class="tab-content" id="regulatory-content">
    <div class="logs-grid">
      <div class="segment-card">
        <div class="segment-header">
          <div class="segment-title">IPDR</div>
          <div class="segment-count" id="ipdr-count">0</div>
        </div>
        <div class="logs-table-container">
          <table class="logs-table">
            <thead><tr>
              <th style="width:15%">File Count</th>
              <th style="width:20%">Status</th>
              <th style="width:30%">Start Time</th>
              <th style="width:35%">End Time</th>
            </tr></thead>
            <tbody id="ipdrLogsTable">
              <tr><td colspan="4" style="text-align:center;padding:40px;">
                <div class="loading-spinner" style="margin:0 auto 10px;"></div>Loading IPDR logs...
              </td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

</div>

<script>
/* Tab switching */
function switchTab(event, tabName) {
  document.querySelectorAll('.tab-button').forEach(b => b.classList.remove('active'));
  event.target.classList.add('active');
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  document.getElementById(tabName + '-content').classList.add('active');
}

/* ═══════════════════════════════════════════════════════
   FILE COUNT GRID
   Layout: 7 columns fixed.
   Row 1: ADJ CM COM DATA MON SMS TRANSFER
   Row 2: VOICE VOU MSC_NOKIA MSC_HUAWEI (then empty cells fill)

   CBS CDR: Main(orange) / Process(blue) / Merge(purple)
   MSC Nokia : Main(orange) / Process(blue) / Dump(purple)
   MSC Huawei: Main(orange) / Process(blue) / Merge(purple) / Dump(green)
   ═══════════════════════════════════════════════════════ */

/* CBS CDR item — same as before */
function cbsCountItem(category, counts) {
  return `<div class="file-count-item">
    <div class="file-count-category">${category.toUpperCase()}</div>
    <div class="file-count-stats">
      <div class="file-count-row"><span class="file-count-label">Main:</span>   <span class="file-count-value main">${counts.main}</span></div>
      <div class="file-count-row"><span class="file-count-label">Process:</span><span class="file-count-value process">${counts.process}</span></div>
      <div class="file-count-row"><span class="file-count-label">Merge:</span>  <span class="file-count-value merge">${counts.merge}</span></div>
    </div>
  </div>`;
}

/* MSC Nokia cell — real counts filled by fetchMscFileCounts() */
function nokiaCountItem() {
  return `<div class="file-count-item">
    <div class="file-count-category">MSC NOKIA</div>
    <div class="file-count-stats">
      <div class="file-count-row"><span class="file-count-label">Main:</span>   <span class="file-count-value msc-orange" id="nokia-main">—</span></div>
      <div class="file-count-row"><span class="file-count-label">Process:</span><span class="file-count-value msc-blue"   id="nokia-process">—</span></div>
      <div class="file-count-row"><span class="file-count-label">Dump:</span>   <span class="file-count-value msc-purple" id="nokia-dump">—</span></div>
    </div>
  </div>`;
}

/* MSC Huawei cell — real counts filled by fetchMscFileCounts() */
function huaweiCountItem() {
  return `<div class="file-count-item">
    <div class="file-count-category">MSC HUAWEI</div>
    <div class="file-count-stats">
      <div class="file-count-row"><span class="file-count-label">Main:</span>   <span class="file-count-value msc-orange" id="huawei-main">—</span></div>
      <div class="file-count-row"><span class="file-count-label">Process:</span><span class="file-count-value msc-blue"   id="huawei-process">—</span></div>
      <div class="file-count-row"><span class="file-count-label">Merge:</span>  <span class="file-count-value msc-purple" id="huawei-merge">—</span></div>
      <div class="file-count-row"><span class="file-count-label">Dump:</span>   <span class="file-count-value msc-green"  id="huawei-dump">—</span></div>
    </div>
  </div>`;
}

/*
  CBS CDR order from Python: ['adj','cm','com','data','mon','sms','transfer','voice','vou']
  Grid is 7 columns:
    Row 1 (cols 1-7): adj  cm  com  data  mon  sms  transfer
    Row 2 (cols 1-2): voice  vou
    Row 2 (cols 3-4): MSC Nokia  MSC Huawei   ← appended right after vou
*/
function renderFileCountGrid(cbsEntries) {
  const grid = document.getElementById('fileCountGrid');
  // cbsEntries is an array of [category, counts] in dict order
  // We render them all then append Nokia + Huawei
  let html = cbsEntries.map(([cat, cnt]) => cbsCountItem(cat, cnt)).join('');
  html += nokiaCountItem();
  html += huaweiCountItem();
  grid.innerHTML = html;
}

/* Fetch real file counts from 204 and update the cells */
async function fetchMscFileCounts() {
  try {
    const r = await fetch('/api/204/file-counts');
    const d = await r.json();
    const set = (id, val) => {
      const el = document.getElementById(id);
      if (el) el.textContent = (val !== undefined && val !== null) ? val : '—';
    };
    if (d.counts && d.counts.nokia) {
      set('nokia-main',    d.counts.nokia.main);
      set('nokia-process', d.counts.nokia.process);
      set('nokia-dump',    d.counts.nokia.dump);
    }
    if (d.counts && d.counts.huawei) {
      set('huawei-main',    d.counts.huawei.main);
      set('huawei-process', d.counts.huawei.process);
      set('huawei-merge',   d.counts.huawei.merge);
      set('huawei-dump',    d.counts.huawei.dump);
    }
  } catch(e) { console.error('[MSC file counts]', e); }
}

/* ═══════════════════════════════════════
   CARD BUILDERS — all same green header
   ═══════════════════════════════════════ */
function statusCell(status) {
  const ok   = ['Registration Complete','Merge','Loaded','Delete Complete','Transferred'].includes(status);
  const warn = ['No file for Registration','No file for merge','Not Loaded','No IPDR files','No Files Deleted'].some(s => status.includes(s));
  const cls  = ok ? 'completed' : warn ? 'no-files' : 'incomplete';
  const icon = ok ? '✅' : warn ? '⚠️' : '❌';
  return `<td class="status ${cls}"><span>${icon}</span> ${status}</td>`;
}

function emptyRow(cols, msg) {
  return `<tr><td colspan="${cols}" style="text-align:center;padding:40px;color:#9e9e9e;"><div style="font-size:36px;opacity:.4">📭</div>${msg}</td></tr>`;
}

function card(title, count, thead, tbody) {
  return `<div class="segment-card">
    <div class="segment-header">
      <div class="segment-title">${title}</div>
      <div class="segment-count">${count}</div>
    </div>
    <div class="logs-table-container">
      <table class="logs-table"><thead>${thead}</thead><tbody>${tbody}</tbody></table>
    </div>
  </div>`;
}

/* Registration */
function regRow(log) {
  return `<tr><td class="filename">${log.count}</td>${statusCell(log.status)}<td class="time">${log.start_time}</td><td class="time">${log.end_time}</td></tr>`;
}
function regCard(name, logs) {
  const label = name.replace('msc_nokia','MSC NOKIA').replace('msc_huawei','MSC HUAWEI').toUpperCase();
  const body  = logs.length ? [...logs].reverse().map(regRow).join('') : emptyRow(4,'No registration logs');
  const thead = `<tr><th style="width:15%">File Count</th><th style="width:30%">Status</th><th style="width:27.5%">Start Time</th><th style="width:27.5%">End Time</th></tr>`;
  return card(label, logs.length, thead, body);
}

/* Merge */
function mergeRow(log) {
  return `<tr><td class="filename">${log.count}</td><td class="filename">${log.file}</td>${statusCell(log.status)}<td class="time">${log.size}</td><td class="time">${log.time}</td></tr>`;
}
function mergeCard(name, logs) {
  const label = name.replace('msc_huawei','MSC HUAWEI').toUpperCase();
  const body  = logs.length ? [...logs].reverse().map(mergeRow).join('') : emptyRow(5,'No merge logs');
  const thead = `<tr><th style="width:12%">Total Count</th><th style="width:33%">File Name</th><th style="width:22%">Status</th><th style="width:10%">Size</th><th style="width:23%">Time</th></tr>`;
  return card(label, logs.length, thead, body);
}

/* Loading */
function loadRow(log) {
  return `<tr><td class="filename">${log.count}</td>${statusCell(log.status)}<td class="time">${log.time}</td></tr>`;
}
function loadCard(name, logs) {
  let label;
  if      (name === 'msc_nokia')  label = 'L1 MSC NOKIA TEMP';
  else if (name === 'msc_huawei') label = 'L1 MSC HUAWEI TEMP';
  else                            label = 'L1 ' + name.toUpperCase() + ' TEMP';
  const body  = logs.length ? [...logs].reverse().map(loadRow).join('') : emptyRow(3,'No loading logs');
  const thead = `<tr><th style="width:20%">Total Count</th><th style="width:30%">Status</th><th style="width:50%">Time</th></tr>`;
  return card(label, logs.length, thead, body);
}

/* Delete CDR (Nokia) */
function deleteRow(log) {
  return `<tr><td class="filename">${log.count}</td>${statusCell(log.status)}<td class="time">${log.start_time}</td><td class="time">${log.end_time}</td></tr>`;
}
function deleteCard(logs) {
  const body  = logs.length ? [...logs].reverse().map(deleteRow).join('') : emptyRow(4,'No delete CDR logs');
  const thead = `<tr><th style="width:15%">File Count</th><th style="width:30%">Status</th><th style="width:27.5%">Start Time</th><th style="width:27.5%">End Time</th></tr>`;
  return card('MSC NOKIA — DELETE CDR', logs.length, thead, body);
}

/* IPDR */
function renderIPDR(data) {
  const tbody   = document.getElementById('ipdrLogsTable');
  const countEl = document.getElementById('ipdr-count');
  if (data && data.logs && data.logs.length > 0) {
    countEl.textContent = data.logs.length;
    tbody.innerHTML = [...data.logs].reverse().map(log =>
      `<tr><td class="filename">${log.count}</td>${statusCell(log.status)}<td class="time">${log.start_time}</td><td class="time">${log.end_time}</td></tr>`
    ).join('');
  } else {
    countEl.textContent = '0';
    tbody.innerHTML = emptyRow(4,'No IPDR logs available');
  }
}

/* ═══════════════════════════════════════════════════════
   PARALLEL FETCH — 202 and 204 together
   ═══════════════════════════════════════════════════════ */
async function refreshAllData() {
  const [r202, r204] = await Promise.all([
    Promise.all([
      fetch('/api/registration-logs').then(r=>r.json()).catch(()=>null),
      fetch('/api/merge-logs').then(r=>r.json()).catch(()=>null),
      fetch('/api/l1-loading-logs').then(r=>r.json()).catch(()=>null),
      fetch('/api/ipdr-logs').then(r=>r.json()).catch(()=>null),
      fetch('/api/file-counts').then(r=>r.json()).catch(()=>null),
    ]),
    Promise.all([
      fetch('/api/204/registration-logs').then(r=>r.json()).catch(()=>null),
      fetch('/api/204/merge-logs').then(r=>r.json()).catch(()=>null),
      fetch('/api/204/l1-loading-logs').then(r=>r.json()).catch(()=>null),
      fetch('/api/204/delete-logs').then(r=>r.json()).catch(()=>null),
    ])
  ]);

  const [regData202, mergeData202, l1Data202, ipdrData, fileCountData] = r202;
  const [regData204, mergeData204, l1Data204, deleteData204]           = r204;

  /* File count grid — render CBS CDR items first, then Nokia+Huawei appended */
  if (fileCountData && fileCountData.counts) {
    const entries = Object.entries(fileCountData.counts);
    if (entries.length) renderFileCountGrid(entries);
  }

  /* After grid is rendered, fetch real MSC file counts and update cells */
  fetchMscFileCounts();

  /* 202 Registration */
  if (regData202 && regData202.segments) {
    const segs = Object.entries(regData202.segments);
    document.getElementById('fileRegistrationGrid').innerHTML = segs.length
      ? segs.map(([n,l]) => regCard(n, l)).join('')
      : '<div class="loading-state">No data</div>';
  }

  /* 202 Merge */
  if (mergeData202 && mergeData202.segments) {
    const segs = Object.entries(mergeData202.segments);
    document.getElementById('mergeLogsGrid').innerHTML = segs.length
      ? segs.map(([n,l]) => mergeCard(n, l)).join('')
      : '<div class="loading-state">No data</div>';
  }

  /* 202 Loading */
  if (l1Data202 && l1Data202.segments) {
    const segs = Object.entries(l1Data202.segments);
    document.getElementById('loadingLogsGrid').innerHTML = segs.length
      ? segs.map(([n,l]) => loadCard(n, l)).join('')
      : '<div class="loading-state">No data</div>';
  }

  /* IPDR */
  if (ipdrData) renderIPDR(ipdrData);

  /* 204 Registration + Delete */
  if (regData204 && regData204.segments) {
    const segs = Object.entries(regData204.segments);
    let html = segs.length ? segs.map(([n,l]) => regCard(n, l)).join('') : '<div class="loading-state">No MSC data</div>';
    if (deleteData204 && deleteData204.segments && deleteData204.segments['msc_nokia'])
      html += deleteCard(deleteData204.segments['msc_nokia']);
    document.getElementById('mscRegistrationGrid').innerHTML = html;
  }

  /* 204 Merge */
  if (mergeData204 && mergeData204.segments) {
    const segs = Object.entries(mergeData204.segments);
    document.getElementById('mscMergeGrid').innerHTML = segs.length
      ? segs.map(([n,l]) => mergeCard(n, l)).join('')
      : '<div class="loading-state">No MSC merge data</div>';
  }

  /* 204 Loading */
  if (l1Data204 && l1Data204.segments) {
    const segs = Object.entries(l1Data204.segments);
    document.getElementById('mscLoadingGrid').innerHTML = segs.length
      ? segs.map(([n,l]) => loadCard(n, l)).join('')
      : '<div class="loading-state">No MSC loading data</div>';
  }
}

/* Clock */
function updateClock() {
  const now = new Date();
  const h = String(now.getHours()).padStart(2,'0');
  const m = String(now.getMinutes()).padStart(2,'0');
  const s = String(now.getSeconds()).padStart(2,'0');
  document.querySelector('.clock-time').textContent = `${h}:${m}:${s}`;
}

refreshAllData();
updateClock();
setInterval(refreshAllData, 300000);
setInterval(updateClock, 1000);
</script>
</body>
</html>'''


# ═══════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    threading.Thread(target=update_file_counts,       daemon=True).start()
    threading.Thread(target=update_merge_logs,        daemon=True).start()
    threading.Thread(target=update_registration_logs, daemon=True).start()
    threading.Thread(target=update_ipdr_logs,         daemon=True).start()
    threading.Thread(target=update_l1_loading_logs,   daemon=True).start()
    threading.Thread(target=update_msc_all_logs,      daemon=True).start()

    print("=" * 55)
    print("CDR Log Monitor Starting...")
    print(f"CBS CDR Server : {SERVER_CONFIG['host']}")
    print(f"MSC CDR Server : {SERVER_CONFIG_204['host']}")
    print(f"Dashboard      : http://localhost:5013")
    print("=" * 55)

    app.run(host='0.0.0.0', port=5013, debug=True)