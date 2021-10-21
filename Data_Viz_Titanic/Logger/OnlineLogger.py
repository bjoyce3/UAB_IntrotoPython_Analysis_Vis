import os
import sys 
import time
import threading
import json
import datetime
import random
import requests
import uuid
from pathlib import Path
import zipfile

class Watcher(object):
    running = True
    refresh_delay_secs = 1

    # Constructor
    def __init__(self, watch_file, call_func_on_change=None, *args, **kwargs):
        self._cached_stamp = 0
        self.filename = watch_file
        self.call_func_on_change = call_func_on_change
        self.args = args
        self.kwargs = kwargs

    # Look for changes
    def look(self):
        stamp = os.stat(self.filename).st_mtime
        if stamp != self._cached_stamp:
            self._cached_stamp = stamp
            # File has changed, so do something...
            if self.call_func_on_change is not None:
                self.call_func_on_change(*self.args, **self.kwargs)

    # Keep watching in a loop        
    def watch(self):
        while self.running: 
            try: 
                # Look for changes
                time.sleep(self.refresh_delay_secs) 
                self.look() 
            except KeyboardInterrupt: 
                #print('\nDone') 
                break 
            except FileNotFoundError:
                #print('File was not found. Please do not change notebook name nor change location relative to logger.py. Rerun intiliaztion cell once filename/location is fixed.')
                break
            except: 
                #print('Stopping logging: Unhandled error: %s' % sys.exc_info())
                return

class Pusher(object):
    running = True
    refresh_delay_secs = 1

    # Constructor
    def __init__(self, log_file, call_func_on_change=None, *args, **kwargs):
        self._cached_stamp = 0
        self.filename = log_file
        self.call_func_on_change = call_func_on_change
        self.args = args
        self.kwargs = kwargs

    # Look for changes
    def look(self):
        if (not os.path.isfile(self.filename)):
            return
        stamp = os.stat(self.filename).st_mtime
        if stamp != self._cached_stamp:
            self._cached_stamp = stamp
            # File has changed, so do something...
            if self.call_func_on_change is not None:
                self.call_func_on_change(*self.args, **self.kwargs)

    # Keep watching in a loop        
    def watch(self):
        while self.running: 
            try: 
                # Look for changes
                time.sleep(self.refresh_delay_secs) 
                self.look() 
            except KeyboardInterrupt: 
                #print('\nDone') 
                break 
            except FileNotFoundError:
                #print('File was not found. Please do not change notebook name nor change location relative to logger.py. Rerun intiliaztion cell once filename/location is fixed.')
                break
            except: 
                #print('Stopping logging: Unhandled error: %s' % sys.exc_info())
                return



def get_same_length_change(old_checkpoint, current_checkpoint):
    change_count = 0
    cell_array_number = []
    cell_change_array = []
    for i in range(0, len(current_checkpoint['cells'])):
        old_cell = old_checkpoint['cells'][i]
        new_cell = current_checkpoint['cells'][i]
        if (old_cell != new_cell):
            change_count += 1
            cell_array_number.append(i)
            cell_change_array.append(new_cell)
    
    return change_count, cell_array_number, cell_change_array


def get_new_cell_set(new_checkpoint):
    return 0, [], new_checkpoint['cells']

def get_changed_cells(old_checkpoint, current_checkpoint):
    
    if len(old_checkpoint['cells']) == len(current_checkpoint['cells']):
        num_changes, cells_changed, new_contents = get_same_length_change(old_checkpoint, current_checkpoint)
        return num_changes, cells_changed, "cells_changed", new_contents

    if len(old_checkpoint['cells']) != len(current_checkpoint['cells']):
        num_changes, cells_changed, new_contents = get_new_cell_set(current_checkpoint)
        return num_changes, cells_changed, "all_cell_refresh", new_contents

    return 0, [], "error", []

def get_diff_dict(old_checkpoint, current_checkpoint, current_time):
    diff_dict = {}
    num_changes, cell_changed, change_type, new_content = get_changed_cells(old_checkpoint, current_checkpoint)
    diff_dict.update({'time' : current_time,
                        "num_changes" : num_changes,
                        "cells_changed" : cell_changed,
                        "change_type" : change_type,
                        "new_contents" : new_content
                        })
    return diff_dict

def parse_lines(line_array):
    new_array = []
    for i in line_array:
        if len(i) > 200:
            new_line = i[0:200]
            new_line = new_line + "\n"
            new_array.append(new_line)
            continue
        new_array.append(i)
    return new_array

def parse_cell(current_cell):
    if "outputs" not in current_cell:
        return current_cell 
    
    all_outputs = current_cell['outputs']
    if len(all_outputs) == 0:
        return current_cell
    
    new_outputs = []
    for i in all_outputs:
        if "text" not in i:
            new_outputs.append(i)
            continue
        
        all_text = i['text']
        all_text = parse_lines(all_text)
        if len(all_text) < 20:
            new_outputs.append(i)
            continue
        
        new_text = all_text[0:20]
        i.update({'text' : new_text})
        new_outputs.append(i)

    current_cell.update({'outputs' : new_outputs})   
    return current_cell

def parse_checkpoint(current_checkpoint):
    cells = current_checkpoint['cells']
    new_cells = []
    for i in cells:
        if i['cell_type'] != "code":
            new_cells.append(i)
            continue
        
        new_cell = parse_cell(i)
        new_cells.append(new_cell)

    current_checkpoint.update({'cells': new_cells})
    return current_checkpoint
    
def push_log(log_filename):
    if os.path.isfile(not log_filename):
        return
    log = None 
    with open(log_filename, 'r') as f:
        log = json.loads(f.read())
    push_to_cloud(log)
    
def push_to_cloud(log):
    url = 'https://us-south.functions.appdomain.cloud/api/v1/web/ORG-UNC-dist-seed-james_dev/cyverse/add-cyverse-log'
    

    help_data = {
        "body": {
            "log_id": log['log_id'],
            "machine_id": log['machine_id'],
            "course_id": log['course_id'],
            "log_type": "Jupyter",
            "log": log
    }
    }

    try :
        requests.post(url, json=help_data)
    except: 
        pass


# Call this function each time a change happens
def logger(base_filename, course_id):
    src_path = os.path.realpath(base_filename)
    dir_path = os.path.dirname(src_path)
    
    historicalSize = -1
    while (historicalSize != os.path.getsize(src_path)):
      historicalSize = os.path.getsize(src_path)
      time.sleep(0.25)
    
    with open(src_path, 'r') as checkpoint_source:
        checkpoint = json.loads(checkpoint_source.read())
        checkpoint = parse_checkpoint(checkpoint)
        log = Path(os.path.join(dir_path, base_filename.split('.')[0]+'_log.json'))
        if log.is_file():
            old = ''
            with open(log, 'r') as f:
                try:
                    old = json.loads(f.read())
                except json.decoder.JSONDecodeError:
                    #print('There is an error decoding log. Log file may be corrupt')
                    return
            
            current_checkpoint = old['current_checkpoint']['checkpoint']
            should_update =  current_checkpoint != checkpoint
            
            if should_update:
                with open(log, 'w') as f:
                    current_time = str(datetime.datetime.now())
                    old["diffs"].append(get_diff_dict(current_checkpoint, checkpoint, current_time))
                    old['current_checkpoint'].update({
                        "time": current_time,
                        "checkpoint" : checkpoint
                    })
                    f.write(json.dumps(old))
            
        else:
            with open(log, "w") as f:
                log_id = str(random.randint(10000000000000000, 99999999999999999))
                mac = hex(uuid.getnode() >> 2)
                machine_id = str(mac)
                new = {
                        "log_id": log_id,
                        "machine_id" : machine_id,
                        "course_id": course_id,
                        "original_checkpoint":{"time":str(datetime.datetime.now()),"checkpoint":checkpoint},
                        "current_checkpoint" : {"time":str(datetime.datetime.now()),"checkpoint":checkpoint},
                        "diffs" : []
                        }                    
                f.write(json.dumps(new))
    
def start(watch_file, course_id = "NoCourseSpecified" ,IRB_consent = True):
    if IRB_consent:
        print('Logging your work!') 
        watcher = Watcher(watch_file, logger, base_filename=watch_file, course_id=course_id)
        log_file = watch_file.split('.')[0] + "_log.json"
        pusher = Pusher(log_file, push_log, log_filename=log_file)
        thread = threading.Thread(target=lambda: watcher.watch(), daemon=True)
        thread.start()
        thread_push = threading.Thread(target=lambda: pusher.watch(), daemon=True)
        thread_push.start()
    else:
        print('Please give consent to logging data by updating agreement variable to True')
        
def compress_log(watch_file):
    base_filename = watch_file
    filename_stem = base_filename.split('.')[0]
    src_path = os.path.realpath(base_filename)
    dir_path = os.path.dirname(src_path)
    log = Path(os.path.join(dir_path, filename_stem+'_log.json'))

    if log.is_file():  
        log_zip = zipfile.ZipFile(filename_stem+'.compressed', 'w')
        log_zip.write(log, filename_stem+'_log.json', compress_type=zipfile.ZIP_DEFLATED)
        log_zip.close()
        print('Compressed log to: ' + str(os.path.join(dir_path, filename_stem+'.compressed')))
    else:
        print('Log file not found. Nothing to compress.')
        pass

def compress_full_log(full_log_file, output_log_file):
    src_path = os.path.realpath(full_log_file)
    dir_path = os.path.dirname(src_path)
    
    log = full_log_file
    new_log = output_log_file
    old = None
    with open(log, 'r') as f:
        try:
            old = json.loads(f.read())
        except json.decoder.JSONDecodeError:
            print('There is an error decoding log. Log file may be corrupt')
            return
    compressed_log = None
    if len(old['checkpoints']) > 0:
        oldest_checkpoint = old['checkpoints'][0]
        newest_checkpoint = old['checkpoints'][len(old['checkpoints']) - 1]

        compressed_log = {"original_checkpoint":{"time":oldest_checkpoint['time'],"checkpoint": parse_checkpoint(oldest_checkpoint['checkpoint'])},
                    "current_checkpoint" : {"time":newest_checkpoint['time'],"checkpoint": parse_checkpoint(newest_checkpoint['checkpoint'])},
                    "diffs" : []
                    }
        for i in range(0, len(old['checkpoints']) - 1):
            current_time = old['checkpoints'][i+1]['time']
            compressed_log['diffs'].append(get_diff_dict(parse_checkpoint(old['checkpoints'][i]['checkpoint']), parse_checkpoint(old['checkpoints'][i + 1]['checkpoint']), current_time))
    else:
        return

    with open(new_log, "w") as g:                   
        g.write(json.dumps(compressed_log))
    return
    

def get_rebuilt_cells(previous_checkpoint_cells, diff_record):
    if diff_record['change_type'] == "all_cell_refresh":
        return diff_record['new_contents']

    all_current_cells = previous_checkpoint_cells.copy()
    for i in range(0, diff_record['num_changes']):
        cell_to_change = diff_record['cells_changed'][i]
        all_current_cells[cell_to_change] = diff_record['new_contents'][i]

    return all_current_cells


def decompress_compressed_log(compressed_log, output_full_file):
    compressed = None
    with open(compressed_log, 'r') as f:
        try:
            compressed = json.loads(f.read())
        except json.decoder.JSONDecodeError:
            print('There is an error decoding log. Log file may be corrupt')
            return

    full_log = {"checkpoints" : []}
    starting_checkpoint = compressed['original_checkpoint']
    full_log['checkpoints'].append(starting_checkpoint)
    for i in range(0, len(compressed['diffs'])):
        diff_info = compressed['diffs'][i]
        new_cell_array = get_rebuilt_cells(starting_checkpoint['checkpoint']['cells'], diff_info)
        checkpoint_dict = {}
        checkpoint_dict.update({
            "time" : diff_info['time'],
            "checkpoint" : {'cells' : new_cell_array,
                            'metadata' : starting_checkpoint['checkpoint']['metadata'],
                            'nbformat' : starting_checkpoint['checkpoint']['nbformat'],
                            'nbformat_minor': starting_checkpoint['checkpoint']['nbformat_minor']}
        })
        full_log['checkpoints'].append(checkpoint_dict)
        starting_checkpoint = checkpoint_dict
            
    with open(output_full_file, "w") as g:                   
        g.write(json.dumps(full_log))
    return
        
        
        