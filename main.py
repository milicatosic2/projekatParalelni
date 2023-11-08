import gzip
import queue
import threading
import multiprocessing
import hashlib
import yaml

file_registry = {}
part_registry = {}
index = {}

file_id_counter = 1
part_id_counter = 1

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

def add_file(file_name,status,additional_data = None):
    global file_id_counter
    file_id_counter += 1
    file_id = file_id_counter
    file_registry[file_id] = {
        'file_name' : file_name,
        'status' : status,
        'additional_data' : additional_data
    }
    return file_id_counter

def remove_file(file_registry,file_id):
    if file_id in file_registry:
        del file_registry[file_id]

def update_status(file_registry,file_id,new_status):
    if file_id in file_registry:
        file_registry[file_id]['status'] = new_status

def get_file_info(file_registry, file_id):
    return file_registry.get(file_id, None)

def add_part(file_id,part_number,md5_hash,additional_data = None):
    global part_id_counter
    part_id_counter += 1
    part_registry[part_id_counter] = {
        'file_id': file_id,
        'part_number': part_number,
        'md5_hash': md5_hash,
        'additional_data': additional_data
    }
    if file_id in index:
        index[file_id].append(part_id_counter)
    else:
        index[file_id] = [part_id_counter]

def process_file_part(data,file_id):

    md5_hash = hashlib.md5(data).hexdigest()
    print(f"MD5 hash: {md5_hash}")

    compressed_data = gzip.compress(data)
    print(f"Compressed data: {compressed_data}")

    # Vraćanje MD5 heša
    return md5_hash

def remove_part(part_registry, index, part_id):
    if part_id in part_registry:
        file_id = part_registry[part_id]['file_id']
        del part_registry[part_id]
        if file_id in index:
            index[file_id].remove(part_id)

def get_parts_of_file(index, file_id):
    return index.get(file_id, [])

def get_part_info(part_registry, part_id):
    return part_registry.get(part_id, None)

def komanda_put(part_registry,index,file_name,data,ui_processes):
    global file_registry;
    file_id = add_file(file_registry, file_name, "incomplete")
    part_number = 0
    for part_data in data:
        part_number += 1
        part_id = add_part(part_registry, index, file_id, part_number, None)  # MD5 će biti postavljen kasnije
        ui_process = ui_processes.apply_async(process_file_part, args=(part_id, part_data))
        ui_process.get()  # Očekuje se da će se vratiti nakon obrade
        md5_hash = hashlib.md5(part_data).hexdigest()
        part_registry[part_id]['md5_hash'] = md5_hash
    update_status(file_registry, file_id, "complete")

def process_file_part_get(part_id,part_data,result_queue):
    md5_hash = hashlib.md5(part_data).hexdigest()
    if md5_hash != part_registry[part_id]['md5_hash']:
        result_queue.put("MD5 hesevi se ne pojklapaju")
    else:
        result_queue.put(part_data)

def komanda_get(file_id,ui_processes):
    print(f"{file_id}")
    file_info = get_file_info(file_registry,file_id)
    if not file_info:
        print("Fajl nije pronadjen")
        return
    if file_info['status'] != "complete":
        print("Fajl nije spreman za obradu")
        return

    parts = get_parts_of_file(index,file_id)
    file_name = file_info['file_name']

    result_queue = queue.Queue()
    with open(file_name,"wb") as output_file:
        for part_id in parts:
            part_info = get_part_info(part_registry,part_id)
            ui_processes = ui_processes.apply_async(process_file_part_get,args = (part_id,part_info['data'],result_queue))
            result = result_queue.get()
            if isinstance(result,str):
                print("Pronadjena je greska")
            else:
                output_file.write(result)


def komanda_delete(file_id):
    print("Komanda delete")

def komanda_list():
    print("Komanda list")

def komande():
    ui_processes = multiprocessing.Pool(processes=5)
    while True:
        komanda = input("Unesite komandu: ")
        if komanda.startswith("put") :
            put_komanda_thread = threading.Thread(target = komanda_put)
            put_komanda_thread.start()
        if komanda.startswith("get") :
            file_id = komanda.split()[1]
            get_komanda_thread = threading.Thread(target = komanda_get,args = (file_id,ui_processes))
            get_komanda_thread.start()
        if komanda.startswith("delete"):
            file_id = komanda.split()[1]
            delete_komanda_thread = threading.Thread(target = komanda_delete,args = (file_id))
            delete_komanda_thread.start()
        if komanda == "list":
            list_komanda_thread = threading.Thread(target=komanda_list)
            list_komanda_thread.start()
        if komanda == "exit":
            break

if __name__ == "__main__":
    komande()
     # data = [b'data1', b'data2', b'data3']  # Ovo može biti stvarni deo fajla
    file_registry = {}
    part_registry = {}
    index = {}
    ui_processes = multiprocessing.Pool(processes=5)  # Ovo omogućava paralelnu obradu delova
    # file_name = input("Unesite put do fajla: ")
    # komanda_put(file_registry, part_registry, index, file_name, data, ui_processes)

    # print("File Registry:")
    # print(file_registry)
    # print("Part Registry:")
    # print(part_registry)
    # print("Index:")
    # print(index)
