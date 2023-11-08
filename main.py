import gzip
import queue
import threading
import multiprocessing
import hashlib
# import yaml
import zlib

file_registry = {}
part_registry = {}
index = {}

file_id_counter = 0
part_id_counter = 0
lock = threading.Lock()

memory = 100000
nrpoc = 10
path = '/'

occupided = 0

# with open('config.yaml', 'r') as file:
#     config = yaml.safe_load(file)
#   memory = config['memory']


def add_file(file_name, status):
    global lock
    global file_registry
    global file_id_counter
    lock.acquire()
    file_id_counter += 1
    file_id = file_id_counter
    lock.release()
    file_registry[file_id] = {
        'file_name': file_name,
        'status': status,
    }
    return file_id


def remove_file(file_registry, file_id):
    if file_id in file_registry:
        del file_registry[file_id]


def update_status(file_registry, file_id, new_status):
    if file_id in file_registry:
        file_registry[file_id]['status'] = new_status


def get_file_info(file_id):
    global file_registry
    return file_registry.get(file_id)


def add_part(file_id, part_number, md5_hash):
    global part_registry
    global part_id_counter
    part_id_counter += 1
    part_registry[part_id_counter] = {
        'file_id': file_id,
        'part_number': part_number,
        'md5_hash': md5_hash,
    }
    if file_id in index:
        index[file_id].append(part_id_counter)
    else:
        index[file_id] = [part_id_counter]
    return part_id_counter

def process_file_part(part_id, data):
    md5_hash = hashlib.md5(data).hexdigest()
    print(f"MD5 hash {md5_hash}")
    compressed_data = zlib.compress(data)
    print(f"Compressed data {compressed_data}")

    with open(f'compressed_{part_id}.part', 'wb') as writer:
        writer.write(compressed_data)

    return md5_hash, compressed_data


def remove_part(part_id):
    global part_registry
    global index
    if part_id in part_registry:
        file_id = part_registry[part_id]['file_id']
        del part_registry[part_id]
        if file_id in index:
            index[file_id].remove(part_id)


def get_parts_of_file(file_id):
    global index
    return index.get(file_id, [])


def get_part_info(part_id):
    global part_registry
    return part_registry.get(part_id)

def process_file_part_get(part_id, md5_hash):
    #mormaon da znamo indeks unutar fajla, kad citamo
    with open(f'compressed_{part_id}.part', 'rb') as reader:
        compressed2 = reader.read()
    block2 = zlib.decompress(compressed2)
    digest2 = hashlib.md5(block2).hexdigest()
    # md5_hash2 = hashlib.md5(md5_hash.encode()).hexdigest()
    # md5_hash3 = part_registry[part_id]['md5_hash']
    # md5_hash3 = hashlib.md5(part_registry[part_id]['md5_hash'].encode()).hexdigest()
    if md5_hash != digest2:
        print("Usli u if")
        return False
        # result_queue.put("MD5 hesevi se ne pojklapaju")
    else:
        print("Usli u else")
        # result_queue.put(md5_hash)
        return block2


def komanda_put(file_name):
     global file_registry, part_registry, index
     file_id = add_file(file_name, "incomplete")
     part_number = 0
     nbytes = 6  # Veličina bloka za čitanje
     with open(file_name, 'rb') as reader:
         while True:
             part_number += 1
             # occupied += 6
             data = reader.read(nbytes)
             if not data:
                 break

             part_id = add_part(file_id, part_number, None)  # MD5 će biti postavljen kasnije

             # Obrada dela fajla
             md5_hash, _ = process_file_part(part_id, data)
             # occupied -= 6

             part_registry[part_id]['md5_hash'] = md5_hash  # Dodavanje MD5 hasha u registry
             # index[file_id].append(part_id)

     file_registry[file_id]['status'] = "complete"  # Ažuriranje statusa fajla na "complete"
     print(file_registry)
     print(part_registry)
     print(index)


def komanda_get(file_id,ui_processes):
    global file_registry
    file_info = get_file_info(file_id)
    if file_info['status'] != "complete":
        print("Fajl nije spreman za obradu")
        return
    if not file_info:
        print("Fajl nije pronadjen")
        return


    parts = get_parts_of_file(file_id)
    file_name = file_info['file_name']

    # result_queue = queue.Queue()
    with open(file_name+"balh", "wb") as output_file:
        parts = []
        ## ress = []
        for part_id in parts:
            ## if ress[0].is_ready():
            ##    res = ress.pop(0).get()
            ##    if isinstance(res, str):
            ##       print("Pronadjena je greska")
            ##    else:
            ##       print("uso u else")
            ##       output_file.write(res)

            part_info = get_part_info(part_id)
            print(part_info)

            # parts.append((part_id, part_info['md5_hash']))
            # if len(parts==5):
            #     ress = ui_processes.map(process_file_part_get, parts)
            res  = ui_processes.apply_async(process_file_part_get, args=(part_id, part_info['md5_hash']))##.get()
            ## ress.append(res)
            #process_file_part_get(part_id, part_info['md5_hash'])
            #result = result_queue.get()

            if isinstance(res, str):
               print("Pronadjena je greska")
            else:
                print("uso u else")
                output_file.write(res)


def komanda_delete(file_id):
    print("Komanda delete")


def komanda_list():
    print("Komanda list")


def komande():
    ui_processes = multiprocessing.Pool(processes=20)

    while True:
        komanda = input("Unesite komandu: ")
        if komanda.startswith("put"):
            file_name = komanda.split()[1]
            put_komanda_thread = threading.Thread(target=komanda_put, args=(file_name,))
            put_komanda_thread.start()
        if komanda.startswith("get"):
            broj = komanda.split()[1]
            file_id = int(broj)
            get_komanda_thread = threading.Thread(target=komanda_get, args=(file_id, ui_processes))
            get_komanda_thread.start()
        if komanda.startswith("delete"):
            file_id = komanda.split()[1]
            delete_komanda_thread = threading.Thread(target=komanda_delete, args=(file_id))
            delete_komanda_thread.start()
        if komanda.startswith("list"):
            list_komanda_thread = threading.Thread(target=komanda_list)
            list_komanda_thread.start()
        if komanda.startswith("exit"):
            break


if __name__ == "__main__":
    komande()
