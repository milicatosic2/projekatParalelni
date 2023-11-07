import threading
import multiprocessing
import hashlib
import zlib

file_registry = {}
part_registry = {}
index = {}

file_id_counter = 0
part_id_counter = 0


def add_file(file_name, status):
    global file_registry
    global file_id_counter
    file_id_counter += 1
    file_id = file_id_counter
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


def get_file_info(file_registry, file_id):
    return file_registry.get(file_id, None)


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
    compressed_data = zlib.compress(data)
    # Simulacija obrade - ovdje bi se mogla obaviti stvarna obrada
    # ...

    return md5_hash, compressed_data  # Vraća se MD5 i kompresovani podaci


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


def komanda_put(file_name):
     global file_registry, part_registry, index
     file_id = add_file(file_name, "incomplete")
     part_number = 0
     nbytes = 6  # Veličina bloka za čitanje
     with open(file_name, 'rb') as reader:
         while True:
             part_number += 1
             data = reader.read(nbytes)
             if not data:
                 break

             part_id = add_part(file_id, part_number, None)  # MD5 će biti postavljen kasnije

             # Obrada dela fajla
             md5_hash, compressed_data = process_file_part(part_id, data)

             part_registry[part_id]['md5_hash'] = md5_hash  # Dodavanje MD5 hasha u registry
             # index[file_id].append(part_id)

     file_registry[file_id]['status'] = "complete"  # Ažuriranje statusa fajla na "complete"
     print(file_registry)
     print(part_registry)
     print(index)


def komanda_get(file_id):
    print("Komanda get")
    with open('nadja.txt', 'rb') as file:
        data = file.read()
        print(data)


def komanda_delete(file_id):
    print("Komanda delete")


def komanda_list():
    print("Komanda list")


def komande():
    while True:
        komanda = input("Unesite komandu: ")
        if komanda.startswith("put"):
            file_name = komanda.split()[1]
            put_komanda_thread = threading.Thread(target=komanda_put, args=(file_name,))
            put_komanda_thread.start()
        if komanda.startswith("get"):
            file_id = komanda.split()[1]
            get_komanda_thread = threading.Thread(target=komanda_get, args=(file_id))
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
