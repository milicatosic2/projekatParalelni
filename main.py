import threading

def komanda_put():
    print("Komanda put")

def komanda_get(file_id):
    print("Komanda get")

def komanda_delete(file_id):
    print("Komanda delete")

def komanda_list():
    print("Komanda list")

def komande():
    while True:
        komanda = input("Unesite komandu: ")
        if komanda == "put":
            put_komanda_thread = threading.Thread(target = komanda_put)
            put_komanda_thread.start()
        if komanda.startswith("get") :
            file_id = komanda.split()[1]
            get_komanda_thread = threading.Thread(target = komanda_get,args = (file_id))
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