from ComDDS.Writter_Agente import conexxion_Datos
from ComDDS.Writter_Agente import conexxion_Status
from ComDDS.Listener_Agente import suscriptor_sendInfo
from ComDDS.Listener_Agente import suscriptor_LamadaRequest
from ComDDS.Listener_Agente import suscriptor_kill
import threading

def main():

    global publi_datos
    global publi_status
    global suscrip_sendinfo
    global suscrip_kill
    global suscrip_llam_query

    while True:
        try:
             #modbus_client = ModbusClient("172.18.136.193", 502)
             print("intentando conectarse")
             #modbus_client.connect()
             break
        except:
             print("Prueba con otra IP")

    suscrip_kill = threading.Thread(target=suscriptor_kill, name="suscrip_kill")
    print('BEFORE:', suscrip_kill, suscrip_kill.is_alive())
    publi_datos = threading.Thread(target=conexxion_Datos, name ="publi_datos")
    print('BEFORE:', publi_datos, publi_datos.is_alive())
    publi_status = threading.Thread(target=conexxion_Status, name ="publi_status")
    print('BEFORE:', publi_status, publi_status.is_alive())
    suscrip_sendinfo = threading.Thread(target=suscriptor_sendInfo, name ="suscrip_sendinfo")
    print('BEFORE:', suscrip_sendinfo, suscrip_sendinfo.is_alive())
    suscrip_llam_query = threading.Thread(target=suscriptor_LamadaRequest, name ="suscrip_llam_query")
    print('BEFORE:', suscrip_llam_query, suscrip_llam_query.is_alive())

    suscrip_kill.do_run = True
    publi_datos.do_run = True
    publi_status.do_run = True
    suscrip_sendinfo.do_run = True
    suscrip_llam_query.do_run = True

    suscrip_kill.start()
    print('DURING:', suscrip_kill, suscrip_kill.is_alive())
    publi_datos.start()
    print('DURING:', publi_datos, publi_datos.is_alive())
    publi_status.start()
    print('DURING:', publi_status, publi_status.is_alive())
    suscrip_sendinfo.start()
    print('DURING:', suscrip_sendinfo, suscrip_sendinfo.is_alive())
    suscrip_llam_query.start()
    print('DURING:', suscrip_llam_query, suscrip_llam_query.is_alive())


    suscrip_kill.join()
    print('JOINED:', suscrip_kill, suscrip_kill.is_alive())
    suscrip_sendinfo.join()
    print('JOINED:', suscrip_sendinfo, suscrip_sendinfo.is_alive())
    suscrip_llam_query.join()
    print('JOINED:', suscrip_llam_query, suscrip_llam_query.is_alive())
    publi_datos.join()
    print('JOINED:', publi_datos, publi_datos.is_alive())
    publi_status.join()
    print('JOINED:', publi_status, publi_status.is_alive())


if __name__ == "__main__":
    main()
