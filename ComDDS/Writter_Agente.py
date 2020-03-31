from dds import *
import ddsutil
import threading
import importlib
import socket
import time
import sys

def conexxion_Datos():

    global Partition
    global AgenteLD
    global AgenteLN
    global Group
    global Redundancy
    global StatusAgente

    Partition = "the_partition"
    AgenteLD = "Modbus"
    AgenteLN = "MShuttle"
    Group = "0"
    Redundancy = "Master"
    StatusAgente = "OPERATINAL"

    publi_datos = threading.current_thread()
    while getattr(publi_datos, "do_run", True):
        qosprovider = QosProvider('file://DDS_DefaultQoS_All.xml', 'DDS DefaultQosProfile')

        # Create participant
        domainparticipant = DomainParticipant(qos=qosprovider.get_participant_qos())

        # Create publisher
        pub = domainparticipant.create_publisher(qos = Qos([DurabilityQosPolicy(DDSDurabilityKind.TRANSIENT), ReliabilityQosPolicy(DDSReliabilityKind.RELIABLE),
                                                        PartitionQosPolicy([Partition])]))

        # Generate python classes from IDL file
        gen_info = ddsutil.get_dds_classes_from_idl('LecturaModBusNumerico.idl',
                                                'com::ulma::supervisor::dds::agents::lecturaModBusNumerico::LecturaModBusNumerico')
        # Type support class
        topic = gen_info.register_topic(domainparticipant, "LecturaModBusNumerico", qosprovider.get_topic_qos())

        AgentType = gen_info.get_class("com::ulma::supervisor::dds::agents::lecturaModBusNumerico::AgentReferenceType")

        # Create a writer
        writer = pub.create_datawriter(topic)
        writer = pub.create_datawriter(topic, qosprovider.get_writer_qos())


    # ES UNA CONDICION PARA QUE NO PUBLIQUE NADA SI EL KILL ESTA ACTIVADO
    #if suscrip_kill.do_run == False:
        #publi_datos.do_run = False

        while suscrip_kill.do_run == True:
            #LO QUE ESTA COMENTADO SIRVE PARA TOMAR LOS HOLDING REGISTER DE UN DISPOSITIVO MODBUS AL QUE ESTEMOS COENCATDOS
             # n = 2999                                                       #Primer holding regitser
             # datos_lectura = modbus_client.read_holdingregisters(n, 70)     #Numero de holding registers
             # lista_variables = list()
             # for i in range(0, len(datos_lectura), 2):                      #Convertir los holding register a numeros enteros
             #      datoConvertido = convert_registers_to_float([datos_lectura[i], datos_lectura[i + 1]])
             #      lista_variables.append(datoConvertido)
             #Variables_central = {'Current_PhA:': lista_variables[0],  'Current_PhB:': lista_variables[1],
             #                     'Current_PhC:': lista_variables[2], 'Current_Avg:': lista_variables[5],
             #                     'Voltage_Avg:': lista_variables[18], 'Active_power:': lista_variables[30]}

            Variables_central = {'Current_PhA:': 2.4, 'Current_PhB:': 3.5,
                                  'Current_PhC:': 1.5, 'Current_Avg:': 3.3,
                                  'Voltage_Avg:': 230.4, 'Active_power:': 4.53}

            for variable in Variables_central:
                 id = AgentType(ld="Modbus", ln=str(variable))
                 dat_lec = Variables_central[variable]

                 sample = gen_info.topic_data_class(src_id=id, lectura=dat_lec)

                 writer.write(sample)
                 time.sleep(6.0)

                 print("datos de lectura")
                 print(sample)

def conexxion_Status():
    publi_status = threading.current_thread()
    while getattr(publi_status, "do_run", True):

        qosprovider = QosProvider('file://DDS_DefaultQoS_All.xml', 'DDS DefaultQosProfile')

        # Create participant
        domainparticipant = DomainParticipant(qos=qosprovider.get_participant_qos())

        # Create publisher
        pub = domainparticipant.create_publisher(qos = Qos([DurabilityQosPolicy(DDSDurabilityKind.TRANSIENT), ReliabilityQosPolicy(DDSReliabilityKind.RELIABLE),
                                                        PartitionQosPolicy([Partition])]))

        # Generate python classes from IDL file
        gen_info = ddsutil.get_dds_classes_from_idl('status.idl',
                                                'com::ulma::supervisor::dds::status::Status')
        # Type support class
        topic = gen_info.register_topic(domainparticipant, "Status", qosprovider.get_topic_qos())

        AgentType = gen_info.get_class("com::ulma::supervisor::dds::status::AgentReferenceType")

        # Create a writer
        writer = pub.create_datawriter(topic)
        writer = pub.create_datawriter(topic, qosprovider.get_writer_qos())

    # ES UNA CONDICION PARA QUE NO PUBLIQUE NADA SI EL KILL ESTA ACTIVADO
    #if suscrip_kill.do_run == False:
        #publi_status.do_run = False

        while suscrip_kill.do_run == True:

            id = AgentType(ld=AgenteLD, ln=AgenteLN)
            sample1 = gen_info.topic_data_class(src_id=id, status=StatusAgente, error_description=sys.argv[1], masterSlave=sys.argv[2])

            writer.write(sample1)
            time.sleep(6.0)

            print("status")
            print(sample1)
