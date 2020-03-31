import ddsutil
from dds import *
import threading


# Data available listener
class DataAvailableListener(Listener):
    def __init__(self):
        Listener.__init__(self)

    def on_data_available(self, entity):
        print('on_data_available called')
        l = entity.read(10)
        for (sd, si) in l:
            sd.print_vars()

def main():

    global sus_datos
    global sus_status

    sus_datos = threading.Thread(target=suscriptor_datix, name="sus_datos")
    print('BEFORE:', sus_datos, sus_datos.is_alive())
    sus_status = threading.Thread(target=suscriptor_statix, name="sus_status")
    print('BEFORE:', sus_status, sus_status.is_alive())

    sus_datos.do_run = True
    sus_status.do_run = True

    sus_datos.start()
    print('DURING:', sus_datos, sus_datos.is_alive())
    sus_status.start()
    print('DURING:', sus_status, sus_status.is_alive())

    sus_datos.join()
    print('JOINED:', sus_datos, sus_datos.is_alive())
    sus_status.join()
    print('JOINED:', sus_status, sus_status.is_alive())

    if (sus_status.is_alive == False) and (sus_status.is_alive == False):
        print("el programa a terminado")

def suscriptor_datix():

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

    sus_datos = threading.current_thread()
    while getattr(sus_datos, "do_run", True):

        qp = QosProvider('file://DDS_DefaultQoS_All.xml', 'DDS DefaultQosProfile')

        # Create participant
        dp = DomainParticipant(qos = qp.get_participant_qos())

        # Create Subscriber
        sub = dp.create_subscriber(qos = Qos([DurabilityQosPolicy(DDSDurabilityKind.TRANSIENT), ReliabilityQosPolicy(DDSReliabilityKind.RELIABLE), PartitionQosPolicy([Partition])]))

        # Generate python classes from IDL file
        gen_info = ddsutil.get_dds_classes_from_idl('LecturaModBusNumerico.idl',
                                                'com::ulma::supervisor::dds::agents::lecturaModBusNumerico::LecturaModBusNumerico')

        # Type support class
        topic = gen_info.register_topic(dp, "LecturaModBusNumerico", qp.get_topic_qos())

        #Create a reader
        readerQos = qp.get_reader_qos()
        reader = sub.create_datareader(topic, readerQos)

        # Type support class
        AgentType = gen_info.get_class("com::ulma::supervisor::dds::agents::lecturaModBusNumerico::AgentReferenceType")

        time.sleep(1)

        # Create waitset
        waitset = WaitSet()
        condition = ReadCondition(reader, DDSMaskUtil.all_samples())

        waitset.attach(condition)

        # Wait for data
        conditions = waitset.wait()

        # Print data
        while sus_datos.do_run == True:
            listaSamples = reader.take(10)
            for (sampleData, sampleInformation) in listaSamples:
                if sampleInformation.valid_data:
                    ldDestinatario = sampleData.src_id.ld
                    lnDestinatario = sampleData.src_id.ln
                    print ("Datos recibido: [LD: {}; LN: {}]".format(ldDestinatario, lnDestinatario))
                    if (ldDestinatario == AgenteLD) or (lnDestinatario == AgenteLN):
                        print ("Es para mi")
                    else:
                        print ("Este dato no es para mi. Es para :  [LD: {}; LN: {}]".format(ldDestinatario, lnDestinatario))
                        sus_datos.do_run = False

def suscriptor_statix():
    sus_status = threading.current_thread()
    while getattr(sus_status, "do_run", True):

        qp = QosProvider('file://DDS_DefaultQoS_All.xml', 'DDS DefaultQosProfile')

        # Create participant
        dp = DomainParticipant(qos=qp.get_participant_qos())

        # Create Subscriber
        sub = dp.create_subscriber(qos = Qos([DurabilityQosPolicy(DDSDurabilityKind.TRANSIENT), ReliabilityQosPolicy(DDSReliabilityKind.RELIABLE), PartitionQosPolicy([Partition])]))

        # Generate python classes from IDL file
        gen_info = ddsutil.get_dds_classes_from_idl('status.idl',
                                                'com::ulma::supervisor::dds::status::Status')
        # Type support class
        topic = gen_info.register_topic(dp, "Status", qp.get_topic_qos())

        # Create a reader
        readerQos = qp.get_reader_qos()
        reader = sub.create_datareader(topic, readerQos)


        AgentType = gen_info.get_class("com::ulma::supervisor::dds::status::AgentReferenceType")

        time.sleep(1)

        # Create waitset
        waitset = WaitSet()
        condition = ReadCondition(reader, DDSMaskUtil.all_samples())

        waitset.attach(condition)

        # Wait for data
        conditions = waitset.wait()

        # Print data
        while sus_status.do_run == True:
            listaSamples = reader.take(10)
            for (sampleData, sampleInformation) in listaSamples:
                if sampleInformation.valid_data:
                    ldDestinatario = sampleData.src_id.ld
                    lnDestinatario = sampleData.src_id.ln
                    print("Status recibido: [LD: {}; LN: {}]".format(ldDestinatario, lnDestinatario))
                    if (ldDestinatario == AgenteLD) and (lnDestinatario == AgenteLN):
                        print("Es para mi")
                    else:
                        print("Este status no es para mi. Es para :  [LD: {}; LN: {}]".format(ldDestinatario, lnDestinatario))
                        sus_status.do_run = False

if __name__ == "__main__":

    main()
