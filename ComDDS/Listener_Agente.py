from dds import *
import ddsutil
import threading
import importlib
import socket
import time
import sys


def suscriptor_sendInfo():

    suscrip_sendinfo = threading.current_thread()
    while getattr(suscrip_sendinfo, "do_run", True):
        qosprovider = QosProvider('file://DDS_DefaultQoS_All.xml', 'DDS DefaultQosProfile')

        # Create participant
        domainparticipant = DomainParticipant(qos = qosprovider.get_participant_qos())

        # Create Subscriber
        sub = domainparticipant.create_subscriber(qos = Qos([DurabilityQosPolicy(DDSDurabilityKind.TRANSIENT), ReliabilityQosPolicy(DDSReliabilityKind.RELIABLE),
                                                         PartitionQosPolicy([Partition])]))

        # Generate python classes from IDL file
        gen_info = ddsutil.get_dds_classes_from_idl('sendInfo.idl',
                                                'com::ulma::supervisor::dds::sendInfo::SendInfo')
        # Type support class
        topic = gen_info.register_topic(domainparticipant, "SendInfo", qosprovider.get_topic_qos())

        #Create a reader
        readerQos = qosprovider.get_reader_qos()
        reader = sub.create_datareader(topic, readerQos)

        # Type support class
        AgentType = gen_info.get_class("com::ulma::supervisor::dds::sendInfo::AgentReferenceType")

        time.sleep(1)

        # Create waitset
        waitset = WaitSet()
        condition = ReadCondition(reader, DDSMaskUtil.all_samples())

        waitset.attach(condition)

        # Wait for data
        conditions = waitset.wait()

        # Print data
        while suscrip_kill.do_run == True:
            listaSamples = reader.take(10)
            for (sampleData, sampleInformation) in listaSamples:
                if sampleInformation.valid_data:
                    ldDestinatario = sampleData.dst_agent_id.ld
                    lnDestinatario = sampleData.dst_agent_id.ln
                    print ("SENDInfo recibido: [LD: {}; LN: {}]".format(ldDestinatario, lnDestinatario))
                    if (ldDestinatario == AgenteLD) and (lnDestinatario == AgenteLN):
                        print ("Es para mi")
                    else:
                        print ("Este SENDInfo no es para mi. Es para :  [LD: {}; LN: {}]".format(ldDestinatario, lnDestinatario))
                        suscrip_sendinfo.do_run = False

def suscriptor_kill():
    suscrip_kill = threading.current_thread()
    while getattr(suscrip_kill, "do_run", True):
        qosprovider = QosProvider('file://DDS_DefaultQoS_All.xml', 'DDS DefaultQosProfile')

        # Create participant
        domainparticipant = DomainParticipant(qos = qosprovider.get_participant_qos())

        # Create Subscriber
        sub = domainparticipant.create_subscriber(qos = Qos([DurabilityQosPolicy(DDSDurabilityKind.TRANSIENT), ReliabilityQosPolicy(DDSReliabilityKind.RELIABLE),
                                                         PartitionQosPolicy([Partition])]))

        # Generate python classes from IDL file
        gen_info = ddsutil.get_dds_classes_from_idl('kill.idl',
                                                'com::ulma::supervisor::dds::kill::Kill')
        # Type support class
        topic = gen_info.register_topic(domainparticipant, "Kill", qosprovider.get_topic_qos())

        #Create a reader
        readerQos = qosprovider.get_reader_qos()
        reader = sub.create_datareader(topic, readerQos)

        # Type support class
        AgentType = gen_info.get_class("com::ulma::supervisor::dds::kill::AgentReferenceType")

        time.sleep(1)

        # Create waitset
        waitset = WaitSet()
        condition = ReadCondition(reader, DDSMaskUtil.all_samples())

        waitset.attach(condition)

        # Wait for data
        conditions = waitset.wait()

        # Print data
        while suscrip_kill.do_run == True:
            listaSamples = reader.take(10)
            for (sampleData, sampleInformation) in listaSamples:
                if sampleInformation.valid_data:
                    ldDestinatario = sampleData.dst_agent_id.ld
                    lnDestinatario = sampleData.dst_agent_id.ln
                    print ("Kill recibido: [LD: {}; LN: {}]".format(ldDestinatario, lnDestinatario))
                    if (ldDestinatario == AgenteLD) and (lnDestinatario == AgenteLN):
                        print ("Es para mi")
                    else:
                        print ("Este kill no es para mi. Es para :  [LD: {}; LN: {}]".format(ldDestinatario, lnDestinatario))
                        suscrip_kill.do_run = False
                        print('do run a false:', suscrip_kill, suscrip_kill.do_run)
                        publi_datos.do_run = False
                        print('do run a false:', publi_datos, publi_datos.do_run)
                        publi_status.do_run = False
                        print('do run a false:', publi_status, publi_status.do_run)
                        suscrip_sendinfo.do_run = False
                        print('do run a false:', suscrip_sendinfo, suscrip_sendinfo.do_run)
                        suscrip_llam_query.do_run = False
                        print('do run a false:', suscrip_llam_query, suscrip_llam_query.do_run)
                        print("todos los hilos han muerto")

def suscriptor_LamadaRequest():
    suscrip_llam_query = threading.current_thread()
    while getattr(suscrip_llam_query, "do_run", True):
        qosprovider = QosProvider('file://DDS_DefaultQoS_All.xml', 'DDS DefaultQosProfile')

        # Create participant
        domainparticipant = DomainParticipant(qos = qosprovider.get_participant_qos())

        # Create Subscriber
        sub = domainparticipant.create_subscriber(qos = Qos([DurabilityQosPolicy(DDSDurabilityKind.TRANSIENT), ReliabilityQosPolicy(DDSReliabilityKind.RELIABLE),
                                                         PartitionQosPolicy([Partition])]))

        # Generate python classes from IDL file
        gen_info = ddsutil.get_dds_classes_from_idl('LauncherReq.idl',
                                                'com::ulma::supervisor::dds::launcherReq::LauncherReq')
        # Type support class
        topic = gen_info.register_topic(domainparticipant, "LauncherReq", qosprovider.get_topic_qos())

        #Create a reader
        readerQos = qosprovider.get_reader_qos()
        reader = sub.create_datareader(topic, readerQos)


        AgentType = gen_info.get_class("com::ulma::supervisor::dds::launcherReq::AgentReferenceType")

        time.sleep(1)

        # Create waitset
        waitset = WaitSet()
        condition = ReadCondition(reader, DDSMaskUtil.all_samples())

        waitset.attach(condition)

        # Wait for data
        conditions = waitset.wait()

        # Print data
        while suscrip_kill.do_run == True:
            listaSamples = reader.take(10)
            for (sampleData, sampleInformation) in listaSamples:
                if sampleInformation.valid_data:
                    ldDestinatario = sampleData.dst_id.ld
                    lnDestinatario = sampleData.dst_id.ln
                    print ("llamadaRequest recibido: [LD: {}; LN: {}]".format(ldDestinatario, lnDestinatario))
                    if (ldDestinatario == AgenteLD) and (lnDestinatario == AgenteLN):
                        print ("Es para mi")
                    else:
                        print ("Esta llamadaRequest no es para mi. Es para :  [LD: {}; LN: {}]".format(ldDestinatario, lnDestinatario))
                        suscrip_llam_query.do_run = False
