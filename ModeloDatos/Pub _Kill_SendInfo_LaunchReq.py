from dds import *
import ddsutil
import time
import multiprocessing


def main():

    publi_sendinfo = multiprocessing.Process(target=conexxion_sendInfo, name="publi_sendinfo")
    print('BEFORE:', publi_sendinfo, publi_sendinfo.is_alive())
    publi_kill = multiprocessing.Process(target=conexxion_Kill, name="publi_kill")
    print('BEFORE:', publi_kill, publi_kill.is_alive())
    publi_request = multiprocessing.Process(target=conexxion_LlamadaRequest, name="publi_request")
    print('BEFORE:', publi_request, publi_request.is_alive())

    publi_sendinfo.do_run = True
    publi_kill.do_run = True
    publi_request.do_run = True

    publi_sendinfo.start()
    print('DURING:', publi_sendinfo, publi_sendinfo.is_alive())
    publi_kill.start()
    print('DURING:', publi_kill, publi_kill.is_alive())
    publi_request.start()
    print('DURING:', publi_request, publi_request.is_alive())

    time.sleep(20)

    publi_sendinfo.do_run = False
    publi_kill.do_run = False
    publi_request.do_run = False

    publi_sendinfo.terminate()
    print('TERMINATED:',publi_sendinfo, publi_sendinfo.is_alive())
    publi_kill.terminate()
    print('TERMINATED:', publi_kill, publi_kill.is_alive())
    publi_request.terminate()
    print('TERMINATED:', publi_request, publi_request.is_alive())

    publi_sendinfo.join()
    print('JOINED:', publi_sendinfo, publi_sendinfo.is_alive())
    publi_kill.join()
    print('JOINED:', publi_kill, publi_kill.is_alive())
    publi_request.join()
    print('JOINED:', publi_request, publi_request.is_alive())

def conexxion_sendInfo():
    publi_sendinfo = multiprocessing.current_process()
    while getattr(publi_sendinfo, "do_run", True):
        qosprovider = QosProvider('file://DDS_DefaultQoS_All.xml', 'DDS DefaultQosProfile')

        # Create participant
        domainparticipant = DomainParticipant(qos=qosprovider.get_participant_qos())

        # Create publisher with type suport class(dentro del parentersis)
        pub = domainparticipant.create_publisher(qos=Qos([DurabilityQosPolicy(DDSDurabilityKind.TRANSIENT), ReliabilityQosPolicy(DDSReliabilityKind.RELIABLE), PartitionQosPolicy(['the_partition'])]))

        # Generate python classes from IDL file
        gen_info = ddsutil.get_dds_classes_from_idl('sendInfo.idl',
                                                'com::ulma::supervisor::dds::sendInfo::SendInfo')
        # Type support class
        topic = gen_info.register_topic(domainparticipant, "SendInfo", qosprovider.get_topic_qos())

        AgentType = gen_info.get_class("com::ulma::supervisor::dds::sendInfo::AgentReferenceType")

        # Create a writer
        writer = pub.create_datawriter(topic)
        writer = pub.create_datawriter(topic, qosprovider.get_writer_qos())


        while publi_sendinfo.do_run == True:
            s_id = AgentType(ld= "modbus", ln="Mshuttle")
            d_id = AgentType(ld="Modbus", ln="MShuttle")
            sample = gen_info.topic_data_class(src_id=s_id, dst_agent_id=d_id)

            writer.write(sample)
            time.sleep(3.0)

            print("SendInfo")
            print(sample)

def conexxion_Kill():
    publi_kill = multiprocessing.current_process()
    while getattr(publi_kill, "do_run", True):
        qosprovider = QosProvider('file://DDS_DefaultQoS_All.xml', 'DDS DefaultQosProfile')

        # Create participant
        domainparticipant = DomainParticipant(qos=qosprovider.get_participant_qos())

        # Create publisher
        pub = domainparticipant.create_publisher(qos=Qos([DurabilityQosPolicy(DDSDurabilityKind.TRANSIENT), ReliabilityQosPolicy(DDSReliabilityKind.RELIABLE), PartitionQosPolicy(['the_partition'])]))

        # Generate python classes from IDL file
        gen_info = ddsutil.get_dds_classes_from_idl('kill.idl',
                                                'com::ulma::supervisor::dds::kill::Kill')
        # Type support class
        topic = gen_info.register_topic(domainparticipant, "Kill", qosprovider.get_topic_qos())

        AgentType = gen_info.get_class("com::ulma::supervisor::dds::kill::AgentReferenceType")

        # Create a writer
        writer = pub.create_datawriter(topic)
        writer = pub.create_datawriter(topic, qosprovider.get_writer_qos())


        while publi_kill.do_run == True:
            d_id = AgentType(ld="Modbus", ln="MSuttle")
            sample = gen_info.topic_data_class(dst_agent_id=d_id)

            writer.write(sample)
            time.sleep(3.0)

            print("kill")
            print(sample)

def conexxion_LlamadaRequest():
    publi_request = multiprocessing.current_process()
    while getattr(publi_request, "do_run", True):
        qosprovider = QosProvider('file://DDS_DefaultQoS_All.xml', 'DDS DefaultQosProfile')

        # Create participant
        domainparticipant = DomainParticipant(qos=qosprovider.get_participant_qos())

        # Create publisher
        pub = domainparticipant.create_publisher(qos=Qos([DurabilityQosPolicy(DDSDurabilityKind.TRANSIENT), ReliabilityQosPolicy(DDSReliabilityKind.RELIABLE), PartitionQosPolicy(['the_partition'])]))

        # Generate python classes from IDL file
        gen_info = ddsutil.get_dds_classes_from_idl('LauncherReq.idl',
                                                'com::ulma::supervisor::dds::launcherReq::LauncherReq')
        # Type support class
        topic = gen_info.register_topic(domainparticipant, "LauncherReq", qosprovider.get_topic_qos())

        AgentType = gen_info.get_class("com::ulma::supervisor::dds::launcherReq::AgentReferenceType")

        # Create a writer
        writer = pub.create_datawriter(topic)
        writer = pub.create_datawriter(topic, qosprovider.get_writer_qos())


        while publi_request.do_run == True:
            s_id = AgentType(ld="Mod", ln="bus")
            d_id = AgentType(ld="Modbus", ln="MShuttle")
            sample = gen_info.topic_data_class(src_id=s_id, dst_id=d_id, command=1, request_id=0, )

            writer.write(sample)
            time.sleep(3.0)

            print("LlamadaReq")
            print(sample)

if __name__ == "__main__":

    main()
