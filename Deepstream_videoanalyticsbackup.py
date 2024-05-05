#!/usr/bin/env python3


import sys
sys.path.append('../')
import platform
import configparser

import gi
gi.require_version('Gst', '1.0')
from gi.repository import GLib, Gst
import os
import cv2
import numpy as np


import uuid
import socket
import pyds
import os.path
from os import path
sys.path.append('../')
from datetime import datetime

import psutil
import GPUtil

import threading
import json


import queue
import asyncio
from websockets.server import serve
import base64

import mysql.connector

def is_aarch64():
    return platform.uname()[4] == 'aarch64'

sys.path.append('/opt/nvidia/deepstream/deepstream/lib')


from PIL import Image



def bus_call(bus, message, loop):
    t = message.type
    if t == Gst.MessageType.EOS:
        sys.stdout.write("End-of-stream\n")
        loop.quit()
    elif t==Gst.MessageType.WARNING:
        err, debug = message.parse_warning()
        sys.stderr.write("Warning: %s: %s\n" % (err, debug))
    elif t == Gst.MessageType.ERROR:
        err, debug = message.parse_error()
        sys.stderr.write("Error: %s: %s\n" % (err, debug))
        loop.quit()
    return True




past_tracking_meta=[0]
past_tracking_meta[0]=1
fps_streams={}
frame_count={}
saved_count={}



Sgie1_classes_str = ("black", "blue", "brown", "gold", "green","grey", "maroon",
                             "orange", "red", "silver", "white", "yellow")


Sgie2_classes_str = ("acura", "audi", "bmw", "chevrolet", "chrysler",
                     "dodge", "dord", "gmc", "honda", "hyundai", "infiniti", "jeep", "kia",
                     "lexus", "mazda", "mercedes", "nissan","subaru", "toyota", "volkswagen")


Sgie3_classes_str = ("coupe", "largevehicle", "sedan", "suv","truck", "van")




PGIE_CLASS_ID_VEHICLE = 0
PGIE_CLASS_ID_BICYCLE = 1
PGIE_CLASS_ID_PERSON = 2
PGIE_CLASS_ID_ROADSIGN = 3

def image_to_blob(img: np.ndarray) -> bytes:
    """Given a numpy 2D array, returns a JPEG image in binary format suitable for a BLOB column."""
    
    img_buffer = cv2.imencode('.jpg', img)[1]
    return img_buffer.tobytes()

def get_image_blob(volume):
    return image_to_blob(volume)

def image_to_base64(img: np.ndarray) -> bytes:
    """ Given a numpy 2D array, returns a JPEG image in base64 format """

    # using opencv 2, there are others ways
    img_buffer = cv2.imencode('.jpg', img)[1]
    return base64.b64encode(img_buffer).decode('utf-8')
    
def get_image_base64(volume):
    return image_to_base64(volume)


# Establish a connection to the database
conn = mysql.connector.connect(
    database="PFE",
    user="root",
    password="",
    host="85.215.230.8",
)
cursor = conn.cursor()


async def echo(websocket):
    while True:
        cpu = psutil.cpu_percent(percpu=False,interval=None)
        ram = psutil.virtual_memory().percent
        gpu = GPUtil.getGPUs()[0].load

        person_count = obj_counter[PGIE_CLASS_ID_PERSON]
        vehicle_count = obj_counter[PGIE_CLASS_ID_VEHICLE]

        now = datetime.now()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        vehicles = vehicle_arr

        image = get_image_base64(img)

        data = {
                "image" : image,
                "resourceUsage" : {
                    "cpu": cpu,
                    "ram": ram,
                    "gpu": gpu
                },
                "detections" : {
                    "timestamp" : current_time,
                    "person_count": person_count,
                    "vehicle_count": vehicle_count,
                    "vehicles" : vehicles
                }
        }

        await websocket.send(json.dumps(data)) # send data to websocket

async def mains():
    async with serve(echo, ip, port):
        await asyncio.Future()

detection_queue = queue.Queue()

def database_worker():
    """Thread worker function that inserts detections into a database."""
    while True:
        try:
            # Block until an item becomes available
            detection = detection_queue.get(timeout=10)
            
            if detection == 'STOP':
                break

            detection_id, cameraIP, regionalIP, image, location, timestamp, person_count, vehicle_count = detection['main']
            
            cursor.execute("""
                INSERT INTO detections (id, cameraIP, regionalIP, image, location, timestamp, person_count, vehicle_count) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (detection_id, cameraIP, regionalIP, image, location, timestamp, person_count, vehicle_count))

            for vehicle in detection['vehicles']:
                cursor.execute("""
                    INSERT INTO vehicles (detection_id, color, maker, type) 
                    VALUES (%s, %s, %s, %s);
                """, (detection_id, vehicle["color"], vehicle["make"], vehicle["type"]))

            conn.commit()

        except queue.Empty:
            pass

# Start the database thread
threading.Thread(target=database_worker, daemon=True).start()


def osd_sink_pad_buffer_probe(pad,info,u_data):

    frame_number=0
    #Intiallizing object counter with 0.
    global obj_counter
    obj_counter = {
        PGIE_CLASS_ID_VEHICLE:0,
        PGIE_CLASS_ID_PERSON:0,
        PGIE_CLASS_ID_BICYCLE:0,
        PGIE_CLASS_ID_ROADSIGN:0
    }
    num_rects=0
    gst_buffer = info.get_buffer()
    if not gst_buffer:
        print("Unable to get GstBuffer ")
        return
    
    # Retrieve batch metadata from the gst_buffer
    # Note that pyds.gst_buffer_get_nvds_batch_meta() expects the
    # C address of gst_buffer as input, which is obtained with hash(gst_buffer)
    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    l_frame = batch_meta.frame_meta_list
    while l_frame is not None:

        vehicle = {
            "color": "",
            "make": "",
            "type": ""
        }

        global vehicle_arr
        vehicle_arr = []


        try:
            # Note that l_frame.data needs a cast to pyds.NvDsFrameMeta
            # The casting is done by pyds.NvDsFrameMeta.cast()
            # The casting also keeps ownership of the underlying memory
            # in the C code, so the Python garbage collector will leave
            # it alone.
            frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
        except StopIteration:
            break   
          
        l_obj = frame_meta.obj_meta_list
        n_frame = pyds.get_nvds_buf_surface(hash(gst_buffer), frame_meta.batch_id)

        global img
        frame_copy = np.array(n_frame, copy=True, order='C')
        img = cv2.cvtColor(frame_copy, cv2.COLOR_RGBA2BGRA)

        while l_obj is not None:
            try:
                obj_meta=pyds.NvDsObjectMeta.cast(l_obj.data)
            except StopIteration:
                break
            # Increment obj_counter according to detected objects
            obj_counter[obj_meta.class_id] += 1

            
            l = obj_meta.classifier_meta_list   
            while l is not None:
                
                classifierMeta = pyds.NvDsClassifierMeta.cast(l.data)
                n = classifierMeta.label_info_list
                while n is not None:
                    labelInfo = pyds.NvDsLabelInfo.cast(n.data) 
                    print(labelInfo.result_label)
                    if (labelInfo.result_label in Sgie1_classes_str):
                        vehicle["color"] = labelInfo.result_label
                    if (labelInfo.result_label in Sgie2_classes_str):
                        vehicle["make"] = labelInfo.result_label
                    if (labelInfo.result_label in Sgie3_classes_str):
                        vehicle["type"] = labelInfo.result_label
                    try:
                        n = n.next
                    except StopIteration:
                        break
                try:
                    l = l.next
                except StopIteration:
                    break   
            try: 
                l_obj=l_obj.next
            except StopIteration:
                break
            vehicle_arr.append(vehicle)          
        # Acquiring a display meta object. The memory ownership remains in
        # the C code so downstream plugins can still access it. Otherwise
        # the garbage collector will claim it when this probe function exits.
        display_meta=pyds.nvds_acquire_display_meta_from_pool(batch_meta)
        display_meta.num_labels = 1
        py_nvosd_text_params = display_meta.text_params[0]
        # Setting display text to be shown on screen
        # Note that the pyds module allocates a buffer for the string, and the
        # memory will not be claimed by the garbage collector.
        # Reading the display_text field here will return the C address of the
        # allocated string. Use pyds.get_string() to get the string content.
        py_nvosd_text_params.display_text = "Frame Number={} Number of Objects={} Vehicle_count={} Person_count={}".format(frame_number, num_rects, obj_counter[PGIE_CLASS_ID_VEHICLE], obj_counter[PGIE_CLASS_ID_PERSON])

        # Now set the offsets where the string should appear
        py_nvosd_text_params.x_offset = 10
        py_nvosd_text_params.y_offset = 12

        # Font , font-color and font-size
        py_nvosd_text_params.font_params.font_name = "Serif"
        py_nvosd_text_params.font_params.font_size = 10
        # set(red, green, blue, alpha); set to White
        py_nvosd_text_params.font_params.font_color.set(1.0, 1.0, 1.0, 1.0)

        # Text background color
        py_nvosd_text_params.set_bg_clr = 1
        # set(red, green, blue, alpha); set to Black
        py_nvosd_text_params.text_bg_clr.set(0.0, 0.0, 0.0, 1.0)
        # Using pyds.get_string() to get display_text as string
        print(pyds.get_string(py_nvosd_text_params.display_text))
        pyds.nvds_add_display_meta_to_frame(frame_meta, display_meta)

        person_count = obj_counter[PGIE_CLASS_ID_PERSON]
        vehicle_count = obj_counter[PGIE_CLASS_ID_VEHICLE]

        now = datetime.now()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        vehicles = vehicle_arr

        image = get_image_blob(img)

        data = {
                "image" : image,
                "detections" : {
                    "timestamp" : current_time,
                    "person_count": person_count,
                    "vehicle_count": vehicle_count,
                    "vehicles" : vehicles
                }
        }



        timestamp = data["detections"]["timestamp"]
        person_count = data["detections"]["person_count"]
        vehicle_count = data["detections"]["vehicle_count"]
        vehicles = data["detections"]["vehicles"]
        image = data["image"]

        # Assuming you have cameraId, cameraIP, and regionalIP values somewhere
        detectionid = str(uuid.uuid4())
        hostname = socket.gethostname()
        IPAddr = socket.gethostbyname(hostname)
        cameraIP = IPAddr
        regionalIP = IPAddr
        location = "vod"  # replace with actual location value
        
        detection_data = {
        'main': (detectionid, cameraIP, regionalIP, image, location, timestamp, person_count, vehicle_count),
        'vehicles': vehicle_arr
        }
        detection_queue.put(detection_data)
        try:
            l_frame=l_frame.next
        except StopIteration:
            break
    return Gst.PadProbeReturn.OK	


    
def run_asyncio_loop():
    asyncio.run(mains())
        
    

def main(args):

    global ip 
    ip = "localhost"

    global port
    port = 8484

    # Check input arguments
    if(len(args)<2):
        sys.stderr.write("usage: %s <h264_elementary_stream> <host> <port>\n" % args[0])
        sys.exit(1)

        
    # Create a thread for running asyncio loop
    async_thread = threading.Thread(target=run_asyncio_loop)




    # Start the thread
    async_thread.start()


    # Standard GStreamer initialization
    Gst.init(None)

    # Create gstreamer elements
    # Create Pipeline element that will form a connection of other elements
    print("Creating Pipeline \n ")
    pipeline = Gst.Pipeline()

    if not pipeline:
        sys.stderr.write(" Unable to create Pipeline \n")

    # Source element for reading from the file
    print("Creating Source \n ")
    source = Gst.ElementFactory.make("filesrc", "file-source")
    if not source:
        sys.stderr.write(" Unable to create Source \n")

    # Since the data format in the input file is elementary h264 stream,
    # we need a h264parser
    print("Creating H264Parser \n")
    h264parser = Gst.ElementFactory.make("h264parse", "h264-parser")
    if not h264parser:
        sys.stderr.write(" Unable to create h264 parser \n")

    # Use nvdec_h264 for hardware accelerated decode on GPU
    print("Creating Decoder \n")
    decoder = Gst.ElementFactory.make("nvv4l2decoder", "nvv4l2-decoder")
    if not decoder:
        sys.stderr.write(" Unable to create Nvv4l2 Decoder \n")

    # Create nvstreammux instance to form batches from one or more sources.
    streammux = Gst.ElementFactory.make("nvstreammux", "Stream-muxer")
    if not streammux:
        sys.stderr.write(" Unable to create NvStreamMux \n")

    # Use nvinfer to run inferencing on decoder's output,
    # behaviour of inferencing is set through config file
    pgie = Gst.ElementFactory.make("nvinfer", "primary-inference")
    if not pgie:
        sys.stderr.write(" Unable to create pgie \n")

    tracker = Gst.ElementFactory.make("nvtracker", "tracker")
    if not tracker:
        sys.stderr.write(" Unable to create tracker \n")

    sgie1 = Gst.ElementFactory.make("nvinfer", "secondary1-nvinference-engine")
    if not sgie1:
        sys.stderr.write(" Unable to make sgie1 \n")


    sgie2 = Gst.ElementFactory.make("nvinfer", "secondary2-nvinference-engine")
    if not sgie2:
        sys.stderr.write(" Unable to make sgie2 \n")


    nvvidconv = Gst.ElementFactory.make("nvvideoconvert", "convertor")
    if not nvvidconv:
        sys.stderr.write(" Unable to create nvvidconv \n")

    caps = Gst.Caps.from_string("video/x-raw(memory:NVMM), format=RGBA")

    filter = Gst.ElementFactory.make("capsfilter", "filter")
    if not filter:    
        sys.stderr.write(" Unable to get the caps filter1 \n")
    
    filter.set_property("caps", caps)


    # Create OSD to draw on the converted RGBA buffer
    nvosd = Gst.ElementFactory.make("nvdsosd", "onscreendisplay")
    if not nvosd:
        sys.stderr.write(" Unable to create nvosd \n")

    sink = Gst.ElementFactory.make("fakesink", "fakesink")
    if not sink:
        sys.stderr.write(" Unable to create egl sink \n")


    print("Playing file %s " %args[1])
    source.set_property('location', args[1])
    streammux.set_property('width', 1280)
    streammux.set_property('height', 720)
    streammux.set_property('batch-size', 1)
    streammux.set_property('batched-push-timeout', 4000000)

    #Set properties of pgie and sgie
    pgie.set_property('config-file-path', "dstest2_pgie_config.txt")
    sgie1.set_property('config-file-path', "dstest2_sgie1_config.txt")
    sgie2.set_property('config-file-path', "dstest2_sgie2_config.txt")


    if not is_aarch64():
        # Use CUDA unified memory in the pipeline so frames
        # can be easily accessed on CPU in Python.
        mem_type = int(pyds.NVBUF_MEM_CUDA_UNIFIED)
        nvvidconv.set_property("nvbuf-memory-type", mem_type)
        

    

    #Set properties of tracker
    config = configparser.ConfigParser()
    config.read('dstest2_tracker_config.txt')
    config.sections()

    for key in config['tracker']:
        if key == 'tracker-width' :
            tracker_width = config.getint('tracker', key)
            tracker.set_property('tracker-width', tracker_width)
        if key == 'tracker-height' :
            tracker_height = config.getint('tracker', key)
            tracker.set_property('tracker-height', tracker_height)
        if key == 'gpu-id' :
            tracker_gpu_id = config.getint('tracker', key)
            tracker.set_property('gpu_id', tracker_gpu_id)
        if key == 'll-lib-file' :
            tracker_ll_lib_file = config.get('tracker', key)
            tracker.set_property('ll-lib-file', tracker_ll_lib_file)
        if key == 'll-config-file' :
            tracker_ll_config_file = config.get('tracker', key)
            tracker.set_property('ll-config-file', tracker_ll_config_file)

      


    print("Adding elements to Pipeline \n")
    pipeline.add(source)
    pipeline.add(h264parser)
    pipeline.add(decoder)
    pipeline.add(streammux)
    pipeline.add(pgie)
    pipeline.add(tracker)
    pipeline.add(sgie1)
    pipeline.add(sgie2)
    pipeline.add(nvvidconv)
    pipeline.add(nvosd)
    pipeline.add(sink)
    pipeline.add(filter)






    

    # we link the elements together
    # file-source -> h264-parser -> nvh264-decoder ->   
    # nvinfer -> nvvidconv -> nvosd -> video-renderer
    print("Linking elements in the Pipeline \n")
    source.link(h264parser)
    h264parser.link(decoder)

    sinkpad = streammux.get_request_pad("sink_0")
    if not sinkpad:
        sys.stderr.write(" Unable to get the sink pad of streammux \n")
    srcpad = decoder.get_static_pad("src")
    if not srcpad:
        sys.stderr.write(" Unable to get source pad of decoder \n")
    srcpad.link(sinkpad)
    streammux.link(pgie)
    pgie.link(tracker)
    tracker.link(sgie1)
    sgie1.link(sgie2)
    sgie2.link(nvvidconv)
    nvvidconv.link(filter)
    filter.link(nvosd)
    nvosd.link(sink)

    ##sgie1.link(sgie2)
    ##sgie2.link(sgie3)
    ##sgie3.link(nvvidconv)





    # create and event loop and feed gstreamer bus mesages to it
    loop = GLib.MainLoop()

    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect ("message", bus_call, loop)

    # Lets add probe to get informed of the meta data generated, we add probe to
    # the sink pad of the osd element, since by that time, the buffer would have
    # had got all the metadata.
    osdsinkpad = nvosd.get_static_pad("src")
    if not osdsinkpad:
        sys.stderr.write(" Unable to get sink pad of nvosd \n")
    osdsinkpad.add_probe(Gst.PadProbeType.BUFFER, osd_sink_pad_buffer_probe, 0)




    
    # start play back and listed to events
    pipeline.set_state(Gst.State.PLAYING)   
    try:
      loop.run()
    except:
      pass

    # cleanup
    pipeline.set_state(Gst.State.NULL)
    detection_queue.put('STOP')

if __name__ == '__main__':
    sys.exit(main(sys.argv))
