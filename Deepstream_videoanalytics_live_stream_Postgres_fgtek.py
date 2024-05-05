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
import psycopg2
import queue
import asyncio
from websockets.server import serve
import base64
import websockets
import mysql.connector
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)

def fgtek_is_aarch64():
    return platform.uname()[4] == 'aarch64'

sys.path.append('/opt/nvidia/deepstream/deepstream/lib')

from PIL import Image

def fgtek_bus_call(fgtek_bus, fgtek_message, fgtek_loop):
    t = fgtek_message.type
    if t == Gst.MessageType.EOS:
        sys.stdout.write("End-of-stream\n")
        fgtek_loop.quit()
    elif t == Gst.MessageType.WARNING:
        err, debug = fgtek_message.parse_warning()
        sys.stderr.write("Warning: %s: %s\n" % (err, debug))
    elif t == Gst.MessageType.ERROR:
        err, debug = fgtek_message.parse_error()
        sys.stderr.write("Error: %s: %s\n" % (err, debug))
        fgtek_loop.quit()
    return True

fgtek_past_tracking_meta = [0]
fgtek_past_tracking_meta[0] = 1
fgtek_fps_streams = {}
fgtek_frame_count = {}
fgtek_saved_count = {}

global fgtek_Sgie1_classes_str 
global fgtek_Sgie2_classes_str 
global fgtek_Sgie3_classes_str 

fgtek_Sgie1_classes_str = ("black", "blue", "brown", "gold", "green", "grey", "maroon",
                           "orange", "red", "silver", "white", "yellow")

fgtek_Sgie2_classes_str = ("acura", "audi", "bmw", "chevrolet", "chrysler",
                           "dodge", "ford", "gmc", "honda", "hyundai", "infiniti", "jeep", "kia",
                           "lexus", "mazda", "mercedes", "nissan", "subaru", "toyota", "volkswagen")

fgtek_Sgie3_classes_str = ("coupe", "largevehicle", "sedan", "suv", "truck", "van")

fgtek_PGIE_CLASS_ID_VEHICLE = 0
fgtek_PGIE_CLASS_ID_BICYCLE = 1
fgtek_PGIE_CLASS_ID_PERSON = 2
fgtek_PGIE_CLASS_ID_ROADSIGN = 3



def fgtek_image_to_blob(fgtek_img: np.ndarray) -> bytes:
    """Given a numpy 2D array, returns a JPEG image in binary format suitable for a BLOB column."""
    
    img_buffer = cv2.imencode('.jpg', fgtek_img)[1]
    return img_buffer.tobytes()

def fgtek_get_image_blob(fgtek_volume):
    return fgtek_image_to_blob(fgtek_volume)

def fgtek_image_to_base64(fgtek_img: np.ndarray) -> bytes:
    """ Given a numpy 2D array, returns a JPEG image in base64 format """

    # using opencv 2, there are others ways
    img_buffer = cv2.imencode('.jpg', fgtek_img)[1]
    return base64.b64encode(img_buffer).decode('utf-8')
    
def fgtek_get_image_base64(fgtek_volume):
    return fgtek_image_to_base64(fgtek_volume)

# Establish a connection to the database
fgtek_conn = psycopg2.connect(
    host="85.215.230.8",
    database="PFE",
    user="postgres",
    password="root",
)
fgtek_cursor = fgtek_conn.cursor()

fgtek_data_ready_event = threading.Event()




async def echo(websocket):
    while True:
        cpu = psutil.cpu_percent(percpu=False)
        ram = psutil.virtual_memory().percent
        gpu = GPUtil.getGPUs()[0].load if GPUtil.getGPUs() else 0

        person_count = fgtek_obj_counter[fgtek_PGIE_CLASS_ID_PERSON]
        vehicle_count = fgtek_obj_counter[fgtek_PGIE_CLASS_ID_VEHICLE]

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        vehicles = fgtek_vehicle_arr
        image = fgtek_get_image_base64(fgtek_img)

        data = {
            "image": image,
            "resourceUsage": {
                "cpu": cpu,
                "ram": ram,
                "gpu": gpu
            },
            "detections": {
                "timestamp": current_time,
                "person_count": person_count,
                "vehicle_count": vehicle_count,
                "vehicles": vehicles
            }
        }

        await websocket.send(json.dumps(data)) # send data to websocket

async def mains():
    async with serve(echo, ws_ip, ws_port):
        await asyncio.Future()

fgtek_detection_queue = queue.Queue()


def fgtek_database_worker():
    """Thread worker function that inserts detections into a database."""
    while True:
        try:
            # Block until an item becomes available
            detection = fgtek_detection_queue.get(timeout=2)
            
            if detection == 'STOP':
                break

            detection_id, cameraIP, regionalIP, image, location, timestamp, person_count, vehicle_count = detection['main']
                

            
            fgtek_cursor.execute("""
                INSERT INTO detections (id, cameraIP, regionalIP, image, location, timestamp, person_count, vehicle_count) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (detection_id, cameraIP, regionalIP, image, location, timestamp, person_count, vehicle_count))

            for vehicle in detection['vehicles']:
                logging.debug("Vehicle data before insertion - Detection ID: %s, Color: %s, Maker: %s, Type: %s", 
                              detection_id, vehicle['color'], vehicle['make'], vehicle['type'])
                
                vehicle_id = str(uuid.uuid4())
                fgtek_cursor.execute("""
                    INSERT INTO vehicles (id, detection_id, color, maker, type) 
                    VALUES (%s, %s, %s, %s, %s);
                """, (vehicle_id, detection_id, vehicle['color'], vehicle['make'], vehicle['type']))


            fgtek_conn.commit()
            logging.debug("Data insertion successful.")
        except psycopg2.Error as e:
            logging.error("Error inserting data into database: %s", e)
            fgtek_conn.rollback() 
        except queue.Empty:
            logging.debug("Queue is empty.")
            pass

# Start the database thread
threading.Thread(target=fgtek_database_worker, daemon=True).start()

def fgtek_osd_sink_pad_buffer_probe(pad,info,u_data):

    fgtek_data_ready_event.set()

    frame_number = 0
    #Intiallizing object counter with 0.
    global fgtek_obj_counter
    fgtek_obj_counter = {
        fgtek_PGIE_CLASS_ID_VEHICLE: 0,
        fgtek_PGIE_CLASS_ID_PERSON: 0,
        fgtek_PGIE_CLASS_ID_BICYCLE: 0,
        fgtek_PGIE_CLASS_ID_ROADSIGN: 0
    }
    num_rects = 0
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

        global fgtek_vehicle_arr
        fgtek_vehicle_arr = []

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

        global fgtek_img
        frame_copy = np.array(n_frame, copy=True, order='C')
        fgtek_img = cv2.cvtColor(frame_copy, cv2.COLOR_RGBA2BGRA)

        while l_obj is not None:
            try:
                obj_meta = pyds.NvDsObjectMeta.cast(l_obj.data)
            except StopIteration:
                break
            # Increment obj_counter according to detected objects
            fgtek_obj_counter[obj_meta.class_id] += 1

            l = obj_meta.classifier_meta_list   
            while l is not None:
                
                classifierMeta = pyds.NvDsClassifierMeta.cast(l.data)
                n = classifierMeta.label_info_list
                while n is not None:
                    labelInfo = pyds.NvDsLabelInfo.cast(n.data) 
                    if (labelInfo.result_label in fgtek_Sgie1_classes_str):
                        
                        vehicle["color"] = labelInfo.result_label
                    if (labelInfo.result_label in fgtek_Sgie2_classes_str):
                        print("make", labelInfo.result_label)
                        vehicle["make"] = labelInfo.result_label
                    if (labelInfo.result_label in fgtek_Sgie3_classes_str):
                        print("type", labelInfo.result_label)
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
                l_obj = l_obj.next
            except StopIteration:
                break
            fgtek_vehicle_arr.append(vehicle)  
              
        # Acquiring a display meta object. The memory ownership remains in
        # the C code so downstream plugins can still access it. Otherwise
        # the garbage collector will claim it when this probe function exits.
        display_meta = pyds.nvds_acquire_display_meta_from_pool(batch_meta)
        display_meta.num_labels = 1
        py_nvosd_text_params = display_meta.text_params[0]
        # Setting display text to be shown on screen
        # Note that the pyds module allocates a buffer for the string, and the
        # memory will not be claimed by the garbage collector.
        # Reading the display_text field here will return the C address of the
        # allocated string. Use pyds.get_string() to get the string content.
        py_nvosd_text_params.display_text = "Frame Number={} Number of Objects={} Vehicle_count={} Person_count={}".format(frame_number, num_rects, fgtek_obj_counter[fgtek_PGIE_CLASS_ID_VEHICLE], fgtek_obj_counter[fgtek_PGIE_CLASS_ID_PERSON])

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

        person_count = fgtek_obj_counter[fgtek_PGIE_CLASS_ID_PERSON]
        vehicle_count = fgtek_obj_counter[fgtek_PGIE_CLASS_ID_VEHICLE]

        now = datetime.now()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        vehicles = fgtek_vehicle_arr

        image = fgtek_get_image_blob(fgtek_img)

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

        print(vehicles)
        
        detection_data = {
        'main': (detectionid, cameraIP, regionalIP, image, location, timestamp, person_count, vehicle_count),
        'vehicles': vehicles
        }
        fgtek_detection_queue.put(detection_data)

        try:
            l_frame=l_frame.next
        except StopIteration:
            break
    return Gst.PadProbeReturn.OK	

def run_asyncio_loop():
    asyncio.run(mains())
        

def fgtek_main(args):

    

    # Check input arguments
    if(len(args) < 3):
        sys.stderr.write("usage: %s <video-feed-device> <host-ip> \n" % args[0])
        sys.exit(1)

    global ws_ip
    global ws_port

    ws_ip = args[2]
    ws_port = 8585

    async_thread = threading.Thread(target=run_asyncio_loop)
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
    source = Gst.ElementFactory.make("v4l2src", "source")
    if not source:
        sys.stderr.write(" Unable to create Source \n")

    caps_v4l2src = Gst.ElementFactory.make("capsfilter", "v4l2src_caps")
    if not caps_v4l2src:
        sys.stderr.write(" Unable to create v4l2src capsfilter \n")

    # videoconvert to make sure a superset of raw formats are supported
    vidconvsrc = Gst.ElementFactory.make("videoconvert", "convertor_src1")
    if not vidconvsrc:
        sys.stderr.write(" Unable to create videoconvert \n")

    # nvvideoconvert to convert incoming raw buffers to NVMM Mem (NvBufSurface API)
    nvvidconvsrc = Gst.ElementFactory.make("nvvideoconvert", "convertor_src2")
    if not nvvidconvsrc:
        sys.stderr.write(" Unable to create Nvvideoconvert \n")

    caps_vidconvsrc = Gst.ElementFactory.make("capsfilter", "nvmm_caps")
    if not caps_vidconvsrc:
        sys.stderr.write(" Unable to create capsfilter \n")

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

    caps_v4l2src.set_property('caps', Gst.Caps.from_string("video/x-raw, framerate=30/1"))
    caps_vidconvsrc.set_property('caps', Gst.Caps.from_string("video/x-raw(memory:NVMM)"))
    source.set_property('device', args[1])
    streammux.set_property('width', 1280)
    streammux.set_property('height', 720)
    streammux.set_property('batch-size', 1)
    streammux.set_property('batched-push-timeout', 4000000)

    #Set properties of pgie and sgie
    pgie.set_property('config-file-path', "dstest2_pgie_config.txt")
    sgie1.set_property('config-file-path', "dstest2_sgie1_config.txt")
    sgie2.set_property('config-file-path', "dstest2_sgie2_config.txt")

    if not fgtek_is_aarch64():
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
    pipeline.add(caps_v4l2src)
    pipeline.add(vidconvsrc)
    pipeline.add(nvvidconvsrc)
    pipeline.add(caps_vidconvsrc)
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
    source.link(caps_v4l2src)
    caps_v4l2src.link(vidconvsrc)
    vidconvsrc.link(nvvidconvsrc)
    nvvidconvsrc.link(caps_vidconvsrc)

    sinkpad = streammux.get_request_pad("sink_0")
    if not sinkpad:
        sys.stderr.write(" Unable to get the sink pad of streammux \n")
    srcpad = caps_vidconvsrc.get_static_pad("src")
    if not srcpad:
        sys.stderr.write(" Unable to get source pad of caps_vidconvsrc \n")
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
    bus.connect ("message", fgtek_bus_call, loop)

    # Lets add probe to get informed of the meta data generated, we add probe to
    # the sink pad of the osd element, since by that time, the buffer would have
    # had got all the metadata.
    osdsinkpad = nvosd.get_static_pad("src")
    if not osdsinkpad:
        sys.stderr.write(" Unable to get sink pad of nvosd \n")
    osdsinkpad.add_probe(Gst.PadProbeType.BUFFER, fgtek_osd_sink_pad_buffer_probe, 0)


    # start play back and listed to events
    pipeline.set_state(Gst.State.PLAYING)   
    try:
      loop.run()
    except:
      pass

    # cleanup
    pipeline.set_state(Gst.State.NULL)
    fgtek_detection_queue.put('STOP')

if __name__ == '__main__':
    sys.exit(fgtek_main(sys.argv))