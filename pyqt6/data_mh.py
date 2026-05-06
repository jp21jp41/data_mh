# Data Migration Hub
from datetime import datetime, date
import dearpygui.dearpygui as dpg
from multiprocessing import Process
import os
import requests
import json
import hdfs
import sys
import time
import asyncio
import pickle
from google.cloud import bigquery
from PyQt6.QtWidgets import QApplication, QWidget, QVBoxLayout, QGridLayout, QPushButton, QStackedWidget, QLabel, QMainWindow
from PyQt6.QtCore import Qt, QObject, pyqtSignal, QThread, QRunnable, QThreadPool, pyqtSlot
from pyqtwaitingspinner import spinner
#from google.oauth2 import service_account
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark import SparkConf

# Establish the User
user = os.environ.get('USER')

# Try PySpark Python Path with User
try:
    os.environ['PYSPARK_PYTHON'] = str(user) + "/AppData/Local/Programs/Python/Python311/python.exe"
except:
    """
    # Enter Input to Correct Failure
    print("\nSystem User Failure." \
    "\nEnter \'t\' into the input to enter a \nTemporary User" \
    "\n --or-- \n" \
    "\nEnter \'d\' into the input to enter a \nFull Directory")
    select = input()
    if select == 't':
        os.environ['USER'] = input()
        user = os.environ['USER']
        os.environ['PYSPARK_PYTHON'] = str(user) + "/AppData/Local/Programs/Python/Python311/python.exe"
    elif select == 'd':
        print("Enter Directory")
        pyspark = input()
        os.environ['PYSPARK_PYTHON'] = pyspark
    else:
        print("Input Error. Exiting Program.")
        sys.exit()
    """
    print("\nSystem User Failure." \
    "\nEnter a \nTemporary User" \
    "\ninto the input to correct the directory.")
    os.environ['USER'] = input()
    try:
        # Try User and Path Again
        user = os.environ.get('USER')
        print(user)
        os.environ['PYSPARK_PYTHON'] = str(user) + "/AppData/Local/Programs/Python/Python311/python.exe"
    except:
        # Exit if Second Failure
        print("Temporary User Failure. Please Restart the Program.")
        sys.exit()

# Pyspark Driver Path
os.environ['PYSPARK_DRIVER_PYTHON'] = str(user) + "/AppData/Local/Programs/Python/Python311/python.exe"

# Spark Home Path
os.environ['SPARK_HOME'] = "C:/spark/spark-3.5.5-bin-hadoop3"

# Hadoop Home Path
os.environ['HADOOP_HOME'] = 'C:/hadoop'

# Java Home Path
os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-17"

class WorkerSignals(QObject):
    finished = pyqtSignal()
    error = pyqtSignal(tuple)
    result = pyqtSignal(object)
    progress = pyqtSignal(int)


class SparkWorker(QRunnable):
    def __init__(self, task_name, the_window):
        super(SparkWorker, self).__init__()
        self.task_name = task_name
        self.window = the_window

    @pyqtSlot()
    def run(self):
        print(f"Starting task: {self.task_name} on thread {QThreadPool.globalInstance().maxThreadCount()}") # {QThreadPool.globalInstance().activeThreadCount()}")
        spark = SparkSession.builder.appName("SimpleSparkApp").getOrCreate()
        #self.window.layout2.removeWidget(self.window.init_label)
        #self.window.init_label = QLabel("Initialized \u2705")
        #self.window.layout2.addWidget(self.window.init_label, 1, 1)
        self.window.init_label.setText("Initialized \u2705")
        self.window.pyspark_initializer.setText("Stop PySpark")
        print(f"Finished task: {self.task_name}")

class HadoopWorker(QRunnable):
    def __init__(self, task_name, the_window):
        super(HadoopWorker, self).__init__()
        self.task_name = task_name
        self.window = the_window

    @pyqtSlot()
    def run(self):
        print(f"Starting task: {self.task_name} on thread {QThreadPool.globalInstance().maxThreadCount()}") # {QThreadPool.globalInstance().activeThreadCount()}")
        session = requests.session()
        self.window.client = hdfs.InsecureClient('http://localhost:9871', user='justi')
        print("Client:")
        print(self.window.client)
        #self.window.layout2.removeWidget(self.window.init_label)
        #self.window.init_label = QLabel("Initialized \u2705")
        #self.window.layout2.addWidget(self.window.init_label, 1, 1)
        self.window.init_label.setText("Initialized \u2705")
        self.window.hadoop_initializer.setText("Stop Hadoop")
        print(f"Finished task: {self.task_name}")

class HadoopEndWorker(QRunnable):
    def __init__(self, task_name, the_window):
        super(HadoopEndWorker, self).__init__()
        self.task_name = task_name
        self.window = the_window

    @pyqtSlot()
    def run(self):
        print(f"Starting task: {self.task_name} on thread {QThreadPool.globalInstance().maxThreadCount()}") # {QThreadPool.globalInstance().activeThreadCount()}")
        with self.window.client.read('features') as reader:
            features = reader.read()
        try:
            print(self.worker.client)
        except:
            pass
        self.window.init_label.setText("Terminated \u0001F5CE")
        self.window.hadoop_initializer.setText("Run Hadoop")
        print(f"Finished task: {self.task_name}")

class Worker(QRunnable):
    def __init__(self, task_name):
        super(Worker, self).__init__()
        self.task_name = task_name

    @pyqtSlot()
    def run(self):
        # Your long-running task goes here
        print(f"Starting task: {self.task_name} on thread {QThreadPool.globalInstance().maxThreadCount()}") # {QThreadPool.globalInstance().activeThreadCount()}")
        time.sleep(1.5) # Simulate a long-running task
        print(f"Finished task: {self.task_name}")

class MyWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Data Migration Hub")
        self.setGeometry(100, 100, 500, 200)  # x, y, width, height
        self.stacked_widget = QStackedWidget()
        self.setCentralWidget(self.stacked_widget)
        self.worker_list = []


        self.front_page = QWidget()
        self.layout1 = QVBoxLayout(self.front_page)
        self.layout1.addWidget(QLabel("Front Page"))
        self.to_pyspark = QPushButton("PySpark")
        self.layout1.addWidget(self.to_pyspark)
        self.to_hadoop = QPushButton("Hadoop")
        self.layout1.addWidget(self.to_hadoop)


        self.pyspark_page = QWidget()
        self.layout2 = QGridLayout(self.pyspark_page)
        self.layout2.addWidget(QLabel("PySpark"), 0, 0)
        self.pyspark_initializer = QPushButton("Run PySpark")
        self.layout2.addWidget(self.pyspark_initializer, 1, 0)
        self.to_front_page1 = QPushButton("Front Page")
        self.to_front_page1.setFixedSize(500, 25)
        self.layout2.addWidget(self.to_front_page1, 2, 0)
        self.hadoop_page = QWidget()
        self.layout3 = QGridLayout(self.hadoop_page)
        self.layout3.addWidget(QLabel("Hadoop"), 0, 0)
        self.hadoop_initializer = QPushButton("Run Hadoop")
        self.layout3.addWidget(self.hadoop_initializer, 1, 0)
        self.to_front_page2 = QPushButton("Front Page")
        self.to_front_page2.setFixedSize(500, 25)
        self.layout3.addWidget(self.to_front_page2, 2, 0)

        
        self.stacked_widget.addWidget(self.front_page)
        self.stacked_widget.addWidget(self.pyspark_page)
        self.stacked_widget.addWidget(self.hadoop_page)

        self.to_pyspark.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.pyspark_page))
        self.to_hadoop.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.hadoop_page))
        self.lambda_start_pyspark = lambda: self.start_pyspark()
        self.lambda_stop_pyspark = lambda: self.stop_pyspark()
        self.lambda_start_hadoop = lambda: self.start_hadoop()
        self.lambda_stop_hadoop = lambda: self.stop_hadoop()
        self.pyspark_initializer.clicked.connect(self.lambda_start_pyspark)
        self.hadoop_initializer.clicked.connect(self.lambda_start_hadoop)
        self.to_front_page1.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.front_page))
        self.to_front_page2.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.front_page))
    
    def quick_geometry_QPushButton(self, text, geometry = (100, 100, 300, 25)):
        the_button = QPushButton(text)
        x = geometry[0]
        y = geometry[1]
        width = geometry[2]
        height = geometry[3]
        #the_button.move(x, y)
        the_button.setFixedSize(width, height)
        return the_button
    
    def start_pyspark(self):
        try:
            self.init_label.setText("Initializing...")
        except Exception as e:
            self.init_label = QLabel("Initializing...")
            self.layout2.addWidget(self.init_label, 1, 1)
        worker = SparkWorker("PySpark",  self)
        print("Horizontal:")
        QThreadPool.globalInstance().start(worker)

    def start_hadoop(self):
        try:
            self.init_label.setText("Initializing...")
        except Exception as e:
            self.init_label = QLabel("Initializing...")
            self.layout3.addWidget(self.init_label, 1, 1)
        worker = HadoopWorker("Hadoop", self)
        QThreadPool.globalInstance().start(worker)
        self.hadoop_initializer.clicked.disconnect(self.lambda_start_hadoop)
        self.hadoop_initializer.clicked.connect(self.lambda_stop_hadoop)
    
    def stop_hadoop(self):
        self.init_label.setText("Terminating...")
        worker = HadoopEndWorker("End Hadoop", self)
        QThreadPool.globalInstance().start(worker)
        self.hadoop_initializer.clicked.disconnect(self.lambda_stop_hadoop)
        self.hadoop_initializer.clicked.connect(self.lambda_start_hadoop)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MyWindow()
    window.show()
    sys.exit(app.exec())


