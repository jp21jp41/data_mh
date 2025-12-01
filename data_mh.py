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
from PyQt6.QtCore import Qt
from pyqtwaitingspinner import spinner
#from google.oauth2 import service_account
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark import SparkConf

"""
Configuration necessity is based on the user's configurations, and there is potential for system-level
fault-tolerance additions.
"""

# Pyspark Paths
# os.environ Pyspark Python linked to Python 311 Executable File (masked directory, the user's directory would be replacing it)
# os.environ['PYSPARK_PYTHON'] = "~AppData/Local/Programs/Python/Python311/python.exe"
# os.environ Pyspark Driver Python linked to Python 311 Executable File
# os.environ['PYSPARK_DRIVER_PYTHON'] = "~AppData/Local/Programs/Python/Python311/python.exe"

"""
The remaining directories are standard as well as private, secure, and anonymous.
"""

# Spark Home Path
os.environ['SPARK_HOME'] = "C:/spark/spark-3.5.5-bin-hadoop3"

# Hadoop Home Path
os.environ['HADOOP_HOME'] = 'C:/hadoop'

# Java Home Path
os.environ['JAVA_HOME'] = "C:\Program Files\Java\jdk-17"



def initialize_pyspark(data):
        print(data)
        the_window = pickle.loads(data)
        spark = SparkSession.builder.appName("SimpleSparkApp").getOrCreate()
        the_window.layout2.removeWidget(the_window.init_label)
        the_window.init_label = QLabel("Initialized \u2705")
        the_window.layout2.addWidget(the_window.init_label, 1, 1)



class QLPickle(QLabel):
    def __init__(self):
        super().__init__()
        pickle.dumps(self)

class MyWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.pickled = [pickle.dumps(self)]
        self.setWindowTitle("Data Migration Hub")
        self.setGeometry(100, 100, 500, 200)  # x, y, width, height
        self.stacked_widget = QStackedWidget()
        #self.pickled_stacked = Widget_to_Pickle(self.stacked_widget)
        self.setCentralWidget(self.stacked_widget)
        #self.append_pickled_data([self.pickled_stacked])


        self.front_page = QWidget()
        self.layout1 = QVBoxLayout(self.front_page)
        self.layout1.addWidget(QLabel("Front Page"))
        self.to_pyspark = QPushButton("PySpark")
        self.layout1.addWidget(self.to_pyspark)
        self.to_hadoop = QPushButton("Hadoop")
        self.layout1.addWidget(self.to_hadoop)
        #self.append_pickled_data([self.front_page, self.layout1, self.to_pyspark,
        #                          self.to_hadoop])


        self.pyspark_page = QWidget()
        self.layout2 = QGridLayout(self.pyspark_page)
        self.layout2.addWidget(QLabel("PySpark"), 0, 0)
        self.pyspark_initializer = QPushButton("Run PySpark")
        self.layout2.addWidget(self.pyspark_initializer, 1, 0)
        self.to_front_page1 = QPushButton("Front Page")
        self.to_front_page1.setFixedSize(500, 25)
        self.layout2.addWidget(self.to_front_page1, 2, 0)
        self.hadoop_page = QWidget()
        layout3 = QVBoxLayout(self.hadoop_page)
        layout3.addWidget(QLabel("Hadoop"))
        self.to_front_page2 = QPushButton("Front Page")
        layout3.addWidget(self.to_front_page2)
        #self.append_pickled_data([self.pyspark_page, self.layout2, self.pyspark_initializer,
        #                          self.to_front_page1, self.layout2, self.hadoop_page,
        #                          layout3, self.to_front_page2])

        
        self.stacked_widget.addWidget(self.front_page)
        self.stacked_widget.addWidget(self.pyspark_page)
        self.stacked_widget.addWidget(self.hadoop_page)

        self.to_pyspark.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.pyspark_page))
        self.to_hadoop.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.hadoop_page))
        self.pyspark_initializer.clicked.connect(lambda: self.start_pyspark())
        self.to_front_page1.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.front_page))
        self.to_front_page2.clicked.connect(lambda: self.stacked_widget.setCurrentWidget(self.front_page))
    
    def append_pickled_data(self, items):
        for item in items:
            self.pickled.append(item.pickled)

    def __getstate__(self):
        # Return a dictionary of attributes to be pickled
        # Exclude 'gui_element' as it's unpicklable
        state = self.__dict__.copy()
        try:
            del state['gui_element']
        except:
            pass
        return state
    
    def __setstate__(self, state):
        # Restore attributes from the pickled state
        self.__dict__.update(state)
        # Re-initialize unpicklable attributes
        self.gui_element = "reinitialized_gui_object"
    
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
        self.init_label = QLabel("Initializing...")
        layout_pickle = {
            "label_text": self.init_label.text(),
            "row": 0,  # e.g., the row position
            "column": 1, # e.g., the column position
            "row_span": 1,
            "col_span": 1,
            "alignment": int(self.init_label.alignment()),
            # ... other properties
            }
        self.init_label.pickled = pickle.dumps(self.init_label)
        self.layout2.addWidget(self.init_label, 1, 1)
        #No Pickled Attribute
        self.append_pickled_data([self.init_label])
        app.processEvents()
        self.process = Process(target=initialize_pyspark, args=self.pickled)
        self.process.start()

        

        
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MyWindow()
    window.show()
    sys.exit(app.exec())
