# data_mh
Data Migration Hub (WIP)

The current case is very limited. Multiprocessing is to be a great portion of the application, because the quite generous capabilities of such an application would be greatly hindered by tools that it supports, since they would stop the system from being able to run properly. The greatest benefit actually lies in the ability to run multiple programs at once, allowing data to be collected, tested, transformed, etc., all at once.

"Pipelining would be a necessary replacement for the tools simply being run at a straightforward button push. Given Python specifically, data needs to be "pickled" such that it stays implementable through the entire system."

- Pickled Data was originally considered, though multiprocessing is not as seamless as multithreading, since pickling data means having to put many elements through the pickling instead of the rather comparatively simple QThreadPool/QRunnable multithreading, which makes the process as simple as a repeat class per new click function.

"While PySpark support, combined with ease of use, is an advantage to Python, Java/Scala particularly are very considerable as efficiency is noted deeper and deeper into the application, especially given the need for better parallel processing support for Apache projects (with Spark being the primary Apache project in consideration).

Python, however, still has several different capabilities that are advantageous to the application."

- Python Multithreading is in fact advantageous. Golang goroutines, C# .NET, etc., can be more efficient, but Python appears to serve such a purpose with enough ease.
