from python_helper import TestHelper

global db_spark
db_spark = 'db_spark'
global db_display 
db_display = 'db_display'

TestHelper.run(__file__, inspectGlobals=False)