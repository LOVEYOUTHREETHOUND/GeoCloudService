import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/geocloudservice')))

import orderProcess

def test_orderProcess():
    orderProcess.orderProcess()
    
test_orderProcess()