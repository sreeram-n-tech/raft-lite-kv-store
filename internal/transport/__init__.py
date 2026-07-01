import os
import sys

# Add the current directory to sys.path to ensure generated protobuf/gRPC imports resolve correctly.
sys.path.insert(0, os.path.dirname(__file__))
