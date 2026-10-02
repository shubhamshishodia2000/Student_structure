#!/usr/bin/env python3
import argparse
from . import teacher_structure_model as model

def main():
    parser=argparse.ArgumentParser(description='Build the two current teacher reports without PTR')
    parser.add_argument('--stage',choices=('all','validate'),default='all')
    args=parser.parse_args()
    conn=model.connect()
    try:
        model.healthy(conn)
        if args.stage=='all': model.build_gold(conn)
        model.validate_management(conn,'teacher_structure_management')
        model.validate_category(conn,'teacher_structure_category')
    finally: conn.close()

if __name__=='__main__': main()
